use crate::{Error, Result, ValidationIssue};
use jsonschema::Validator;
use mcpx_protocol::ToolDescriptor;
use serde::Serialize;
use serde_json::{Map, Value};
use std::{
    collections::BTreeMap,
    rc::{Rc, Weak},
    sync::Arc,
};

struct CompiledTool {
    schema: ToolSchema,
}

/// Immutable compiled input/output rules and the original descriptor.
/// Sharing this snapshot never shares a binding's owner/argument identity.
#[derive(Clone)]
pub struct ToolSchema(Arc<CompiledSchemas>);
struct CompiledSchemas {
    descriptor: ToolDescriptor,
    input: Arc<Validator>,
    output: Option<Arc<Validator>>,
}
impl ToolSchema {
    fn compile(
        descriptor: ToolDescriptor,
        registry: &crate::SchemaRegistry,
        cache: &mut crate::validator_cache::ValidatorCache,
    ) -> Result<Self> {
        let input = cache.compile(&descriptor.input_schema, registry)?;
        let output = descriptor
            .output_schema
            .as_ref()
            .map(|schema| cache.compile(schema, registry))
            .transpose()?;
        Ok(Self(Arc::new(CompiledSchemas {
            descriptor,
            input,
            output,
        })))
    }
    pub fn descriptor(&self) -> &ToolDescriptor {
        &self.0.descriptor
    }
    pub fn bind(&self) -> ToolBinding {
        ToolBinding(Rc::new(CompiledTool {
            schema: self.clone(),
        }))
    }
}

#[derive(Clone)]
pub struct ToolBinding(Rc<CompiledTool>);

#[derive(Clone)]
pub struct BoundArgs {
    binding: ToolBinding,
    value: Value,
}

pub struct ArgsBuilder {
    binding: ToolBinding,
    fields: Map<String, Value>,
}

fn regex_limited(issue: &jsonschema::ValidationError<'_>) -> bool {
    matches!(
        issue.kind(),
        jsonschema::error::ValidationErrorKind::BacktrackLimitExceeded { .. }
            | jsonschema::error::ValidationErrorKind::RegexEngineFailure { .. }
    )
}
fn validate(validator: &Validator, value: &Value) -> Result<()> {
    // iter_errors collects every error before returning its iterator. Taking a
    // prefix afterward does not bound that allocation or diagnostic work.
    if let Err(issue) = validator.validate(value) {
        let (code, message) = if regex_limited(&issue) {
            (
                "ValidationLimitExceeded",
                "schema validation could not complete within regex engine limits",
            )
        } else {
            (
                "ValidationFailed",
                "arguments do not satisfy the remote schema",
            )
        };
        return Err(Error {
            issues: vec![ValidationIssue {
                instance_path: issue.instance_path().to_string(),
                schema_path: issue.schema_path().to_string(),
            }],
            ..Error::local(code, message)
        });
    }
    Ok(())
}

impl ToolBinding {
    pub fn new(descriptor: ToolDescriptor) -> Result<Self> {
        Self::with_schema_registry(descriptor, &crate::SchemaRegistry::default())
    }
    pub fn with_schema_registry(
        descriptor: ToolDescriptor,
        registry: &crate::SchemaRegistry,
    ) -> Result<Self> {
        let schema = ToolSchema::compile(
            descriptor,
            registry,
            &mut crate::validator_cache::ValidatorCache::default(),
        )?;
        Ok(schema.bind())
    }
    pub fn compiled_schema(&self) -> ToolSchema {
        self.0.schema.clone()
    }
    pub fn descriptor(&self) -> &ToolDescriptor {
        self.0.schema.descriptor()
    }
    pub fn args(&self) -> ArgsBuilder {
        ArgsBuilder {
            binding: self.clone(),
            fields: Map::new(),
        }
    }
    /// Serialize a caller-owned Rust type once, then validate the complete object
    /// against this immutable remote Schema without constructing a command line.
    pub fn args_from(&self, value: impl Serialize) -> Result<BoundArgs> {
        self.validate_json(crate::serialize_value(value)?)
    }
    pub fn validate_json(&self, value: Value) -> Result<BoundArgs> {
        crate::check_json(&value, 0)?;
        if !value.is_object() {
            return Err(Error::local(
                "ValidationFailed",
                "arguments must be an object",
            ));
        }
        crate::serde_args::check_size(&value)?;
        validate(&self.0.schema.0.input, &value)?;
        Ok(BoundArgs {
            binding: self.clone(),
            value,
        })
    }
    pub fn check_args(&self, args: &BoundArgs) -> Result<()> {
        if !Rc::ptr_eq(&self.0, &args.binding.0) {
            return Err(Error::local(
                "BindingMismatch",
                "arguments belong to another binding",
            ));
        }
        Ok(())
    }
    pub fn validate_output(&self, value: Option<&Value>) -> Option<bool> {
        match self.validate_output_status(value) {
            crate::OutputValidation::NotDeclared => None,
            status => Some(matches!(status, crate::OutputValidation::Valid)),
        }
    }
    /// Retains the difference between invalid output and unfinished validation.
    pub fn validate_output_status(&self, value: Option<&Value>) -> crate::OutputValidation {
        let Some(validator) = &self.0.schema.0.output else {
            return crate::OutputValidation::NotDeclared;
        };
        let Some(value) = value else {
            return crate::OutputValidation::Invalid;
        };
        match validator.validate(value) {
            Ok(()) => crate::OutputValidation::Valid,
            Err(issue) if regex_limited(&issue) => crate::OutputValidation::Indeterminate,
            Err(_) => crate::OutputValidation::Invalid,
        }
    }
}

impl BoundArgs {
    pub fn value(&self) -> &Value {
        &self.value
    }
}

impl ArgsBuilder {
    pub fn set(mut self, name: &str, value: impl Serialize) -> Result<Self> {
        if self.fields.contains_key(name) {
            return Err(Error::local(
                "DuplicateArgument",
                format!("argument {name} is already set"),
            ));
        }
        self.fields
            .insert(name.into(), crate::serialize_value(value)?);
        Ok(self)
    }
    pub fn replace(mut self, name: &str, value: impl Serialize) -> Result<Self> {
        self.fields
            .insert(name.into(), crate::serialize_value(value)?);
        Ok(self)
    }
    pub fn finish(self) -> Result<BoundArgs> {
        self.binding.validate_json(Value::Object(self.fields))
    }
}

pub struct Catalog {
    revisions: crate::CatalogRevisionSource,
    tools: BTreeMap<String, ToolDescriptor>,
    bindings: BTreeMap<String, Weak<CompiledTool>>,
    generation: u64,
    registry: crate::SchemaRegistry,
    validators: crate::validator_cache::ValidatorCache,
}

impl Default for Catalog {
    fn default() -> Self {
        Self {
            revisions: crate::CatalogRevisionSource::new(),
            tools: Default::default(),
            bindings: Default::default(),
            generation: 0,
            registry: Default::default(),
            validators: Default::default(),
        }
    }
}
impl Drop for Catalog {
    fn drop(&mut self) {
        self.revisions.close();
    }
}
impl Catalog {
    pub fn revision_source(&self) -> crate::CatalogRevisionSource {
        self.revisions.clone()
    }
    pub(crate) fn invalidate_revision(&self) {
        self.revisions.invalidate();
    }
    pub(crate) fn close_watchers(&self) {
        self.revisions.close();
    }
    pub fn set_schema_registry(&mut self, registry: crate::SchemaRegistry) {
        self.bindings.clear();
        self.validators.clear();
        self.registry = registry;
        self.revisions.invalidate();
    }
    pub fn validator_cache_stats(&self) -> crate::ValidatorCacheStats {
        self.validators.stats()
    }
    pub fn set_validator_cache_capacity(&mut self, capacity: usize) -> Result<()> {
        self.validators.set_capacity(capacity)
    }
    pub fn clear_validator_cache(&mut self) {
        self.validators.clear();
    }
    pub fn schema_registry(&self) -> &crate::SchemaRegistry {
        &self.registry
    }
    pub fn invalidate(&mut self) {
        self.validators.clear();
        self.registry = Default::default();
        self.tools.clear();
        self.bindings.clear();
        // Invalidating an exhausted owner must still discard its old authority.
        self.generation = self.generation.saturating_add(1).min(crate::MAX_HOST_ID);
        self.revisions.invalidate();
    }
    pub(crate) fn next_generation(&self) -> Result<u64> {
        self.generation
            .checked_add(1)
            .filter(|next| *next <= crate::MAX_HOST_ID)
            .ok_or_else(|| Error::local("LimitExceeded", "catalog generation exhausted"))
    }
    #[cfg(test)]
    pub(crate) fn seed_generation_for_test(&mut self, generation: u64) {
        self.generation = generation;
    }
    pub fn replace(&mut self, tools: Vec<ToolDescriptor>) -> Result<()> {
        let generation = self.next_generation()?;
        if tools.len() > 10_000
            || serde_json::to_vec(&tools)
                .map_err(|_| Error::local("InvalidCatalog", "invalid descriptors"))?
                .len()
                > mcpx_protocol::MAX_BODY
        {
            return Err(Error::local(
                "LimitExceeded",
                "catalog exceeds tool or byte limit",
            ));
        }
        let mut next = BTreeMap::new();
        for tool in tools {
            if tool.name.is_empty() || next.insert(tool.name.clone(), tool).is_some() {
                return Err(Error::local(
                    "InvalidCatalog",
                    "empty or duplicate tool name",
                ));
            }
        }
        self.bindings.retain(|name, binding| {
            binding
                .upgrade()
                .is_some_and(|current| next.get(name) == Some(current.schema.descriptor()))
        });
        self.tools = next;
        self.generation = generation;
        self.revisions.publish();
        Ok(())
    }
    pub fn tools(&self) -> impl Iterator<Item = &ToolDescriptor> {
        self.tools.values()
    }
    /// Exact host-safe revision number. Exhaustion prevents new publication;
    /// invalidation still clears authority and saturates at MAX_HOST_ID.
    /// Use revision_source for observing invalidation after this ceiling.
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub(crate) fn compile_local_binding(
        &mut self,
        descriptor: ToolDescriptor,
    ) -> Result<ToolBinding> {
        Ok(ToolSchema::compile(descriptor, &self.registry, &mut self.validators)?.bind())
    }
    pub fn bind(&mut self, name: &str) -> Result<ToolBinding> {
        if let Some(binding) = self.bindings.get(name).and_then(Weak::upgrade) {
            return Ok(ToolBinding(binding));
        }
        self.bindings
            .retain(|_, binding| binding.strong_count() > 0);
        let descriptor = self
            .tools
            .get(name)
            .ok_or_else(|| Error::local("ToolNotFound", format!("unknown tool {name}")))?
            .clone();
        let binding = ToolSchema::compile(descriptor, &self.registry, &mut self.validators)?.bind();
        self.bindings.insert(name.into(), Rc::downgrade(&binding.0));
        Ok(binding)
    }
    pub fn check(&self, binding: &ToolBinding) -> Result<()> {
        if !self
            .bindings
            .get(&binding.descriptor().name)
            .and_then(Weak::upgrade)
            .is_some_and(|current| Rc::ptr_eq(&binding.0, &current))
        {
            return Err(Error::local(
                "BindingStale",
                "binding is not current for this remote",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod generation_tests {
    use super::*;
    use crate::MAX_HOST_ID;
    use serde_json::json;

    fn descriptor(name: &str) -> ToolDescriptor {
        serde_json::from_value(json!({"name":name,"inputSchema":{"type":"object"}})).unwrap()
    }

    #[test]
    fn last_generation_is_exact_and_failed_publication_keeps_bindings_cache_and_revision() {
        let mut catalog = Catalog::default();
        catalog.replace(vec![descriptor("echo")]).unwrap();
        let binding = catalog.bind("echo").unwrap();
        catalog.seed_generation_for_test(MAX_HOST_ID - 1);
        let source = catalog.revision_source();
        let previous = source.snapshot();
        catalog.replace(vec![descriptor("echo")]).unwrap();
        assert_eq!(catalog.generation(), MAX_HOST_ID);
        let published = source.snapshot();
        assert_ne!(previous, published);
        let stats = catalog.validator_cache_stats();
        for _ in 0..2 {
            let error = catalog
                .replace(vec![descriptor("replacement")])
                .unwrap_err();
            assert_eq!(error.code, "LimitExceeded");
            assert_eq!(
                error.execution_certainty,
                crate::ExecutionCertainty::NotSent
            );
            assert_eq!(catalog.generation(), MAX_HOST_ID);
            assert_eq!(source.snapshot(), published);
            assert_eq!(
                catalog
                    .tools()
                    .map(|tool| tool.name.as_str())
                    .collect::<Vec<_>>(),
                ["echo"]
            );
            catalog.check(&binding).unwrap();
            assert_eq!(catalog.validator_cache_stats().entries, stats.entries);
            assert_eq!(
                catalog.validator_cache_stats().compilations,
                stats.compilations
            );
        }
    }

    #[test]
    fn invalidation_at_the_generation_ceiling_still_revokes_every_old_binding() {
        let mut catalog = Catalog::default();
        catalog.set_schema_registry(
            crate::SchemaRegistry::new([(
                "https://schemas.example/a".into(),
                json!({"type":"object"}),
            )])
            .unwrap(),
        );
        catalog.replace(vec![descriptor("echo")]).unwrap();
        let binding = catalog.bind("echo").unwrap();
        catalog.seed_generation_for_test(MAX_HOST_ID - 1);
        let source = catalog.revision_source();
        let mut previous = source.snapshot();
        for _ in 0..3 {
            catalog.invalidate();
            assert_eq!(catalog.generation(), MAX_HOST_ID);
            assert_ne!(source.snapshot(), previous);
            previous = source.snapshot();
            assert_eq!(catalog.check(&binding).unwrap_err().code, "BindingStale");
            assert_eq!(catalog.validator_cache_stats().entries, 0);
            assert_eq!(catalog.schema_registry().stats().resources, 0);
            assert!(catalog.tools().next().is_none());
            assert_eq!(
                catalog.replace(vec![descriptor("echo")]).unwrap_err().code,
                "LimitExceeded"
            );
        }
    }
}
