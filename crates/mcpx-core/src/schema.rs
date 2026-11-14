use crate::{Error, Result};
use jsonschema::{Draft, Registry, Uri, Validator, uri};
use serde::de::{DeserializeSeed, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};

pub const MAX_SCHEMA_BYTES: usize = 1024 * 1024;
pub const MAX_SCHEMA_RESOURCES: usize = 128;
pub const MAX_SCHEMA_RESOURCE_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_SCHEMA_BUNDLE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_SCHEMA_NODES: usize = 16_384;
pub const MAX_SCHEMA_PATTERNS: usize = 256;
pub const MAX_SCHEMA_PATTERN_BYTES: usize = 4096;
pub const MAX_SCHEMA_PATTERN_TOTAL_BYTES: usize = 64 * 1024;
/// Approximate backend NFA and DFA cache limits, not allocator/RSS measurements.
pub const MAX_REGEX_COMPILED_BYTES: usize = 1024 * 1024;
pub const MAX_REGEX_DFA_BYTES: usize = 1024 * 1024;
pub const MAX_REGEX_VM_INSTRUCTIONS: usize = 16_384;
pub const MAX_REGEX_BACKTRACKS: usize = 20_000;
pub const MAX_REGEX_REPEAT: usize = 4096;
pub const MAX_REGEX_STATIC_WORK: usize = 65_536;
pub const MAX_REGEX_AST_DEPTH: usize = 64;
const MAX_URI: usize = 2048;
const ROOT: &str = "json-schema:///";
const DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";

/// The exact supported vocabulary URIs. Format and content remain annotations.
pub const SCHEMA_VOCABULARIES: &[&str] = &[
    "https://json-schema.org/draft/2020-12/vocab/core",
    "https://json-schema.org/draft/2020-12/vocab/applicator",
    "https://json-schema.org/draft/2020-12/vocab/validation",
    "https://json-schema.org/draft/2020-12/vocab/unevaluated",
    "https://json-schema.org/draft/2020-12/vocab/meta-data",
    "https://json-schema.org/draft/2020-12/vocab/format-annotation",
    "https://json-schema.org/draft/2020-12/vocab/content",
];

/// A fixed host configuration record. Schema JSON is text to preserve large integers in JS.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaResource {
    pub uri: String,
    pub schema_json: String,
}
struct ResourceRecords<'a>(&'a mut bool);
impl<'de> DeserializeSeed<'de> for ResourceRecords<'_> {
    type Value = Vec<SchemaResource>;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        struct Records<'a>(&'a mut bool);
        impl<'de> Visitor<'de> for Records<'_> {
            type Value = Vec<SchemaResource>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("bounded schema resource records")
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut records = Vec::new();
                let mut bytes = 0usize;
                while let Some(resource) = sequence.next_element::<SchemaResource>()? {
                    bytes = bytes
                        .saturating_add(resource.uri.len())
                        .saturating_add(resource.schema_json.len())
                        .saturating_add(128);
                    if records.len() == MAX_SCHEMA_RESOURCES
                        || resource.uri.len() > MAX_URI
                        || resource.schema_json.len() > MAX_SCHEMA_BYTES
                        || bytes > MAX_SCHEMA_RESOURCE_BYTES
                    {
                        *self.0 = true;
                        return Err(serde::de::Error::custom("schema resource budget exceeded"));
                    }
                    records.push(resource);
                }
                Ok(records)
            }
        }
        deserializer.deserialize_seq(Records(self.0))
    }
}
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct SchemaRegistryStats {
    pub resources: usize,
    /// Source JSON/URI bytes plus 128 bytes per registered document; not allocator measurement.
    pub bytes: usize,
    pub schema_nodes: usize,
    pub patterns: usize,
    pub pattern_bytes: usize,
}

#[derive(Clone, Default)]
pub struct SchemaRegistry(Option<Arc<Prepared>>);
struct Prepared {
    index: Registry<'static>,
    audit: Audit,
    stats: SchemaRegistryStats,
    fingerprint: [u8; 32],
}
impl std::fmt::Debug for SchemaRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaRegistry")
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

fn unsupported(message: &str) -> Error {
    Error::local("SchemaUnsupported", message)
}
fn limit(message: &str) -> Error {
    Error::local("LimitExceeded", message)
}
// Conservative finite-repetition admission estimate, not execution fuel.
fn regex_work(expr: &fancy_regex::Expr, depth: usize) -> Result<usize> {
    use fancy_regex::{AstNode, Expr};
    let rejected = || unsupported("schema regular expression exceeds the static work policy");
    if depth > MAX_REGEX_AST_DEPTH {
        return Err(rejected());
    }
    let child = |expr| regex_work(expr, depth + 1);
    let work = match expr {
        Expr::Concat(nodes) | Expr::Alt(nodes) => nodes.iter().try_fold(1usize, |cost, node| {
            cost.checked_add(child(node)?)
                .filter(|cost| *cost <= MAX_REGEX_STATIC_WORK)
                .ok_or_else(rejected)
        })?,
        Expr::Group(node) => 1 + child(node)?,
        Expr::LookAround(node, _) | Expr::AstNode(AstNode::AstGroup { inner: node, .. }, _) => {
            1 + child(node)?
        }
        Expr::Repeat {
            child: node,
            lo,
            hi,
            ..
        } => {
            if *lo > MAX_REGEX_REPEAT || (*hi != usize::MAX && *hi > MAX_REGEX_REPEAT) {
                return Err(rejected());
            }
            let repetitions = if *hi == usize::MAX { (*lo).max(1) } else { *hi };
            child(node)?
                .checked_mul(repetitions)
                .and_then(|cost| cost.checked_add(1))
                .ok_or_else(rejected)?
        }
        Expr::Literal { val, .. } => val.len().max(1),
        Expr::LiteralBytes { bytes } => bytes.len().max(1),
        Expr::Delegate { inner, .. } => inner.len().max(1),
        Expr::Empty
        | Expr::Any { .. }
        | Expr::Assertion(_)
        | Expr::Backref { .. }
        | Expr::AstNode(AstNode::Backref { .. }, _) => 1,
        _ => {
            return Err(unsupported(
                "schema regular expression uses unsupported engine constructs",
            ));
        }
    };
    if work > MAX_REGEX_STATIC_WORK {
        return Err(rejected());
    }
    Ok(work)
}
fn reference_error(error: jsonschema::ReferencingError) -> Error {
    match error {
        jsonschema::ReferencingError::Unretrievable { .. } => Error::local(
            "SchemaReferenceUnavailable",
            "schema reference is not registered in memory",
        ),
        _ => unsupported("schema reference cannot be resolved under the supported policy"),
    }
}
#[derive(Debug)]
struct NoRetrieval;
impl std::fmt::Display for NoRetrieval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("schema retrieval is disabled")
    }
}
impl std::error::Error for NoRetrieval {}
impl jsonschema::Retrieve for NoRetrieval {
    fn retrieve(
        &self,
        _: &Uri<String>,
    ) -> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err(Box::new(NoRetrieval))
    }
}
fn reserved(uri: &str) -> bool {
    uri.starts_with("json-schema:")
        || uri.starts_with("https://json-schema.org/")
        || uri.starts_with("http://json-schema.org/")
}
fn document_uri(text: &str) -> Result<String> {
    if text.len() > MAX_URI {
        return Err(limit("schema URI exceeds 2048 bytes"));
    }
    let parsed = Uri::parse(text)
        .map_err(|_| Error::local("ConfigurationError", "schema resource URI must be absolute"))?;
    if parsed.has_fragment() {
        return Err(Error::local(
            "ConfigurationError",
            "schema resource URI must not have a fragment",
        ));
    }
    let parsed = parsed.normalize().to_string();
    if reserved(&parsed) {
        return Err(unsupported("schema resource identifier is reserved"));
    }
    Ok(parsed)
}
fn resolve(base: &str, reference: &str) -> Result<String> {
    if reference.len() > MAX_URI {
        return Err(limit("schema reference exceeds 2048 bytes"));
    }
    let base = uri::from_str(base).map_err(reference_error)?;
    let target = uri::resolve_against(&base.borrow(), reference).map_err(reference_error)?;
    if target.as_str().len() > MAX_URI {
        return Err(limit("resolved schema URI exceeds 2048 bytes"));
    }
    Ok(target.to_string())
}

#[derive(Clone, Default)]
struct Audit {
    // Shared Value inputs keep these pointer identities stable. No pointer is dereferenced here.
    seen: BTreeSet<usize>,
    ids: BTreeMap<String, usize>,
    anchors: BTreeMap<(String, String), usize>,
    references: VecDeque<(String, String)>,
    patterns: usize,
    pattern_bytes: usize,
    preflighted: BTreeSet<String>,
}
impl Audit {
    fn pattern(&mut self, pattern: &str) -> Result<()> {
        if pattern.len() > MAX_SCHEMA_PATTERN_BYTES
            || self.patterns == MAX_SCHEMA_PATTERNS
            || pattern.len() > MAX_SCHEMA_PATTERN_TOTAL_BYTES - self.pattern_bytes
        {
            return Err(limit("schema regular expression source budget exceeded"));
        }
        self.patterns += 1;
        self.pattern_bytes += pattern.len();
        if self.preflighted.contains(pattern) {
            return Ok(());
        }
        if !jsonschema_regex::is_valid_ecma_regex(pattern) {
            return Err(unsupported(
                "schema regular expression is not valid ECMA syntax",
            ));
        }
        let translated = jsonschema_regex::to_rust_regex(pattern)
            .map_err(|_| unsupported("schema regular expression cannot be translated"))?;
        let tree = fancy_regex::Expr::parse_tree(&translated)
            .map_err(|_| unsupported("schema regular expression cannot be parsed"))?;
        regex_work(&tree.expr, 0)?;
        // jsonschema exposes delegate limits but not fancy-regex's VM cap.
        // Preflight the same translated pattern/engine before allowing its compile.
        fancy_regex::RegexBuilder::new(&translated)
            .backtrack_limit(MAX_REGEX_BACKTRACKS)
            .delegate_size_limit(MAX_REGEX_COMPILED_BYTES)
            .delegate_dfa_size_limit(MAX_REGEX_DFA_BYTES)
            .delegate_dfa_skip_cache_capacity_check(false)
            .max_prog_size(MAX_REGEX_VM_INSTRUCTIONS)
            .build()
            .map_err(|_| {
                unsupported("schema regular expression exceeds the supported engine policy")
            })?;
        self.preflighted.insert(pattern.into());
        Ok(())
    }
    fn identity(&mut self, id: String, node: usize) -> Result<()> {
        if self.ids.insert(id, node).is_some_and(|old| old != node) {
            return Err(unsupported("schema resource identifiers conflict"));
        }
        Ok(())
    }
    fn walk(&mut self, schema: &Value, base: &str) -> Result<()> {
        let node = std::ptr::from_ref(schema) as usize;
        if !self.seen.insert(node) {
            return Ok(());
        }
        if self.seen.len() > MAX_SCHEMA_NODES {
            return Err(limit("schema graph exceeds 16384 schema nodes"));
        }
        if schema.is_boolean() {
            return Ok(());
        }
        let object = schema
            .as_object()
            .ok_or_else(|| unsupported("schema must be an object or boolean"))?;
        if let Some(pattern) = object.get("pattern").and_then(Value::as_str) {
            self.pattern(pattern)?;
        }
        if let Some(patterns) = object.get("patternProperties").and_then(Value::as_object) {
            for pattern in patterns.keys() {
                self.pattern(pattern)?;
            }
        }
        let mut base = base.to_owned();
        if let Some(id) = object.get("$id") {
            base = resolve(
                &base,
                id.as_str()
                    .ok_or_else(|| unsupported("schema $id must be a string"))?,
            )?;
            if uri::from_str(&base)
                .map_err(reference_error)?
                .has_fragment()
                || (reserved(&base) && !base.starts_with("json-schema:"))
            {
                return Err(unsupported(
                    "schema $id has a fragment or uses a reserved identifier",
                ));
            }
            self.identity(base.clone(), node)?;
        }
        if object
            .get("$schema")
            .is_some_and(|v| v.as_str() != Some(DIALECT))
        {
            return Err(unsupported("only JSON Schema Draft 2020-12 is supported"));
        }
        if let Some(vocabulary) = object.get("$vocabulary") {
            let vocabulary = vocabulary
                .as_object()
                .ok_or_else(|| unsupported("schema $vocabulary must be an object"))?;
            for (name, required) in vocabulary {
                let required = required.as_bool().ok_or_else(|| {
                    unsupported("schema vocabulary requirements must be booleans")
                })?;
                Uri::parse(name.as_str())
                    .map_err(|_| unsupported("schema vocabulary URI must be absolute"))?;
                if required && !SCHEMA_VOCABULARIES.contains(&name.as_str()) {
                    return Err(unsupported("required schema vocabulary is unsupported"));
                }
            }
        }
        for key in ["$anchor", "$dynamicAnchor"] {
            if let Some(anchor) = object.get(key) {
                let anchor = anchor
                    .as_str()
                    .ok_or_else(|| unsupported("schema anchor must be a string"))?;
                if anchor.len() > MAX_URI {
                    return Err(limit("schema anchor exceeds 2048 bytes"));
                }
                if self
                    .anchors
                    .insert((base.clone(), anchor.into()), node)
                    .is_some_and(|old| old != node)
                {
                    return Err(unsupported("schema anchors conflict"));
                }
            }
        }
        for key in ["$ref", "$dynamicRef"] {
            if let Some(reference) = object.get(key) {
                let reference = reference
                    .as_str()
                    .ok_or_else(|| unsupported("schema reference must be a string"))?;
                resolve(&base, reference)?;
                self.references.push_back((base.clone(), reference.into()));
            }
        }
        // Data in annotations/const/enum is inspected only when explicitly referenced as a Schema.
        for key in [
            "properties",
            "patternProperties",
            "$defs",
            "dependentSchemas",
        ] {
            if let Some(map) = object.get(key).and_then(Value::as_object) {
                for child in map.values() {
                    self.walk(child, &base)?;
                }
            }
        }
        for key in ["allOf", "anyOf", "oneOf", "prefixItems"] {
            if let Some(array) = object.get(key).and_then(Value::as_array) {
                for child in array {
                    self.walk(child, &base)?;
                }
            }
        }
        for key in [
            "items",
            "contains",
            "additionalProperties",
            "unevaluatedProperties",
            "unevaluatedItems",
            "propertyNames",
            "not",
            "if",
            "then",
            "else",
            "contentSchema",
        ] {
            if let Some(child) = object.get(key) {
                self.walk(child, &base)?;
            }
        }
        Ok(())
    }
    fn finish(&mut self, registry: &Registry<'_>) -> Result<()> {
        let mut meta = None;
        while let Some((base, reference)) = self.references.pop_front() {
            let resolved = registry
                .resolver(uri::from_str(&base).map_err(reference_error)?)
                .lookup(&reference)
                .map_err(reference_error)?;
            let target = resolved.contents();
            if !self.seen.contains(&(std::ptr::from_ref(target) as usize)) {
                self.walk(target, resolved.resolver().base_uri().as_str())?;
                meta.get_or_insert_with(jsonschema::draft202012::meta::validator)
                    .validate(target)
                    .map_err(|_| {
                        unsupported("referenced target is not a valid Draft 2020-12 schema")
                    })?;
            }
        }
        Ok(())
    }
}

impl SchemaRegistry {
    pub fn new(resources: impl IntoIterator<Item = (String, Value)>) -> Result<Self> {
        Self::prepare(resources.into_iter().map(|(uri, value)| {
            crate::check_json(&value, 0)?;
            let bytes = serde_json::to_vec(&value)
                .map_err(|_| unsupported("schema cannot be serialized"))?
                .len();
            Ok((uri, value, bytes))
        }))
    }
    pub fn from_resources(resources: impl IntoIterator<Item = SchemaResource>) -> Result<Self> {
        Self::prepare(resources.into_iter().map(|r| {
            if r.schema_json.len() > MAX_SCHEMA_BYTES {
                return Err(limit("schema resource exceeds 1 MiB"));
            }
            Ok((
                r.uri,
                crate::parse_json(&r.schema_json)?,
                r.schema_json.len(),
            ))
        }))
    }
    pub fn from_json(text: &str) -> Result<Self> {
        if text.len() > MAX_SCHEMA_BUNDLE_BYTES {
            return Err(limit("schema bundle exceeds 16 MiB"));
        }
        let mut exceeded = false;
        let mut deserializer = serde_json::Deserializer::from_str(text);
        let resources = ResourceRecords(&mut exceeded)
            .deserialize(&mut deserializer)
            .map_err(|_| {
                if exceeded {
                    limit("schema resource count or source byte budget exceeded")
                } else {
                    Error::local(
                        "ConfigurationError",
                        "schema bundle must be a list of uri/schema_json string records",
                    )
                }
            })?;
        deserializer.end().map_err(|_| {
            Error::local("ConfigurationError", "schema bundle has trailing JSON data")
        })?;
        Self::from_resources(resources)
    }
    fn prepare(
        resources: impl IntoIterator<Item = Result<(String, Value, usize)>>,
    ) -> Result<Self> {
        let mut builder = Registry::new()
            .draft(Draft::Draft202012)
            .retriever(NoRetrieval);
        let mut audit = Audit::default();
        let mut stats = SchemaRegistryStats::default();
        let mut meta = None;
        let mut fingerprints = BTreeMap::new();
        for entry in resources {
            let (uri, value, bytes) = entry?;
            stats.resources += 1;
            if stats.resources > MAX_SCHEMA_RESOURCES || bytes > MAX_SCHEMA_BYTES {
                return Err(limit(
                    "schema resource count or document size exceeds its limit",
                ));
            }
            let uri_bytes = uri.len();
            let uri = document_uri(&uri)?;
            stats.bytes = stats
                .bytes
                .saturating_add(bytes)
                .saturating_add(uri_bytes)
                .saturating_add(128);
            if stats.bytes > MAX_SCHEMA_RESOURCE_BYTES {
                return Err(limit("schema resources exceed 8 MiB source budget"));
            }
            let value = Arc::new(value);
            fingerprints.insert(
                uri.clone(),
                crate::validator_cache::value_fingerprint(&value)?,
            );
            audit.identity(uri.clone(), std::ptr::from_ref(value.as_ref()) as usize)?;
            audit.walk(&value, &uri)?;
            meta.get_or_insert_with(jsonschema::draft202012::meta::validator)
                .validate(&value)
                .map_err(|_| {
                    unsupported("registered resource is not a valid Draft 2020-12 schema")
                })?;
            builder = builder.add(uri, value).map_err(reference_error)?;
        }
        if stats.resources == 0 {
            return Ok(Self::default());
        }
        let index = builder.prepare().map_err(reference_error)?;
        audit.finish(&index)?;
        stats.schema_nodes = audit.seen.len();
        stats.patterns = audit.patterns;
        stats.pattern_bytes = audit.pattern_bytes;
        let mut digest = Sha256::new();
        digest.update(b"mcpx:schema-registry:v1:");
        for (uri, fingerprint) in fingerprints {
            digest.update((uri.len() as u64).to_le_bytes());
            digest.update(uri.as_bytes());
            digest.update(fingerprint);
        }
        Ok(Self(Some(Arc::new(Prepared {
            index,
            audit,
            stats,
            fingerprint: digest.finalize().into(),
        }))))
    }
    pub fn stats(&self) -> SchemaRegistryStats {
        self.0.as_ref().map_or_else(Default::default, |p| p.stats)
    }
    pub(crate) fn fingerprint(&self) -> [u8; 32] {
        self.0.as_ref().map_or([0; 32], |p| p.fingerprint)
    }

    pub(crate) fn compile(&self, schema: &Value) -> Result<Validator> {
        crate::check_json(schema, 0)?;
        if !schema.is_object() {
            return Err(unsupported("tool schema must be an object"));
        }
        if serde_json::to_vec(schema)
            .map_err(|_| unsupported("schema cannot be serialized"))?
            .len()
            > MAX_SCHEMA_BYTES
        {
            return Err(limit("schema exceeds 1 MiB"));
        }
        // A server can publish a root already present in a closed host bundle.
        // Reuse an identical resource rather than introducing a second definition of its ID.
        let schema = if let Some((prepared, id)) = self
            .0
            .as_ref()
            .zip(schema.get("$id").and_then(Value::as_str))
        {
            let id = resolve(ROOT, id)?;
            if prepared.audit.ids.contains_key(&id) {
                let existing = prepared
                    .index
                    .resolver(uri::from_str(&id).map_err(reference_error)?)
                    .lookup("")
                    .map_err(reference_error)?;
                if existing.contents() != schema {
                    return Err(unsupported("schema resource identifiers conflict"));
                }
                Arc::new(json!({"$ref":id}))
            } else {
                Arc::new(schema.clone())
            }
        } else {
            Arc::new(schema.clone())
        };
        let mut audit = self
            .0
            .as_ref()
            .map_or_else(Audit::default, |p| p.audit.clone());
        audit.identity(ROOT.into(), std::ptr::from_ref(schema.as_ref()) as usize)?;
        audit.walk(&schema, ROOT)?;
        let builder = if let Some(prepared) = &self.0 {
            prepared
                .index
                .add(ROOT, schema.clone())
                .map_err(reference_error)?
        } else {
            Registry::new()
                .draft(Draft::Draft202012)
                .retriever(NoRetrieval)
                .add(ROOT, schema.clone())
                .map_err(reference_error)?
        };
        let index = builder
            .retriever(NoRetrieval)
            .prepare()
            .map_err(reference_error)?;
        audit.finish(&index)?;
        jsonschema::options()
            .with_draft(Draft::Draft202012)
            .with_base_uri(ROOT)
            .with_registry(&index)
            .with_retriever(NoRetrieval)
            .with_pattern_options(
                jsonschema::PatternOptions::fancy_regex()
                    .backtrack_limit(MAX_REGEX_BACKTRACKS)
                    .size_limit(MAX_REGEX_COMPILED_BYTES)
                    .dfa_size_limit(MAX_REGEX_DFA_BYTES),
            )
            .should_validate_formats(false)
            .build(&schema)
            .map_err(|_| unsupported("schema cannot be compiled with the supported validator"))
    }
}
