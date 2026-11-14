use crate::{Error, OutputValidation, Result, ToolResponse};
impl ToolResponse {
    /// Deserialize only structuredContent, retaining this entire response on
    /// both success and failure. Text content is never interpreted as JSON.
    /// Serde's requested Rust type controls field and numeric conversions.
    pub fn error_context(&self) -> crate::ErrorContext {
        crate::ErrorContext {
            operation_id: self.operation_id,
            queue_operation_id: self.queue_operation_id,
            execution_certainty: crate::ExecutionCertainty::ResultReceived,
        }
    }
    pub fn deserialize<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        self.deserialize_inner()
            .map_err(|error| error.with_context(self.error_context()))
    }
    fn deserialize_inner<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        if self.is_error {
            return Err(Error::new(
                "ToolFailure",
                "tool returned a business failure",
            ));
        }
        if matches!(self.output_validation, OutputValidation::Indeterminate) {
            return Err(Error::new(
                "OutputValidationLimitExceeded",
                "tool output schema validation could not complete within regex engine limits",
            ));
        }
        if matches!(self.output_validation, OutputValidation::Invalid) {
            return Err(Error::new(
                "OutputValidation",
                "tool output does not satisfy the remote Schema",
            ));
        }
        let value = self
            .raw
            .get("structuredContent")
            .filter(|value| value.is_object())
            .ok_or_else(|| {
                Error::new(
                    "StructuredContentMissing",
                    "tool response has no structured object",
                )
            })?;
        crate::check_json(value, 0)?;
        T::deserialize(value).map_err(|_| {
            Error::new(
                "DeserializationFailed",
                "structured tool result does not match the requested Rust type",
            )
        })
    }
}
