use super::*;
use wasip2::{http::types::FutureIncomingResponse, io::streams::OutputStream};

/// A nonblocking outgoing request. Child resources are declared before parents.
pub(super) struct Exchange {
    stream: Option<OutputStream>,
    outgoing: Option<OutgoingBody>,
    response: FutureIncomingResponse,
    body: Vec<u8>,
    offset: usize,
    flushing: bool,
}
pub(super) enum Step {
    Progress,
    Pending,
    Response(IncomingResponse),
}
impl Exchange {
    pub(super) fn new(
        method: Method,
        endpoint: &str,
        headers: &BTreeMap<String, String>,
        body: Vec<u8>,
        credential: &Credential,
        deadline: u64,
        notification: bool,
    ) -> Result<Self> {
        let (request, outgoing, stream, options) = (|| -> Result<_> {
            ensure(deadline)?;
            let url = url::Url::parse(endpoint)
                .map_err(|_| Error::new("InvalidEndpoint", "invalid HTTP endpoint"))?;
            let scheme = match url.scheme() {
                "http" => Scheme::Http,
                "https" => Scheme::Https,
                _ => {
                    return Err(Error::new(
                        "InvalidEndpoint",
                        "endpoint must use HTTP or HTTPS",
                    ));
                }
            };
            let mut values: Vec<_> = headers
                .iter()
                .map(|(k, v)| (k.clone(), v.as_bytes().to_vec()))
                .collect();
            if let Some(token) = credential.token() {
                values.push((
                    "authorization".into(),
                    format!("Bearer {token}").into_bytes(),
                ));
            }
            if values.len() > 256
                || values.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() > 65536
            {
                return Err(Error::new(
                    "LimitExceeded",
                    "HTTP request headers exceed budget",
                ));
            }
            let request =
                OutgoingRequest::new(Fields::from_list(&values).map_err(|_| host_error())?);
            request.set_method(&method).map_err(|_| host_error())?;
            request
                .set_scheme(Some(&scheme))
                .map_err(|_| host_error())?;
            request
                .set_authority(Some(
                    &url[url::Position::BeforeHost..url::Position::AfterPort],
                ))
                .map_err(|_| host_error())?;
            request
                .set_path_with_query(Some(
                    &url[url::Position::BeforePath..url::Position::AfterQuery],
                ))
                .map_err(|_| host_error())?;
            let outgoing = request.body().map_err(|_| host_error())?;
            // Finish an empty request before dispatch. A peer may answer a GET
            // immediately; flushing a needless output stream afterward races
            // that response and can report Closed despite a valid response.
            let (outgoing, stream) = if body.is_empty() {
                OutgoingBody::finish(outgoing, None).map_err(http_error)?;
                (None, None)
            } else {
                let stream = outgoing.write().map_err(|_| host_error())?;
                (Some(outgoing), Some(stream))
            };
            let options = RequestOptions::new();
            let remaining = deadline.saturating_sub(monotonic_clock::now());
            options
                .set_connect_timeout(Some(remaining))
                .map_err(|_| host_error())?;
            options
                .set_first_byte_timeout(Some(remaining))
                .map_err(|_| host_error())?;
            // Notification lifetime is not the ordinary operation deadline. The
            // host may still impose its own idle timeout when no override is set.
            options
                .set_between_bytes_timeout((!notification).then_some(remaining))
                .map_err(|_| host_error())?;
            Ok((request, outgoing, stream, options))
        })()
        .map_err(|error| error.with_context(mcpx_core::ErrorContext::not_sent()))?;
        let response = outgoing_handler::handle(request, Some(options)).map_err(http_error)?;
        Ok(Self {
            stream,
            outgoing,
            response,
            body,
            offset: 0,
            flushing: false,
        })
    }
    pub(super) fn step(&mut self) -> Result<Step> {
        if let Some(stream) = &self.stream {
            // Finish writing before waiting for flush completion, including
            // empty bodies and a final write that exhausted its whole permit.
            if self.offset == self.body.len() && !self.flushing {
                stream.flush().map_err(|_| host_error())?;
                self.flushing = true;
                return Ok(Step::Progress);
            }
            let permitted = stream.check_write().map_err(|_| host_error())?;
            if permitted == 0 {
                return Ok(Step::Pending);
            }
            if self.offset < self.body.len() {
                let length = permitted
                    .min(65536)
                    .min((self.body.len() - self.offset) as u64)
                    as usize;
                stream
                    .write(&self.body[self.offset..self.offset + length])
                    .map_err(|_| host_error())?;
                self.offset += length;
                return Ok(Step::Progress);
            }
            drop(self.stream.take());
            OutgoingBody::finish(self.outgoing.take().expect("unfinished request"), None)
                .map_err(http_error)?;
            self.body = Vec::new();
        }
        match self.response.get() {
            Some(value) => value
                .map_err(|_| host_error())?
                .map(Step::Response)
                .map_err(http_error),
            None => Ok(Step::Pending),
        }
    }
    pub(super) fn subscribe(&self) -> Pollable {
        match &self.stream {
            Some(stream) => stream.subscribe(),
            None => self.response.subscribe(),
        }
    }
}
