//! Test-only WASIp2 embedder with an explicit HTTPS origin and PEM trust store.
use futures_util::TryStreamExt;
use http_body_util::{BodyDataStream, BodyExt, StreamBody};
use std::{error::Error as _, future::Future, io::Read as _, path::Path, time::Duration};
use wasmtime::{
    Engine, Result, Store, bail,
    component::{Component, Linker, ResourceTable},
    ensure,
};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};
use wasmtime_wasi_http::{
    Error, RequestOptions, WasiBody, WasiHttpCtx, WasiHttpCtxView, WasiHttpHooks, WasiHttpView,
};

type IoFuture = Box<dyn Future<Output = Result<(), Error>> + Send>;
type ResponseFuture =
    Box<dyn Future<Output = Result<(http::Response<WasiBody>, IoFuture), Error>> + Send>;
struct Https {
    origin: String,
    certificates: Vec<reqwest::Certificate>,
}
fn network_error(error: reqwest::Error, body: bool) -> Error {
    if error.is_timeout() {
        return if body {
            Error::HttpResponseTimeout
        } else {
            Error::ConnectionTimeout
        };
    }
    let mut source = error.source();
    while let Some(cause) = source {
        if cause
            .downcast_ref::<rustls::Error>()
            .is_some_and(|error| matches!(error, rustls::Error::InvalidCertificate(_)))
        {
            return Error::TlsCertificateError;
        }
        if let Some(error) = cause.downcast_ref::<std::io::Error>() {
            match error.kind() {
                std::io::ErrorKind::ConnectionRefused => return Error::ConnectionRefused,
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted => {
                    return Error::ConnectionTerminated;
                }
                _ => {}
            }
        }
        source = cause.source();
    }
    Error::HttpProtocolError
}
impl WasiHttpHooks for Https {
    fn is_supported_scheme(&mut self, scheme: &http::uri::Scheme) -> bool {
        *scheme == http::uri::Scheme::HTTPS
    }
    fn send_request(
        &mut self,
        request: http::Request<WasiBody>,
        options: Option<RequestOptions>,
        _body_io: IoFuture,
    ) -> ResponseFuture {
        let allowed = self.origin.clone();
        let certificates = self.certificates.clone();
        Box::new(async move {
            let options = options.unwrap_or_default();
            let url = reqwest::Url::parse(&request.uri().to_string())
                .map_err(|_| Error::HttpRequestUriInvalid)?;
            if url.scheme() != "https" || url.origin().ascii_serialization() != allowed {
                return Err(Error::HttpRequestDenied);
            }
            let mut client = reqwest::Client::builder()
                .no_proxy()
                .http1_only()
                .redirect(reqwest::redirect::Policy::none())
                .tls_certs_only(certificates);
            if let Some(timeout) = options.connect_timeout {
                client = client.connect_timeout(timeout);
            }
            if let Some(timeout) = options.between_bytes_timeout {
                client = client.read_timeout(timeout);
            }
            let client = client.build().map_err(|_| Error::ConfigurationError)?;
            let (parts, body) = request.into_parts();
            let request = client
                .request(parts.method, url)
                .headers(parts.headers)
                .body(reqwest::Body::wrap_stream(BodyDataStream::new(body)));
            let response = tokio::time::timeout(
                options
                    .first_byte_timeout
                    .unwrap_or(Duration::from_secs(60)),
                request.send(),
            )
            .await
            .map_err(|_| Error::ConnectionReadTimeout)?
            .map_err(|error| network_error(error, false))?;
            let mut output = http::Response::builder()
                .status(response.status())
                .version(response.version());
            *output.headers_mut().ok_or(Error::HttpProtocolError)? = response.headers().clone();
            let stream = response
                .bytes_stream()
                .map_ok(http_body::Frame::data)
                .map_err(|error| network_error(error, true));
            let body = StreamBody::new(stream).boxed_unsync();
            let response = output.body(body).map_err(|_| Error::HttpProtocolError)?;
            Ok((response, Box::new(async { Ok(()) }) as IoFuture))
        })
    }
}
struct State {
    table: ResourceTable,
    wasi: WasiCtx,
    http: WasiHttpCtx,
    hooks: Https,
}
impl WasiView for State {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}
impl WasiHttpView for State {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.http,
            table: &mut self.table,
            hooks: &mut self.hooks,
        }
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() < 5 || args[0] != "--ca-pem" || args[2] != "--allow-origin" {
        bail!(
            "usage: mcpx-wasi-https-host --ca-pem PATH --allow-origin HTTPS_ORIGIN MODULE [ARGS...]"
        );
    }
    let ca = Path::new(&args[1]);
    let mut pem = Vec::new();
    std::fs::File::open(ca)?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut pem)?;
    ensure!(pem.len() <= 1024 * 1024, "CA bundle exceeds 1 MiB");
    let certificates = reqwest::Certificate::from_pem_bundle(&pem)?;
    ensure!(
        !certificates.is_empty() && certificates.len() <= 64,
        "CA bundle requires 1..64 certificates"
    );
    let origin = reqwest::Url::parse(&args[3])?;
    ensure!(
        origin.scheme() == "https"
            && origin.path() == "/"
            && origin.query().is_none()
            && origin.fragment().is_none()
            && origin.username().is_empty()
            && origin.password().is_none(),
        "allow-origin must be an HTTPS origin"
    );
    let engine = Engine::default();
    let component = Component::from_file(&engine, &args[4])
        .map_err(|error| error.context("compile WASI component"))?;
    let mut linker = Linker::new(&engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
    wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker)?;
    let wasi = WasiCtxBuilder::new()
        .inherit_stdout()
        .inherit_stderr()
        .args(&args[4..])
        .build();
    let mut store = Store::new(
        &engine,
        State {
            table: ResourceTable::new(),
            wasi,
            http: WasiHttpCtx::new(),
            hooks: Https {
                origin: origin.origin().ascii_serialization(),
                certificates,
            },
        },
    );
    let command =
        wasmtime_wasi::p2::bindings::Command::instantiate_async(&mut store, &component, &linker)
            .await?;
    ensure!(
        command.wasi_cli_run().call_run(&mut store).await?.is_ok(),
        "WASI guest returned failure"
    );
    Ok(())
}
