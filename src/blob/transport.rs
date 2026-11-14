//! Process-local, signed streaming HTTP. Dropping a cancelled request also drops
//! its partial response; unread connections are not returned as reusable peers.
use super::Store;
use crate::{Result, invalid, process};
use aws_credential_types::Credentials;
use aws_sigv4::{
    http_request::{
        PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest, SigningSettings,
        UriPathNormalizationMode, sign,
    },
    sign::v4,
};
use reqwest::{
    Client, Method,
    header::{HeaderName, HeaderValue},
};
use std::{
    future::Future,
    io::Write,
    pin::pin,
    sync::OnceLock,
    time::{Duration, SystemTime},
};
use tokio::{
    runtime::{Builder, Runtime},
    sync::{Semaphore, mpsc},
};

struct Transport {
    runtime: Runtime,
    client: Client,
    active: Semaphore,
}

#[derive(Debug)]
pub(super) struct Status(pub(super) u16);
impl std::fmt::Display for Status {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(output, "S3 request returned HTTP {}", self.0)
    }
}
impl std::error::Error for Status {}

fn global() -> Result<&'static Transport> {
    static TRANSPORT: OnceLock<std::result::Result<Transport, String>> = OnceLock::new();
    TRANSPORT
        .get_or_init(|| Transport::new().map_err(|error| error.to_string()))
        .as_ref()
        .map_err(|message| invalid(message))
}

pub(super) fn initialize() -> Result<()> {
    global().map(|_| ())
}

impl Transport {
    fn new() -> Result<Self> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(4)
            .thread_name("git-rs-s3")
            .enable_all()
            .build()?;
        let client = {
            let _entered = runtime.enter();
            Client::builder()
                .tls_backend_native()
                .tls_version_min(reqwest::tls::Version::TLS_1_2)
                .http1_only()
                .no_proxy()
                .no_gzip()
                .no_brotli()
                .no_deflate()
                .no_zstd()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .connect_timeout(Duration::from_secs(5))
                .pool_idle_timeout(Duration::from_secs(30))
                .pool_max_idle_per_host(4)
                .build()
                .map_err(|_| invalid("S3 HTTP client initialization failed"))?
        };
        Ok(Self {
            runtime,
            client,
            active: Semaphore::new(8),
        })
    }

    fn run<T>(
        &self,
        seconds: u64,
        control: Option<&git_rs_core::Control>,
        future: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        process::check_operation()?;
        if let Some(control) = control {
            control.check()?;
        }
        let limit = Duration::from_secs(seconds);
        let remaining = if process::Operation::current().is_some() {
            process::remaining()?.min(limit)
        } else {
            limit
        };
        self.runtime.block_on(async {
            let work = async {
                let _permit = self.active.acquire().await?;
                future.await
            };
            let mut work = pin!(tokio::time::timeout(remaining, work));
            let mut cancellation = tokio::time::interval(Duration::from_millis(20));
            loop {
                tokio::select! {
                    biased;
                    _ = cancellation.tick() => {
                        process::check_operation()?;
                        if let Some(control) = control { control.check()?; }
                    },
                    result = &mut work => return result.map_err(|_| invalid("S3 request deadline exceeded"))?,
                }
            }
        })
    }
}

impl Store {
    /// Upload a borrowed admitted body through a bounded two-chunk channel.
    /// The sender and HTTP future are joined inside the same absolute timeout;
    /// neither a detached pump nor a file/memfd bridge survives cancellation.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn request_memory(
        &self,
        method: Method,
        url: &str,
        hash: Option<&str>,
        input: Option<&[u8]>,
        range: Option<(u64, u64, u64)>,
        output: &mut impl Write,
        limit: u64,
        seconds: u64,
    ) -> Result<()> {
        self.request_data(
            method, url, hash, input, range, output, limit, seconds, None,
        )
        .map(|_| ())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn request_memory_controlled(
        &self,
        method: Method,
        url: &str,
        hash: Option<&str>,
        input: Option<&[u8]>,
        range: Option<(u64, u64, u64)>,
        output: &mut impl Write,
        limit: u64,
        seconds: u64,
        control: &git_rs_core::Control,
    ) -> Result<()> {
        self.request_data(
            method,
            url,
            hash,
            input,
            range,
            output,
            limit,
            seconds,
            Some(control),
        )
        .map(|_| ())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn request_metadata(
        &self,
        method: Method,
        url: &str,
        hash: Option<&str>,
        input: Option<&[u8]>,
        output: &mut impl Write,
        control: &git_rs_core::Control,
    ) -> Result<Option<String>> {
        self.request_data(
            method,
            url,
            hash,
            input,
            None,
            output,
            16384,
            110,
            Some(control),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn request_data(
        &self,
        method: Method,
        url: &str,
        hash: Option<&str>,
        input: Option<&[u8]>,
        range: Option<(u64, u64, u64)>,
        output: &mut impl Write,
        limit: u64,
        seconds: u64,
        control: Option<&git_rs_core::Control>,
    ) -> Result<Option<String>> {
        if let Some((start, end, total)) = range
            && (start >= end
                || end > total
                || end - start != limit
                || method != Method::GET
                || input.is_some())
        {
            return Err(invalid("invalid immutable S3 byte range"));
        }
        let transport = global()?;
        transport.run(seconds, control, async {
            let identity =
                Credentials::new(&self.access, &self.secret, None, None, "git-rs-s3").into();
            let mut settings = SigningSettings::default();
            // S3 signs the actual object path, without generic URI normalization
            // or a second percent-encoding pass.
            settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
            settings.percent_encoding_mode = PercentEncodingMode::Single;
            settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
            let params = v4::SigningParams::builder()
                .identity(&identity)
                .region(&self.region)
                .name("s3")
                .time(SystemTime::now())
                .settings(settings)
                .build()
                .map_err(|_| invalid("S3 signing configuration failed"))?
                .into();
            let body = hash.map_or_else(SignableBody::empty, |value| {
                SignableBody::Precomputed(value.into())
            });
            let range_header = range.map(|(start, end, _)| format!("bytes={start}-{}", end - 1));
            let signable = SignableRequest::new(
                method.as_str(),
                url,
                range_header
                    .as_deref()
                    .map(|value| ("range", value))
                    .into_iter(),
                body,
            )
            .map_err(|_| invalid("S3 request signing failed"))?;
            let (instructions, _) = sign(signable, &params)
                .map_err(|_| invalid("S3 request signing failed"))?
                .into_parts();
            let mut request = transport.client.request(method, url);
            if let Some(range) = &range_header {
                request = request
                    .header(reqwest::header::RANGE, range)
                    .header(reqwest::header::ACCEPT_ENCODING, "identity");
            }
            for (name, value) in instructions.headers() {
                let name: HeaderName = name.parse()?;
                let mut value: HeaderValue = value.parse()?;
                value.set_sensitive(true);
                request = request.header(name, value);
            }
            let mut response = match input {
                Some(bytes) => {
                    let (send, receive) =
                        mpsc::channel::<std::result::Result<Vec<u8>, std::io::Error>>(1);
                    let stream = futures_util::stream::unfold(receive, |mut receive| async move {
                        receive.recv().await.map(|chunk| (chunk, receive))
                    });
                    request = request
                        .header(reqwest::header::CONTENT_LENGTH, bytes.len())
                        .body(reqwest::Body::wrap_stream(stream));
                    let sender = async move {
                        for chunk in bytes.chunks(65536) {
                            process::check_operation()?;
                            if let Some(control) = control {
                                control.check()?;
                            }
                            send.send(Ok(chunk.to_vec()))
                                .await
                                .map_err(|_| invalid("S3 upload body was not consumed"))?;
                        }
                        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
                    };
                    let response = async move {
                        request
                            .send()
                            .await
                            .map_err(|_| invalid("S3 HTTP request failed"))
                    };
                    tokio::try_join!(sender, response)?.1
                }
                None => request
                    .send()
                    .await
                    .map_err(|_| invalid("S3 HTTP request failed"))?,
            };
            if let Some((start, end, total)) = range {
                let expected = format!("bytes {start}-{}/{total}", end - 1);
                if response.status() != reqwest::StatusCode::PARTIAL_CONTENT
                    || response
                        .headers()
                        .get(reqwest::header::CONTENT_RANGE)
                        .and_then(|value| value.to_str().ok())
                        != Some(expected.as_str())
                    || response
                        .content_length()
                        .is_some_and(|length| length != end - start)
                    || response
                        .headers()
                        .get(reqwest::header::CONTENT_ENCODING)
                        .is_some_and(|value| value != "identity")
                {
                    return Err(invalid("S3 did not return the exact immutable byte range"));
                }
            }
            if !response.status().is_success() {
                return Err(Box::new(Status(response.status().as_u16())) as _);
            }
            if response.content_length().is_some_and(|size| size > limit) {
                return Err(invalid("S3 response length limit exceeded"));
            }
            let etag = response
                .headers()
                .get(reqwest::header::ETAG)
                .map(|value| -> Result<String> {
                    let value = value.to_str().map_err(|_| invalid("invalid S3 ETag"))?;
                    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control)
                    {
                        return Err(invalid("S3 ETag bound exceeded"));
                    }
                    Ok(value.to_owned())
                })
                .transpose()?;
            let mut written = 0u64;
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| invalid("S3 response read failed"))?
            {
                process::check_operation()?;
                if let Some(control) = control {
                    control.check()?;
                }
                written = written.saturating_add(chunk.len() as u64);
                if written > limit {
                    return Err(invalid("S3 response length limit exceeded"));
                }
                for part in chunk.chunks(32 * 1024) {
                    process::check_operation()?;
                    if let Some(control) = control {
                        control.check()?;
                    }
                    output.write_all(part)?;
                }
            }
            if range.is_some() && written != limit {
                return Err(invalid("S3 byte range was truncated"));
            }
            process::check_operation()?;
            if let Some(control) = control {
                control.check()?;
            }
            Ok(etag)
        })
    }
}
