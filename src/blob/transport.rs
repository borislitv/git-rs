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
    fs::File,
    future::Future,
    io::Write,
    pin::pin,
    sync::OnceLock,
    time::{Duration, SystemTime},
};
use tokio::{
    io::AsyncReadExt,
    runtime::{Builder, Runtime},
    sync::Semaphore,
};
use tokio_util::io::ReaderStream;

struct Transport {
    runtime: Runtime,
    client: Client,
    active: Semaphore,
}

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

    fn run<T>(&self, seconds: u64, future: impl Future<Output = Result<T>>) -> Result<T> {
        process::check_operation()?;
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
                    _ = cancellation.tick() => process::check_operation()?,
                    result = &mut work => return result.map_err(|_| invalid("S3 request deadline exceeded"))?,
                }
            }
        })
    }
}

impl Store {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn request(
        &self,
        method: Method,
        url: &str,
        hash: Option<&str>,
        input: Option<File>,
        output: &mut impl Write,
        limit: u64,
        seconds: u64,
    ) -> Result<()> {
        let transport = global()?;
        transport.run(seconds, async {
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
            let signable = SignableRequest::new(method.as_str(), url, std::iter::empty(), body)
                .map_err(|_| invalid("S3 request signing failed"))?;
            let (instructions, _) = sign(signable, &params)
                .map_err(|_| invalid("S3 request signing failed"))?
                .into_parts();
            let mut request = transport.client.request(method, url);
            for (name, value) in instructions.headers() {
                let name: HeaderName = name.parse()?;
                let mut value: HeaderValue = value.parse()?;
                value.set_sensitive(true);
                request = request.header(name, value);
            }
            if let Some(file) = input {
                let size = file.metadata()?.len();
                let body =
                    ReaderStream::with_capacity(tokio::fs::File::from_std(file).take(size), 65536);
                request = request
                    .header(reqwest::header::CONTENT_LENGTH, size)
                    .body(reqwest::Body::wrap_stream(body));
            }
            let mut response = request
                .send()
                .await
                .map_err(|_| invalid("S3 HTTP request failed"))?;
            if !response.status().is_success() {
                return Err(invalid(&format!(
                    "S3 request returned HTTP {}",
                    response.status().as_u16()
                )));
            }
            if response.content_length().is_some_and(|size| size > limit) {
                return Err(invalid("S3 response length limit exceeded"));
            }
            let mut written = 0u64;
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| invalid("S3 response read failed"))?
            {
                process::check_operation()?;
                written = written.saturating_add(chunk.len() as u64);
                if written > limit {
                    return Err(invalid("S3 response length limit exceeded"));
                }
                output.write_all(&chunk)?;
            }
            process::check_operation()
        })
    }
}
