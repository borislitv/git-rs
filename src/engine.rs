use crate::{Result, blob::Store, cache::Cache, http, invalid, process};
use std::{io::Write, sync::Arc};

pub struct Engine {
    pub blame: crate::blame::Blame,
    pub store: Store,
    pub cache: Arc<Cache>,
    pub operation_seconds: u64,
}

impl Engine {
    /// Direct Rust receive entrypoint. The transport owns the absolute output
    /// interrupt for the entire call, including cleanup and terminal framing.
    pub fn receive_ssh(
        &self,
        slug: &str,
        principal: &str,
        protocol: &str,
        input: std::os::fd::OwnedFd,
        output: &mut crate::protocol::Framed<'_, impl Write>,
    ) -> Result<()> {
        let mut receipt = None;
        let result = (|| {
            if !["", "version=0", "version=1", "version=2"].contains(&protocol) {
                return Err(invalid("unsupported Git protocol header"));
            }
            let operation = process::Operation::current()
                .ok_or_else(|| invalid("receive requires a bounded transport operation"))?;
            let cpu = self.blame.cpu();
            let admission = cpu
                .try_admit(operation)?
                .ok_or_else(|| invalid("receive CPU admission busy"))?;
            let session =
                crate::receive::ReceiveSession::acquire(admission, self, slug, principal)?;
            session.advertise(false, output)?;
            output.flush()?;
            let prepared = session
                .run_ssh(input)?
                .ok_or_else(|| invalid("receive interrupted before completion"))?;
            let receipt = receipt.insert(crate::receive::publish(prepared, &self.store));
            receipt.write_status(output)?;
            output.flush()?;
            receipt
                .publication()
                .map(|_| ())
                .map_err(|_| invalid("receive publication rejected"))
        })();
        // Receipt/source/admission remain live even when writing status failed.
        let finished = output.finish(result.is_ok());
        if let Some(receipt) = receipt {
            self.warm_after_confirmed(slug, principal, receipt);
        }
        finished?;
        result
    }

    /// Preserve the parsed BufReader and all bytes prefetched from the socket.
    pub fn receive_http(
        &self,
        request: &http::Request,
        principal: &str,
        input: &mut impl std::io::BufRead,
        output: &mut impl Write,
    ) -> Result<()> {
        let operation = process::Operation::current()
            .ok_or_else(|| invalid("receive requires a bounded transport operation"))?;
        let cpu = self.blame.cpu();
        let admission = cpu
            .try_admit(operation)?
            .ok_or_else(|| invalid("receive CPU admission busy"))?;
        let http::Action::Git(cgi) = &request.action else {
            return Err(invalid("receive requires a Git HTTP action"));
        };
        if cgi.method == "GET" {
            let discovery =
                crate::receive::ReceiveDiscovery::acquire_http(admission, request, principal)?;
            output.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-git-receive-pack-advertisement\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n")?;
            discovery.write(output)?;
            output.flush()?;
            return Ok(());
        }
        let session =
            crate::receive::ReceiveSession::acquire(admission, self, &request.slug, principal)?;
        let prepared = session
            .run_http(request, input)?
            .ok_or_else(|| invalid("receive interrupted before completion"))?;
        let receipt = crate::receive::publish(prepared, &self.store);
        let result = (|| {
            output.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-git-receive-pack-result\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n")?;
            receipt.write_status(output)?;
            output.flush()?;
            receipt
                .publication()
                .map(|_| ())
                .map_err(|_| invalid("receive publication rejected"))
        })();
        self.warm_after_confirmed(&request.slug, principal, receipt);
        result
    }
}

/// Validated Smart HTTP route/body metadata, without CGI process execution.
pub struct Cgi {
    pub path: String,
    pub method: String,
    pub query: String,
    pub content_type: String,
    pub encoding: String,
    pub length: Option<u64>,
}
