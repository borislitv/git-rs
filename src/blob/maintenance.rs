//! Bounded multipart reconciliation for permanently retired source identities.
//! Listing discovers upload identities; PostgreSQL still authorizes each abort.

use super::{Store, validate_key, xml};
use crate::{Result, invalid};
use git_rs_core::Control;
use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct PendingUpload {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "UploadId")]
    pub upload: String,
}

#[derive(Deserialize)]
struct Listing {
    #[serde(rename = "Upload", default)]
    uploads: Vec<PendingUpload>,
    #[serde(rename = "IsTruncated")]
    truncated: bool,
    #[serde(rename = "NextKeyMarker", default)]
    next_key: String,
    #[serde(rename = "NextUploadIdMarker", default)]
    next_upload: String,
}

pub(crate) struct UploadPage {
    pub uploads: Vec<PendingUpload>,
    pub next_key: String,
    pub next_upload: String,
}

fn upload_id(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 1024 || value.chars().any(char::is_control) {
        return Err(invalid("invalid multipart upload identity"));
    }
    Ok(())
}

impl Store {
    pub(crate) fn pending_uploads(
        &self,
        repo: &str,
        key: &str,
        upload: &str,
        control: &Control,
    ) -> Result<UploadPage> {
        let prefix = format!("repos/{repo}/operations/");
        validate_key(&prefix)?;
        if !key.is_empty() {
            validate_key(key)?;
            if !key.starts_with(&prefix) {
                return Err(invalid("multipart cursor escaped repository"));
            }
        }
        if !upload.is_empty() {
            upload_id(upload)?;
        }
        let mut url = reqwest::Url::parse(&format!("{}/{}/", self.endpoint, self.bucket))?;
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("uploads", "")
                .append_pair("prefix", &prefix)
                .append_pair("max-uploads", "4");
            if !key.is_empty() {
                query.append_pair("key-marker", key);
            }
            if !upload.is_empty() {
                query.append_pair("upload-id-marker", upload);
            }
        }
        let mut body = Vec::with_capacity(16384);
        self.request_metadata(
            reqwest::Method::GET,
            url.as_str(),
            None,
            None,
            &mut body,
            control,
        )?;
        let xml = xml::document(&body, b"ListMultipartUploadsResult", true)?;
        let listing: Listing = quick_xml::de::from_str(xml)?;
        if listing.uploads.len() > 4 {
            return Err(invalid("multipart listing count exceeded"));
        }
        for pending in &listing.uploads {
            control.check()?;
            validate_key(&pending.key)?;
            upload_id(&pending.upload)?;
            if !pending.key.starts_with(&prefix) {
                return Err(invalid("multipart listing escaped repository"));
            }
        }
        let (next_key, next_upload) = if listing.truncated {
            validate_key(&listing.next_key)?;
            upload_id(&listing.next_upload)?;
            // Keys are byte-ordered, but UploadId is opaque; uploads of one key
            // are ordered by initiation, not by lexical UploadId.
            if !listing.next_key.starts_with(&prefix)
                || listing.next_key.as_str() < key
                || (listing.next_key == key && listing.next_upload == upload)
            {
                return Err(invalid("multipart listing cursor did not advance"));
            }
            (listing.next_key, listing.next_upload)
        } else {
            (String::new(), String::new())
        };
        Ok(UploadPage {
            uploads: listing.uploads,
            next_key,
            next_upload,
        })
    }

    pub(crate) fn abort_retired_upload(
        &self,
        key: &str,
        upload: &str,
        control: &Control,
    ) -> Result<()> {
        validate_key(key)?;
        upload_id(upload)?;
        let mut url = reqwest::Url::parse(&format!("{}/{}/{}", self.endpoint, self.bucket, key))?;
        url.query_pairs_mut().append_pair("uploadId", upload);
        match self.request_metadata(
            reqwest::Method::DELETE,
            url.as_str(),
            None,
            None,
            &mut std::io::sink(),
            control,
        ) {
            Ok(_) => Ok(()),
            // A raced Complete/Abort may already have removed the upload. Its
            // object/intents/tombstone still require normal repeated deletion.
            Err(error)
                if error
                    .downcast_ref::<super::transport::Status>()
                    .is_some_and(|status| status.0 == 404) =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }
}
