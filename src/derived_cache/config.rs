use crate::{Result, invalid};

#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub namespace: String,
    pub owner: String,
    pub inline_bytes: usize,
    pub max_payload_bytes: usize,
    pub memory_bytes: usize,
    pub namespace_bytes: i64,
    pub namespace_rows: i32,
    pub connections: usize,
    pub ttl_seconds: i32,
    pub lease_seconds: i32,
    pub job_seconds: i32,
    pub read_seconds: i32,
    pub grace_seconds: i32,
    pub batch_rows: i32,
}

pub(crate) fn uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
}

pub(crate) fn identifier() -> Result<String> {
    let value = crate::scratch::identifier()?;
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &value[..8],
        &value[8..12],
        &value[12..16],
        &value[16..20],
        &value[20..]
    ))
}

impl CacheConfig {
    pub fn new(namespace: String, owner: String) -> Self {
        Self {
            namespace,
            owner,
            inline_bytes: 256 * 1024,
            max_payload_bytes: 8 * 1024 * 1024,
            memory_bytes: 64 * 1024 * 1024,
            namespace_bytes: 1024 * 1024 * 1024,
            namespace_rows: 10000,
            connections: 4,
            ttl_seconds: 3600,
            lease_seconds: 15,
            job_seconds: 60,
            read_seconds: 30,
            grace_seconds: 30,
            batch_rows: 64,
        }
    }

    pub fn from_env() -> Result<Self> {
        let mut config = Self::new(
            std::env::var("GIT_RS_CACHE_NAMESPACE")?,
            std::env::var("GIT_RS_CACHE_OWNER")?,
        );
        macro_rules! setting {
            ($field:ident, $name:literal) => {
                if let Ok(value) = std::env::var(concat!("GIT_RS_CACHE_", $name)) {
                    config.$field = value
                        .parse()
                        .map_err(|_| invalid(concat!("invalid cache ", $name)))?;
                }
            };
        }
        setting!(inline_bytes, "INLINE_BYTES");
        setting!(max_payload_bytes, "MAX_PAYLOAD_BYTES");
        setting!(memory_bytes, "MEMORY_BYTES");
        setting!(namespace_bytes, "NAMESPACE_BYTES");
        setting!(namespace_rows, "NAMESPACE_ROWS");
        setting!(connections, "CONNECTIONS");
        setting!(ttl_seconds, "TTL_SECONDS");
        setting!(lease_seconds, "LEASE_SECONDS");
        setting!(job_seconds, "JOB_SECONDS");
        setting!(read_seconds, "READ_SECONDS");
        setting!(grace_seconds, "GRACE_SECONDS");
        setting!(batch_rows, "BATCH_ROWS");
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if !uuid(&self.namespace)
            || !uuid(&self.owner)
            || !(1..=8 * 1024 * 1024).contains(&self.max_payload_bytes)
            || self.inline_bytes > self.max_payload_bytes
            || !(self.max_payload_bytes * 6 + 65536..=1024 * 1024 * 1024)
                .contains(&self.memory_bytes)
            || !(self.max_payload_bytes as i64..=1_099_511_627_776).contains(&self.namespace_bytes)
            || !(16..=1_000_000).contains(&self.namespace_rows)
            || !(1..=8).contains(&self.connections)
            || !(1..=86400).contains(&self.ttl_seconds)
            || !(1..=60).contains(&self.lease_seconds)
            || !(self.lease_seconds..=120).contains(&self.job_seconds)
            || !(1..=120).contains(&self.read_seconds)
            || !(1..=86400).contains(&self.grace_seconds)
            || !(1..=100).contains(&self.batch_rows)
        {
            return Err(invalid(
                "invalid artifact cache limits or namespace identity",
            ));
        }
        Ok(())
    }

    pub fn object_prefix(&self) -> String {
        format!("cache/{}/", self.namespace)
    }
}
