#![forbid(unsafe_code)]

pub mod auth;
pub mod blame;
pub mod blob;
pub mod cache;
pub mod cpu_jobs;
mod database;
pub mod derived_cache;
pub mod engine;
pub mod history;
pub mod hooks;
pub mod http;
pub mod metadata;
pub mod metrics;
pub mod process;
pub mod protocol;
pub mod receive;
pub mod receive_io;
mod repository;
pub mod scratch;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub fn invalid(message: &str) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message).into()
}
