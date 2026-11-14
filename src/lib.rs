#![forbid(unsafe_code)]

pub mod auth;
pub mod blame;
pub mod blob;
pub mod cache;
pub mod cpu_jobs;
mod database;
pub mod derived_cache;
pub mod engine;
pub mod health;
pub mod history;
pub mod http;
pub mod maintenance;
pub mod metadata;
pub mod metrics;
mod object_source;
pub mod process;
pub mod protocol;
pub mod receive;
pub mod receive_io;
mod repository;
pub mod scratch;
pub mod serving_socket;
pub mod upload;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub fn invalid(message: &str) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message).into()
}
