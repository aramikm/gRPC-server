use std::fs;
use std::path::Path;

use serde::Deserialize;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub db: DatabaseConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub address: String,
    pub log_level: String,
    pub default_page_size: u32,
    pub max_page_size: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DatabaseConfig {
    pub path: String,
    pub max_open_files: i32,
    pub write_buffer_size: usize,
    pub compression: Compression,
    pub create_if_missing: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    None,
    #[default]
    Snappy,
    Lz4,
    Zstd,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            address: "0.0.0.0:50051".into(),
            log_level: "info".into(),
            default_page_size: 100,
            max_page_size: 1000,
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            path: "./data/rocksdb".into(),
            max_open_files: -1,
            write_buffer_size: 64 * 1024 * 1024,
            compression: Compression::Snappy,
            create_if_missing: true,
        }
    }
}

impl Config {
    /// Load configuration from `$CONFIG_PATH` (default `config.toml`).
    /// Returns defaults when the file does not exist.
    pub fn load() -> Result<Self> {
        let path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| "config.toml".to_string());
        if !Path::new(&path).exists() {
            return Ok(Self::default());
        }
        Self::from_file(&path)
    }

    pub fn from_file(path: &str) -> Result<Self> {
        let s = fs::read_to_string(path).map_err(|e| Error::Config(format!("read {path}: {e}")))?;
        Self::parse(&s)
    }

    pub fn parse(s: &str) -> Result<Self> {
        toml::from_str(s).map_err(|e| Error::Config(format!("parse: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sensible() {
        let c = Config::default();
        assert_eq!(c.server.address, "0.0.0.0:50051");
        assert!(c.server.default_page_size <= c.server.max_page_size);
        assert!(c.db.create_if_missing);
    }

    #[test]
    fn partial_toml_falls_back_to_defaults() {
        let c = Config::parse("[server]\nlog_level = \"debug\"\n").unwrap();
        assert_eq!(c.server.log_level, "debug");
        assert_eq!(c.server.address, "0.0.0.0:50051");
        assert_eq!(c.db.compression, Compression::Snappy);
    }

    #[test]
    fn compression_parses_lowercase() {
        let c = Config::parse("[db]\ncompression = \"zstd\"\n").unwrap();
        assert_eq!(c.db.compression, Compression::Zstd);
    }

    #[test]
    fn bad_toml_is_an_error() {
        assert!(Config::parse("this is not toml = = =").is_err());
    }
}
