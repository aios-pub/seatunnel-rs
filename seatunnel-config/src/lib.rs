/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Configuration parsing for SeaTunnel.
//!
//! Supports YAML, TOML, and HOCON formats.

use std::error::Error;
use std::path::Path;

pub mod hocon;
pub mod job_config;
pub mod parser;

pub use hocon::{get_dot_path, parse_hocon};
pub use job_config::{JobConfig, JobMode, SchemaConfig, SinkConfig, SourceConfig, TransformConfig};
pub use parser::{ConfigFormat, ParsedConfig, parse_config_file};

/// Parse a config file into a typed JobConfig.
/// Auto-detects format from extension: .toml -> TOML, .conf -> HOCON, else -> YAML.
pub fn load_config(path: &Path) -> Result<JobConfig, Box<dyn Error>> {
    let contents = std::fs::read_to_string(path)?;
    let format = detect_format(path);
    let parsed = parse_config_file(&contents, format)?;
    JobConfig::from_parsed(parsed)
}

fn detect_format(path: &Path) -> ConfigFormat {
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        match ext {
            "toml" => ConfigFormat::TOML,
            "conf" | "hocon" => ConfigFormat::HOCON,
            "yaml" | "yml" => ConfigFormat::YAML,
            _ => ConfigFormat::YAML,
        }
    } else {
        ConfigFormat::YAML
    }
}

/// Load a config string with the given format.
pub fn load_config_from_str(
    content: &str,
    format: ConfigFormat,
) -> Result<JobConfig, Box<dyn Error>> {
    let parsed = parse_config_file(content, format)?;
    JobConfig::from_parsed(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    #[test]
    fn test_detect_format_toml() {
        assert_eq!(detect_format(Path::new("config.toml")), ConfigFormat::TOML);
    }

    #[test]
    fn test_detect_format_hocon() {
        assert_eq!(detect_format(Path::new("job.conf")), ConfigFormat::HOCON);
        assert_eq!(detect_format(Path::new("job.hocon")), ConfigFormat::HOCON);
    }

    #[test]
    fn test_load_hocon() {
        let content = "
            env {
              job.name = \"demo\"
              job.mode = \"streaming\"
              parallelism.default = 4
            }
            source {
              fake {
                row.num = 10
              }
            }
            sink {
              console {
                format = \"json\"
              }
            }
        ";
        let config = load_config_from_str(content, ConfigFormat::HOCON).unwrap();
        assert!(config.env.contains_key("job"));
        assert_eq!(config.sources.len(), 1);
        assert_eq!(config.sinks.len(), 1);
    }

    #[test]
    fn test_load_from_file() {
        let mut f = fs::File::create("/tmp/test_seatunnel.conf").unwrap();
        write!(
            f,
            "env {{ job.name = \"file-test\" }}\nsource {{ kafka {{ topic = \"t\" }} }}\nsink {{ console {{ format = \"json\" }} }}"
        )
        .unwrap();
        let config = load_config(Path::new("/tmp/test_seatunnel.conf")).unwrap();
        assert!(config.env.contains_key("job"));
        fs::remove_file("/tmp/test_seatunnel.conf").ok();
    }
}
