use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// PostgreSQL server version parsed from `SELECT version()`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl std::fmt::Display for PgVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl PgVersion {
    /// Parse the output of `SELECT version()`.
    ///
    /// Expects a string like `"PostgreSQL 17.2 on x86_64-..."` or
    /// `"PostgreSQL 16.1.3 (Debian 16.1.3-1) on ..."`.
    pub fn parse_from_version_string(version_str: &str) -> Result<Self> {
        // Find the first token that looks like a version number (digits and dots).
        let version_token = version_str
            .split_whitespace()
            .find(|token| {
                let t = token.trim_end_matches(',');
                !t.is_empty()
                    && t.chars().next().is_some_and(|c| c.is_ascii_digit())
                    && t.contains('.')
            })
            .ok_or_else(|| {
                Error::VersionParse(format!("no version token found in: {version_str}"))
            })?;

        let version_token = version_token.trim_end_matches(',');
        let parts: Vec<&str> = version_token.split('.').collect();

        let parse_part = |s: &str| -> Result<u32> {
            // Strip any trailing non-digit characters (e.g. "2beta1" -> 2)
            let numeric: String = s.chars().take_while(|c| c.is_ascii_digit()).collect();
            numeric
                .parse()
                .map_err(|_| Error::VersionParse(format!("invalid version component: {s}")))
        };

        let major = parts
            .first()
            .ok_or_else(|| Error::VersionParse("missing major version".into()))
            .and_then(|s| parse_part(s))?;
        let minor = parts
            .get(1)
            .map(|s| parse_part(s))
            .transpose()?
            .unwrap_or(0);
        let patch = parts
            .get(2)
            .map(|s| parse_part(s))
            .transpose()?
            .unwrap_or(0);

        Ok(PgVersion {
            major,
            minor,
            patch,
        })
    }
}
