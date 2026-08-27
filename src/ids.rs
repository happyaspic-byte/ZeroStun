use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{Error, Result};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentId([u8; 32]);

impl ContentId {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        if s.len() != 64 {
            return Err(Error::InvalidIdentifier(format!(
                "ContentId hex must be 64 characters, got {}",
                s.len()
            )));
        }
        let mut raw = [0u8; 32];
        hex::decode_to_slice(s, &mut raw)
            .map_err(|e| Error::InvalidIdentifier(format!("invalid hex in ContentId: {e}")))?;
        Ok(Self(raw))
    }
}

impl fmt::Debug for ContentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContentId({})", self.to_hex())
    }
}

impl fmt::Display for ContentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl FromStr for ContentId {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

impl Serialize for ContentId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for ContentId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ContentId::parse(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RootHash([u8; 32]);

impl RootHash {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        if s.len() != 64 {
            return Err(Error::InvalidIdentifier(format!(
                "RootHash hex must be 64 characters, got {}",
                s.len()
            )));
        }
        let mut raw = [0u8; 32];
        hex::decode_to_slice(s, &mut raw)
            .map_err(|e| Error::InvalidIdentifier(format!("invalid hex in RootHash: {e}")))?;
        Ok(Self(raw))
    }
}

impl fmt::Debug for RootHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RootHash({})", self.to_hex())
    }
}

impl fmt::Display for RootHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl FromStr for RootHash {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

impl Serialize for RootHash {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for RootHash {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        RootHash::parse(&s).map_err(serde::de::Error::custom)
    }
}

pub fn generate_backup_id() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let mut rnd = [0u8; 4];
    let _ = getrandom::fill(&mut rnd);
    format!("bkp-{now:013x}-{}", hex::encode(rnd))
}

pub fn validate_backup_id(id: &str) -> Result<()> {
    let id = id.trim();
    if id.is_empty() || id.len() > 64 {
        return Err(Error::InvalidIdentifier(
            "backup ID length must be between 1 and 64".to_string(),
        ));
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(Error::InvalidIdentifier(format!(
            "backup ID '{id}' contains invalid characters"
        )));
    }
    Ok(())
}
