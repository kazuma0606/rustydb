use derive_more::Display;
use strum::EnumString;
use serde::{Deserialize, Serialize};
use std::fmt;


/// データベースでサポートされるデータ型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, EnumString, Serialize, Deserialize)]
pub enum DataType {
    #[strum(serialize = "INTEGER")]
    Integer,

    #[strum(serialize = "FLOAT")]
    Float,

    #[strum(serialize = "TEXT")]
    Text,

    #[strum(serialize = "BOOLEAN")]
    Boolean,

    #[strum(serialize = "TIMESTANMP")]
    Timestamp,

    #[strum(serialize = "NULL")]
    Null,
}

impl DataType {
    pub fn is_nullable(&self) -> bool {
        // 今はすべての型でNULLを許容することにします
        // 後で NOT NULL 制約を実装する際に変更します
        true
    }

    pub fn is_integer(&self) -> bool {
        matches!(self, DataType::Integer)
    }

    pub fn is_float(&self) -> bool {
        matches!(self, DataType::Float)
    }

    pub fn is_text(&self) -> bool {
        matches!(self, DataType::Text)
    }

    pub fn is_boolean(&self) -> bool {
        matches!(self, DataType::Boolean)
    }

    pub fn is_timestamp(&self) -> bool {
        matches!(self, DataType::Timestamp)
    }

    pub fn is_null(&self) -> bool {
        matches!(self, DataType::Null)
    }

    pub fn from_str(s: &str) -> Result<Self, String> {
        match s.to_uppercase().as_str() {
            "INTEGER" | "INT" => Ok(DataType::Integer),
            "FLOAT" | "REAL" | "DOUBLE" => Ok(DataType::Float),
            "TEXT" | "VARCHAR" | "CHAR" | "STRING" => Ok(DataType::Text),
            "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
            "TIMESTAMP" | "DATETIME" => Ok(DataType::Timestamp),
            "NULL" => Ok(DataType::Null),
            _ => Err(format!("Unsupported data type: {}", s)),
        }
    }
    
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Constraint {
    // 主キー制約
    PrimaryKey,
    // ユニーク制約
    Unique,
    // NOT NULL 制約
    NotNull,
    // デフォルト値
    Default(String),
}

impl fmt::Display for Constraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Constraint::PrimaryKey => write!(f, "PRIMARY KEY"),
            Constraint::Unique => write!(f, "UNIQUE"),
            Constraint::NotNull => write!(f, "NOT NULL"),
            Constraint::Default(value) => write!(f, "DEFAULT {}", value),
        }
    }
}