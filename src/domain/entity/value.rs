use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use crate::domain::entity::data_type::DataType;
use thiserror::Error;

// 値型エラーの定義
#[derive(Error, Debug, PartialEq)]
pub enum ValueError {
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: DataType, actual: DataType },

    #[error("Cannot convert {0} to {1}")]
    ConversionError(String, String),

    #[error("NUll value not allowed")]
    NullValueNotAllowed,
}

// DB内の値の表現
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Integer(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
    Timestamp(DateTime<Utc>),
    Null,
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Integer(_) => DataType::Integer,
            Value::Float(_) => DataType::Float,
            Value::Text(_) => DataType::Text,
            Value::Boolean(_) => DataType::Boolean,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::Null => DataType::Null,
        }
    }

    //指定したデータ型に変換する
    pub fn cast_to(&self, target_type: DataType) -> Result<Value, ValueError> {
        match (self, target_type) {
            //NUllはどの型にも変換できる
            (Value::Null, _) => Ok(Value::Null),

            //同じ型への変換はそのまま返す
            (v, t) if v.data_type() == t => Ok(v.clone()),

            //整数から他の型への変換
            (Value::Integer(i), DataType::Float) => Ok(Value::Float(*i as f64)),
            (Value::Integer(i), DataType::Text) => Ok(Value::Text(i.to_string())),
            (Value::Integer(i), DataType::Boolean) => Ok(Value::Boolean(*i != 0)),

            //浮動小数点数から他の型への変換
            (Value::Float(f), DataType::Integer) => Ok(Value::Integer(*f as i64)),
            (Value::Float(f), DataType::Text) => Ok(Value::Text(f.to_string())),
            (Value::Float(f), DataType::Boolean) => Ok(Value::Boolean(*f != 0.0)),

            //文字列から他の型への変換
            (Value::Text(s), DataType::Integer) => s
                .parse::<i64>()
                .map(Value::Integer)
                .map_err(|_| ValueError::ConversionError(s.to_string(), "INTEGER".to_string())),
            (Value::Text(s), DataType::Float) => s
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| ValueError::ConversionError(s.to_string(), "FLOAT".to_string())),
            (Value::Text(s), DataType::Boolean) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" | "y" => Ok(Value::Boolean(true)),
                "false" | "0" | "no" | "n" => Ok(Value::Boolean(false)),
                _ => Err(ValueError::ConversionError(s.to_string(), "BOOLEAN".to_string())),
            },

            // その他の変換はエラー
            (value, target) => Err(ValueError::TypeMismatch {
                expected: target,
                actual: value.data_type(),
            }),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(n) => write!(f, "{}", n),
            Value::Text(s) => write!(f, "{}", s),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Timestamp(dt) => write!(f, "{}", dt),
            Value::Null => write!(f, "NULL"),
        }
    }
}

impl From<i64> for Value {
    fn from(val: i64) -> Self {
        Value::Integer(val)
    }
}
impl From<f64> for Value {
    fn from(val: f64) -> Self {
        Value::Float(val)
    }
}
impl From<String> for Value {
    fn from(val: String) -> Self {
        Value::Text(val)
    }
}
impl From<bool> for Value {
    fn from(val: bool) -> Self {
        Value::Boolean(val)
    }
}
impl From<DateTime<Utc>> for Value {
    fn from(val: DateTime<Utc>) -> Self {
        Value::Timestamp(val)
    }
}
