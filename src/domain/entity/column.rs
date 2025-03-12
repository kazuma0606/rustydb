use crate::domain::entity::data_type::{DataType, Constraint};
// use derive_more::Display;
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;
use std::fmt;

/// テーブルのカラムを表すエンティティ
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TypedBuilder)]
pub struct Column {
    /// カラム名
    pub name: String,

    /// データ型
    pub data_type: DataType,

    /// 制約
    #[builder(default)]
    pub constraints: Vec<Constraint>,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            constraints: Vec::new(),
        }
    }

    // primary key constraint
    pub fn primary_key(mut self) -> Self {
        self.constraints.push(Constraint::PrimaryKey);
        self.constraints.push(Constraint::NotNull); // primary key は not null 制約を持つ
        self
    }

    // NOT NULL constraint
    pub fn not_null(mut self) -> Self {
        if !self.constraints.contains(&Constraint::NotNull) {
            self.constraints.push(Constraint::NotNull);
        }
        self
    }

    // UNIQUE constraint
    pub fn unique(mut self) -> Self {
        if !self.constraints.contains(&Constraint::Unique) {
            self.constraints.push(Constraint::Unique);
        }
        self
    }

    //DEFAULT constraint
    pub fn with_default(mut self, value: impl Into<String>) -> Self {
        // 既存のDEFAULT制約を削除
        self.constraints.retain(|c| {
            !matches!(c, Constraint::Default(_))
        });

        // 新しいDEFAULT制約を追加
        self.constraints.push(Constraint::Default(value.into()));
        self
    }
/// このカラムがプライマリキーかどうかをチェックする
    pub fn is_primary_key(&self) -> bool {
        self.constraints.contains(&Constraint::PrimaryKey)
    }
/// このカラムがNOT NULL制約を持つかどうかをチェックする
    pub fn is_not_null(&self) -> bool {
        self.constraints.contains(&Constraint::NotNull)
    }
 /// このカラムがUNIQUE制約を持つかどうかをチェックする
    pub fn is_unique(&self) -> bool {
        self.constraints.contains(&Constraint::Unique)
    }
/// このカラムのDEFAULT値を取得する（存在する場合）
    pub fn default_value(&self) -> Option<&str> {
        self.constraints.iter().find_map(|c| {
            if let Constraint::Default(value) = c {
                Some(value.as_str())
            } else {
                None
            }
        })
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;
        for constraint in &self.constraints {
            write!(f, " {}", constraint)?;
        }
        Ok(())
    }
}