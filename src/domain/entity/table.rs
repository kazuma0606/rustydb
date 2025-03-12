use crate::domain::entity::column::Column;
// use crate::domain::entity::data_type::{DataType, Constraint};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TableError {
    #[error("Column '{0}' already exists in table")]
    ColumnAlreadyExists(String),
    
    #[error("Column '{0}' not found in table")]
    ColumnNotFound(String),
    
    #[error("Table must have at least one column")]
    NoColumns,
    
    #[error("Multiple primary keys not allowed")]
    MultiplePrimaryKeys,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Table {
    // table name
    pub name: String,

    // table columns
    pub columns: Vec<Column>,
}

impl  Table {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
        }
    }

    pub fn add_column(&mut self, column: Column) -> Result<(), TableError> {
         // 同名のカラムが既に存在するかチェック
        if self.get_column(&column.name).is_some() {
            return Err(TableError::ColumnAlreadyExists(column.name));
        }
        // 既にプライマリキーが存在する場合、新しいカラムがプライマリキーであればエラー
        if column.is_primary_key() && self.get_primary_key().is_some() {
            return Err(TableError::MultiplePrimaryKeys);
        }
        
        self.columns.push(column);
        Ok(())
    } 

    /// ビルダーパターンでカラムを追加する
    pub fn with_column(mut self, column: Column) -> Result<Self, TableError> {
        self.add_column(column)?;
        Ok(self)
    }

    /// 名前でカラムを検索する
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// プライマリキーのカラムを取得する
    pub fn get_primary_key(&self) -> Option<&Column> {
        self.columns.iter().find(|c| c.is_primary_key())
    }

    /// テーブルが有効かチェックする
    pub fn validate(&self) -> Result<(), TableError> {
        if self.columns.is_empty() {
            return Err(TableError::NoColumns);
        }
        
        Ok(())
    }

    /// カラムの位置インデックスを取得する
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// テーブルのカラム名のリストを取得する
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

/// 1行のデータを表現する
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Row {
    /// カラム名と値のマッピング
    pub values: HashMap<String, crate::domain::entity::value::Value>,
}

impl Row {
        /// 新しい空の行を作成する
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    /// カラム名と値のペアから新しい行を作成する
    pub fn from_values(values: HashMap<String, crate::domain::entity::value::Value>) -> Self {
        Self { values }
    }

    /// 特定のカラムの値を取得する
    pub fn get(&self, column_name: &str) -> Option<&crate::domain::entity::value::Value> {
        self.values.get(column_name)
    }

    /// 特定のカラムの値を設定する
    pub fn set(&mut self, column_name: impl Into<String>, value: crate::domain::entity::value::Value) {
        self.values.insert(column_name.into(), value);
    }
}

impl Default for Row {
    fn default() -> Self {
        Self::new()
    }
}

/// クエリ結果セットを表現する
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultSet {
    //結果セットのスキーマ
    pub columns: Vec<Column>,

    /// カラム名と値のマッピング
    pub rows: Vec<Row>,
}

impl ResultSet {
    /// 新しい空の結果セットを作成する
    pub fn new(columns: Vec<Column>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
        }
    }

    /// 結果セットに行を追加する
    pub fn add_row(&mut self, row: Row) {
        self.rows.push(row);
    }

    //行数を取得する
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    // 結果セットが空かどうか
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}