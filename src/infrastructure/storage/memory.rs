use std::collections::HashMap;
use std::sync::RwLock;

use crate::domain::entity::{Table, Column, Row, Value, DataType};
use crate::domain::repository::{FilterCondition, FilterOperator};
use thiserror::Error;

/// ストレージエラー
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Table {0} not found")]
    TableNotFound(String),
    
    #[error("Table {0} already exists")]
    TableAlreadyExists(String),
    
    #[error("Column {0} not found in table {1}")]
    ColumnNotFound(String, String),
    
    #[error("Data type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: DataType, actual: DataType },
    
    #[error("Not null constraint violation for column {0}")]
    NotNullViolation(String),
    
    #[error("Unique constraint violation for column {0}")]
    UniqueViolation(String),
    
    #[error("Primary key constraint violation")]
    PrimaryKeyViolation,
    
    #[error("Internal storage error: {0}")]
    Internal(String),
}

/// テーブルのデータを保持する構造体
#[derive(Debug, Clone)]
struct TableData {
    schema: Table,
    rows: Vec<Row>,
    // インデックス（後で実装）：カラム名 -> 値 -> 行インデックスのセット
    // simple_indices: HashMap<String, BTreeMap<Value, Vec<usize>>>,
}

impl TableData {
    fn new(schema: Table) -> Self {
        Self {
            schema,
            rows: Vec::new(),
            // simple_indices: HashMap::new(),
        }
    }
    
    fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.schema.get_column_index(column_name)
    }
    
    fn validate_row(&self, row: &Row) -> Result<(), StorageError> {
        // 各カラムのデータ型と制約をチェック
        for column in &self.schema.columns {
            // カラムが存在するかチェック
            let value = match row.get(&column.name) {
                Some(v) => v,
                None => {
                    // NOT NULL制約のチェック
                    if column.is_not_null() {
                        return Err(StorageError::NotNullViolation(column.name.clone()));
                    }
                    // NULL値が許容されるのでスキップ
                    continue;
                }
            };
            
            // NULL値のチェック
            if value.data_type() == DataType::Null {
                if column.is_not_null() {
                    return Err(StorageError::NotNullViolation(column.name.clone()));
                }
                continue;
            }
            
            // データ型のチェック
            if value.data_type() != column.data_type {
                return Err(StorageError::TypeMismatch { 
                    expected: column.data_type,
                    actual: value.data_type(),
                });
            }
            
            // プライマリキーと一意制約のチェックは後で実装
        }
        
        Ok(())
    }
    
    fn insert_row(&mut self, row: Row) -> Result<(), StorageError> {
        // 行のバリデーション
        self.validate_row(&row)?;
        
        // プライマリキーと一意制約のチェック
        self.check_constraints(&row)?;
        
        // 行を追加
        self.rows.push(row);
        
        // インデックスの更新は後で実装
        
        Ok(())
    }
    
    fn check_constraints(&self, row: &Row) -> Result<(), StorageError> {
        // プライマリキーと一意制約のチェック
        for column in &self.schema.columns {
            if column.is_primary_key() || column.is_unique() {
                if let Some(value) = row.get(&column.name) {
                    // NULL値はユニーク制約に違反しない（標準SQLの仕様）
                    if value.data_type() == DataType::Null {
                        continue;
                    }
                    
                    // 既存の行に同じ値があるかチェック
                    for existing_row in &self.rows {
                        if let Some(existing_value) = existing_row.get(&column.name) {
                            if existing_value == value {
                                if column.is_primary_key() {
                                    return Err(StorageError::PrimaryKeyViolation);
                                } else {
                                    return Err(StorageError::UniqueViolation(column.name.clone()));
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    fn filter_rows(&self, filter: &FilterCondition) -> Vec<&Row> {
        self.rows.iter()
            .filter(|row| self.eval_filter(row, filter))
            .collect()
    }
    
    fn eval_filter(&self, row: &Row, filter: &FilterCondition) -> bool {
        match filter {
            FilterCondition::Simple { column, operator, value } => {
                let row_value = match row.get(column) {
                    Some(v) => v,
                    None => return false,
                };
                
                match operator {
                    FilterOperator::Equal => row_value == value,
                    FilterOperator::NotEqual => row_value != value,
                    FilterOperator::Greater => {
                        match (row_value, value) {
                            (Value::Integer(a), Value::Integer(b)) => a > b,
                            (Value::Float(a), Value::Float(b)) => a > b,
                            (Value::Text(a), Value::Text(b)) => a > b,
                            _ => false,
                        }
                    },
                    FilterOperator::GreaterOrEqual => {
                        match (row_value, value) {
                            (Value::Integer(a), Value::Integer(b)) => a >= b,
                            (Value::Float(a), Value::Float(b)) => a >= b,
                            (Value::Text(a), Value::Text(b)) => a >= b,
                            _ => false,
                        }
                    },
                    FilterOperator::Less => {
                        match (row_value, value) {
                            (Value::Integer(a), Value::Integer(b)) => a < b,
                            (Value::Float(a), Value::Float(b)) => a < b,
                            (Value::Text(a), Value::Text(b)) => a < b,
                            _ => false,
                        }
                    },
                    FilterOperator::LessOrEqual => {
                        match (row_value, value) {
                            (Value::Integer(a), Value::Integer(b)) => a <= b,
                            (Value::Float(a), Value::Float(b)) => a <= b,
                            (Value::Text(a), Value::Text(b)) => a <= b,
                            _ => false,
                        }
                    },
                    FilterOperator::Like => {
                        // シンプルなLIKE演算子の実装（%のみサポート）
                        if let (Value::Text(text), Value::Text(pattern)) = (row_value, value) {
                            if pattern.starts_with('%') && pattern.ends_with('%') {
                                let search = &pattern[1..pattern.len()-1];
                                text.contains(search)
                            } else if pattern.starts_with('%') {
                                let search = &pattern[1..];
                                text.ends_with(search)
                            } else if pattern.ends_with('%') {
                                let search = &pattern[..pattern.len()-1];
                                text.starts_with(search)
                            } else {
                                text == pattern
                            }
                        } else {
                            false
                        }
                    },
                }
            },
            FilterCondition::And(conditions) => {
                conditions.iter().all(|c| self.eval_filter(row, c))
            },
            FilterCondition::Or(conditions) => {
                conditions.iter().any(|c| self.eval_filter(row, c))
            },
        }
    }
    
    fn update_rows(&mut self, updates: &[(String, Value)], filter: Option<&FilterCondition>) -> usize {
        let mut updated_count = 0;
        
        // 事前にフィルタを通過する行のインデックスを収集
        let indices_to_update = if let Some(f) = filter {
            self.rows.iter()
                .enumerate()
                .filter(|(_, row)| self.eval_filter(row, f))
                .map(|(i, _)| i)
                .collect::<Vec<_>>()
        } else {
            (0..self.rows.len()).collect()
        };
        
        // 収集したインデックスの行を更新
        for idx in indices_to_update {
            if let Some(row) = self.rows.get_mut(idx) {
                for (column, value) in updates {
                    row.set(column.clone(), value.clone());
                }
                updated_count += 1;
            }
        }
        
        updated_count
    }
    
    fn delete_rows(&mut self, filter: Option<&FilterCondition>) -> usize {
        let initial_len = self.rows.len();
        
        if let Some(filter) = filter {
            // 削除する代わりに保持する行を収集
            let rows_to_keep = self.rows.iter()
                .filter(|row| !self.eval_filter(row, filter))
                .cloned()  // クローンを作成
                .collect::<Vec<_>>();
            
            let deleted_count = initial_len - rows_to_keep.len();
            self.rows = rows_to_keep;  // 保持する行で置き換え
            deleted_count
        } else {
            let deleted_count = self.rows.len();
            self.rows.clear();
            deleted_count
        }
    }
}

/// インメモリストレージの実装
#[derive(Debug, Default)]
pub struct MemoryStorage {
    tables: RwLock<HashMap<String, TableData>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }
    
    /// テーブルを作成する
    pub fn create_table(&self, table: Table, if_not_exists: bool) -> Result<(), StorageError> {
        let mut tables = self.tables.write().unwrap();
        
        if tables.contains_key(&table.name) {
            if if_not_exists {
                return Ok(());
            }
            return Err(StorageError::TableAlreadyExists(table.name));
        }
        
        tables.insert(table.name.clone(), TableData::new(table));
        Ok(())
    }
    
    /// テーブルを削除する
    pub fn drop_table(&self, table_name: &str, if_exists: bool) -> Result<(), StorageError> {
        let mut tables = self.tables.write().unwrap();
        
        if !tables.contains_key(table_name) {
            if if_exists {
                return Ok(());
            }
            return Err(StorageError::TableNotFound(table_name.to_string()));
        }
        
        tables.remove(table_name);
        Ok(())
    }
    
    /// テーブルが存在するか確認する
    pub fn table_exists(&self, table_name: &str) -> bool {
        let tables = self.tables.read().unwrap();
        tables.contains_key(table_name)
    }
    
    /// テーブルのスキーマを取得する
    pub fn get_table(&self, table_name: &str) -> Result<Table, StorageError> {
        let tables = self.tables.read().unwrap();
        
        let table_data = tables.get(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
        
        Ok(table_data.schema.clone())
    }
    
    /// すべてのテーブル名を取得する
    pub fn get_table_names(&self) -> Vec<String> {
        let tables = self.tables.read().unwrap();
        tables.keys().cloned().collect()
    }
    
    /// 行を挿入する
    pub fn insert_row(&self, table_name: &str, row: Row) -> Result<(), StorageError> {
        let mut tables = self.tables.write().unwrap();
        
        let table_data = tables.get_mut(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
        
        table_data.insert_row(row)
    }
    
    /// 複数行を挿入する
    pub fn insert_rows(&self, table_name: &str, rows: Vec<Row>) -> Result<(), StorageError> {
        let mut tables = self.tables.write().unwrap();
        
        let table_data = tables.get_mut(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
        
        for row in rows {
            table_data.insert_row(row)?;
        }
        
        Ok(())
    }
    
    /// 行を検索する
    pub fn select_rows(
        &self,
        table_name: &str,
        columns: Option<&[String]>,
        filter: Option<&FilterCondition>
    ) -> Result<(Vec<Column>, Vec<Row>), StorageError> {
        let tables = self.tables.read().unwrap();
        
        let table_data = tables.get(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
        
        // フィルタリング
        let rows: Vec<Row> = if let Some(filter) = filter {
            table_data.filter_rows(filter).into_iter().cloned().collect()
        } else {
            table_data.rows.clone()
        };
        
        // カラムの選択
        let selected_columns = if let Some(column_names) = columns {
            let mut cols = Vec::new();
            for name in column_names {
                if let Some(col) = table_data.schema.get_column(name) {
                    cols.push(col.clone());
                } else {
                    return Err(StorageError::ColumnNotFound(
                        name.clone(), table_name.to_string()
                    ));
                }
            }
            cols
        } else {
            table_data.schema.columns.clone()
        };
        
        Ok((selected_columns, rows))
    }
    
    /// 行を更新する
    pub fn update_rows(
        &self,
        table_name: &str,
        updates: &[(String, Value)],
        filter: Option<&FilterCondition>
    ) -> Result<usize, StorageError> {
        let mut tables = self.tables.write().unwrap();
        
        let table_data = tables.get_mut(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
        
        // 更新前にカラムの存在確認
        for (column_name, _) in updates {
            if table_data.get_column_index(column_name).is_none() {
                return Err(StorageError::ColumnNotFound(
                    column_name.clone(), table_name.to_string()
                ));
            }
        }
        
        Ok(table_data.update_rows(updates, filter))
    }
    
    /// 行を削除する
    pub fn delete_rows(
        &self,
        table_name: &str,
        filter: Option<&FilterCondition>
    ) -> Result<usize, StorageError> {
        let mut tables = self.tables.write().unwrap();
        
        let table_data = tables.get_mut(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
        
        Ok(table_data.delete_rows(filter))
    }
}