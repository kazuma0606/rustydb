use std::sync::Arc;
use async_trait::async_trait;

use crate::domain::entity::{Table, Row, Value, ResultSet};
use crate::domain::repository::{TableRepository, RepositoryError, FilterCondition};
use crate::infrastructure::storage::{MemoryStorage, StorageError};

/// インメモリリポジトリの実装
pub struct MemoryTableRepository {
    storage: Arc<MemoryStorage>,
}

impl MemoryTableRepository {
    pub fn new(storage: Arc<MemoryStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl TableRepository for MemoryTableRepository {
    async fn create_table(&self, table: &Table) -> Result<(), RepositoryError> {
        self.storage.create_table(table.clone(), false)
            .map_err(|e: StorageError| RepositoryError::from(e))
    }
    
    async fn table_exists(&self, table_name: &str) -> Result<bool, RepositoryError> {
        Ok(self.storage.table_exists(table_name))
    }
    
    async fn drop_table(&self, table_name: &str) -> Result<(), RepositoryError> {
        self.storage.drop_table(table_name, false)
            .map_err(|e: StorageError| RepositoryError::from(e))
    }
    
    async fn get_table(&self, table_name: &str) -> Result<Table, RepositoryError> {
        self.storage.get_table(table_name)
            .map_err(|e: StorageError| RepositoryError::from(e))
    }
    
    async fn get_table_names(&self) -> Result<Vec<String>, RepositoryError> {
        Ok(self.storage.get_table_names())
    }
    
    async fn insert(&self, table_name: &str, row: &Row) -> Result<(), RepositoryError> {
        self.storage.insert_row(table_name, row.clone())
            .map_err(|e: StorageError| RepositoryError::from(e))
    }
    
    async fn insert_many(&self, table_name: &str, rows: &[Row]) -> Result<(), RepositoryError> {
        self.storage.insert_rows(table_name, rows.to_vec())
            .map_err(|e: StorageError| RepositoryError::from(e))
    }
    
    async fn select(
        &self,
        table_name: &str,
        columns: &[String],
        filter: Option<&FilterCondition>
    ) -> Result<ResultSet, RepositoryError> {
        // 空配列の場合はNoneとして扱う（すべてのカラムを選択）
        let cols = if columns.is_empty() { None } else { Some(columns) };
        
        let (selected_columns, rows) = self.storage.select_rows(table_name, cols, filter)
            .map_err(|e: StorageError| RepositoryError::from(e))?;
        
        let mut result = ResultSet::new(selected_columns);
        for row in rows {
            result.add_row(row);
        }
        
        Ok(result)
    }
    
    async fn update(
        &self,
        table_name: &str,
        updates: &[(String, Value)],
        filter: Option<&FilterCondition>
    ) -> Result<usize, RepositoryError> {
        self.storage.update_rows(table_name, updates, filter)
            .map_err(|e: StorageError| RepositoryError::from(e))
    }
    
    async fn delete(
        &self,
        table_name: &str,
        filter: Option<&FilterCondition>
    ) -> Result<usize, RepositoryError> {
        self.storage.delete_rows(table_name, filter)
            .map_err(|e: StorageError| RepositoryError::from(e))
    }
}

impl From<StorageError> for RepositoryError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::TableNotFound(name) => RepositoryError::TableNotFound(name),
            StorageError::TableAlreadyExists(name) => RepositoryError::TableAlreadyExists(name),
            StorageError::ColumnNotFound(col, table) => RepositoryError::ColumnNotFound(col, table),
            StorageError::TypeMismatch { expected, actual } => 
                RepositoryError::DataError(format!("Type mismatch: expected {:?}, got {:?}", expected, actual)),
            StorageError::NotNullViolation(col) => 
                RepositoryError::DataError(format!("NOT NULL constraint violation for column {}", col)),
            StorageError::UniqueViolation(col) => 
                RepositoryError::DataError(format!("UNIQUE constraint violation for column {}", col)),
            StorageError::PrimaryKeyViolation => 
                RepositoryError::DataError("PRIMARY KEY constraint violation".to_string()),
            StorageError::Internal(msg) => RepositoryError::InternalError(msg),
        }
    }
}