use async_trait::async_trait;
use crate::domain::entity::{Table, Row, ResultSet};
use crate::domain::entity::value::Value;
use crate::Error;
use  std::sync::Arc;

// テーブルリポジトリトレイト
#[derive(thiserror::Error, Debug)]
pub enum RepositoryError {
    #[error("Table {0} not found")]
    TableNotFound(String),

    #[error("Table {0} already exists")]
    TableAlreadyExists(String),

    #[error("Column {0} not found in table {1}")]
    ColumnNotFound(String, String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Data error: {0}")]
    DataError(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

impl From<RepositoryError> for Error {
    fn from(err: RepositoryError) -> Self {
        match err {
            RepositoryError::TableNotFound(name) => Error::Schema(format!("Table {} not found", name)),
            RepositoryError::TableAlreadyExists(name) => Error::Schema(format!("Table {} already exists", name)),
            RepositoryError::ColumnNotFound(column, table) => Error::Schema(format!("Column {} not found in table {}", column, table)),
            RepositoryError::StorageError(msg) => Error::Storage(msg),
            RepositoryError::DataError(msg) => Error::Execution(msg),
            RepositoryError::InternalError(msg) => Error::Internal(msg),
        }
    }
}

//テーブルリポジトリ - データベーステーブルの永続化と取得のための抽象インターフェース
#[async_trait]
pub trait TableRepository: Send + Sync {
    // テーブルを作成する
    async fn create_table(&self, table: &Table) -> Result<(), RepositoryError>;

    // テーブルが存在するかチェックする
    async fn table_exists(&self, table_name: &str) -> Result<bool, RepositoryError>;

    // テーブルを削除する
    async fn drop_table(&self, table_name: &str) -> Result<(), RepositoryError>;

   /// 名前でテーブルを取得する
   async fn get_table(&self, table_name: &str) -> Result<Table, RepositoryError>;
    
   /// すべてのテーブル名を取得する
   async fn get_table_names(&self) -> Result<Vec<String>, RepositoryError>;
   
   /// テーブルに1行のデータを挿入する
   async fn insert(&self, table_name: &str, row: &Row) -> Result<(), RepositoryError>;
   
   /// 複数行のデータを一括挿入する
   async fn insert_many(&self, table_name: &str, rows: &[Row]) -> Result<(), RepositoryError>;
   
    /// シンプルな条件でテーブルからデータを取得する
    /// ※完全なクエリ機能は後で実装します
    async fn select(
        &self,
        table_name: &str,
        column_names: &[String],
        filter: Option<&FilterCondition>
    ) -> Result<ResultSet, RepositoryError>;

    /// 条件に合致する行を更新する
    async fn update(
        &self,
        table_name: &str,
        updates: &[(String, Value)],
        filter: Option<&FilterCondition>,
    ) -> Result<usize, RepositoryError>;

    async fn delete(
        &self,
        table_name: &str,
        filter: Option<&FilterCondition>,
    ) -> Result<usize, RepositoryError>;
}

/// クエリフィルター条件
#[derive(Debug, Clone)]
pub enum FilterCondition {
   /// 単一条件（カラム名、演算子、値）
   Simple {
         column: String,
         operator: FilterOperator,
         value: Value,
   },

   /// 複数条件（ANDまたはOR）
   And(Vec<FilterCondition>),
   Or(Vec<FilterCondition>),
}

/// フィルター演算子
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOperator {
    Equal,
    NotEqual,
    Greater, // GreaterThanではなくGreater
    GreaterOrEqual, // GreaterThanOrEqualではなくGreaterOrEqual
    Less, // LessThanではなくLess
    LessOrEqual, // LessThanOrEqualではなくLessOrEqual
    Like,
}

/// リポジトリファクトリトレイト
/// 様々なリポジトリ実装を生成する責任を持つ
pub trait  RepositoryFactory: Send + Sync {
    /// テーブルリポジトリを取得する
    fn table_repository(&self) -> Arc<dyn TableRepository>;
}