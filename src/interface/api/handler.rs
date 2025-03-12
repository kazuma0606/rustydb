use axum::{
    extract::{Path, Json, Extension},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

use crate::domain::repository::{TableRepository, RepositoryError};
use crate::domain::entity::{Table, Row, Value};
use crate::infrastructure::parser::{SqlParser, ParsedStatement};

/// API エラー
#[derive(Error, Debug)]
pub enum ApiError {
    #[error("SQL syntax error: {0}")]
    SqlSyntax(String),
    
    #[error("Repository error: {0}")]
    Repository(#[from] RepositoryError),
    
    #[error("Unsupported SQL: {0}")]
    UnsupportedSql(String),
    
    #[error("Internal server error: {0}")]
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            ApiError::SqlSyntax(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::Repository(e) => match e {
                RepositoryError::TableNotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
                RepositoryError::TableAlreadyExists(_) => (StatusCode::CONFLICT, e.to_string()),
                _ => (StatusCode::BAD_REQUEST, e.to_string()),
            },
            ApiError::UnsupportedSql(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };
        
        let body = Json(ErrorResponse {
            error: error_message,
        });
        
        (status, body).into_response()
    }
}

/// エラーレスポンス
#[derive(Serialize)]
pub struct ErrorResponse {
    error: String,
}

/// SQLクエリのリクエスト
#[derive(Deserialize)]
pub struct QueryRequest {
    sql: String,
}

/// テーブル情報のレスポンス
#[derive(Serialize)]
pub struct TableInfoResponse {
    name: String,
    columns: Vec<ColumnInfo>,
}

/// カラム情報
#[derive(Serialize)]
pub struct ColumnInfo {
    name: String,
    data_type: String,
    constraints: Vec<String>,
}

/// クエリ実行結果
#[derive(Serialize)]
pub struct QueryResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    columns: Option<Vec<String>>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    rows: Option<Vec<serde_json::Value>>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    affected_rows: Option<usize>,
    
    statement_type: String,
}

/// ヘルスチェックハンドラー
pub async fn health_check_handler() -> impl IntoResponse {
    StatusCode::OK
}

/// テーブル一覧取得ハンドラー
pub async fn get_tables_handler(
    Extension(repository): Extension<Arc<dyn TableRepository>>,
) -> Result<Json<Vec<String>>, ApiError> {
    let tables = repository.get_table_names().await?;
    Ok(Json(tables))
}

/// テーブル詳細取得ハンドラー
pub async fn get_table_handler(
    Path(table_name): Path<String>,
    Extension(repository): Extension<Arc<dyn TableRepository>>,
) -> Result<Json<TableInfoResponse>, ApiError> {
    let table = repository.get_table(&table_name).await?;
    
    let columns = table.columns.iter().map(|col| {
        ColumnInfo {
            name: col.name.clone(),
            data_type: col.data_type.to_string(),
            constraints: col.constraints.iter().map(|c| c.to_string()).collect(),
        }
    }).collect();
    
    Ok(Json(TableInfoResponse {
        name: table.name,
        columns,
    }))
}

/// SQL実行ハンドラー
pub async fn execute_sql_handler(
    Extension(repository): Extension<Arc<dyn TableRepository>>,
    Extension(parser): Extension<Arc<SqlParser>>,
    Json(payload): Json<QueryRequest>,
) -> Result<Json<QueryResult>, ApiError> {
    // SQLの解析
    let statements = match parser.parse(&payload.sql) {
        Ok(stmts) => stmts,
        Err(e) => return Err(ApiError::SqlSyntax(e.to_string())),
    };
    
    if statements.is_empty() {
        return Err(ApiError::SqlSyntax("No SQL statement provided".to_string()));
    }
    
    // 現時点では単一のSQLステートメントのみをサポート
    let stmt = &statements[0];
    
    // ステートメントのタイプに応じた処理
    match stmt {
        ParsedStatement::CreateTable(create_stmt) => {
            let mut table = Table::new(&create_stmt.table_name);
            for column in &create_stmt.columns {
                table.add_column(column.clone())
                    .map_err(|e| ApiError::Internal(e.to_string()))?;
            }
            
            repository.create_table(&table).await?;
            
            Ok(Json(QueryResult {
                columns: None,
                rows: None,
                affected_rows: Some(0),
                statement_type: "CREATE_TABLE".to_string(),
            }))
        },
        
        ParsedStatement::Select(select_stmt) => {
            let columns = select_stmt.columns.as_ref().map_or(Vec::new(), |c| c.clone());
            let result = repository.select(
                &select_stmt.table_name, 
                &columns, 
                select_stmt.filter.as_ref()
            ).await?;
            
            // 結果を変換
            let column_names = result.columns.iter().map(|c| c.name.clone()).collect();
            
            let rows = result.rows.iter().map(|row| {
                let mut obj = serde_json::Map::new();
                for column in &result.columns {
                    let value = match row.get(&column.name) {
                        Some(Value::Integer(i)) => serde_json::Value::Number(serde_json::Number::from(*i)),
                        Some(Value::Float(f)) => {
                            if let Some(num) = serde_json::Number::from_f64(*f) {
                                serde_json::Value::Number(num)
                            } else {
                                serde_json::Value::String(f.to_string())
                            }
                        },
                        Some(Value::Text(s)) => serde_json::Value::String(s.clone()),
                        Some(Value::Boolean(b)) => serde_json::Value::Bool(*b),
                        Some(Value::Timestamp(dt)) => serde_json::Value::String(dt.to_string()),
                        Some(Value::Null) => serde_json::Value::Null,
                        None => serde_json::Value::Null,
                    };
                    obj.insert(column.name.clone(), value);
                }
                serde_json::Value::Object(obj)
            }).collect();
            
            Ok(Json(QueryResult {
                columns: Some(column_names),
                rows: Some(rows),
                affected_rows: None,
                statement_type: "SELECT".to_string(),
            }))
        },
        
        ParsedStatement::Insert(insert_stmt) => {
            let mut affected_rows = 0;
            
            for values in &insert_stmt.values {
                let mut row = Row::new();
                for (i, value) in values.iter().enumerate() {
                    if i < insert_stmt.columns.len() {
                        row.set(insert_stmt.columns[i].clone(), value.clone());
                    }
                }
                repository.insert(&insert_stmt.table_name, &row).await?;
                affected_rows += 1;
            }
            
            Ok(Json(QueryResult {
                columns: None,
                rows: None,
                affected_rows: Some(affected_rows),
                statement_type: "INSERT".to_string(),
            }))
        },
        
        ParsedStatement::Update(update_stmt) => {
            let affected = repository.update(
                &update_stmt.table_name,
                &update_stmt.updates,
                update_stmt.filter.as_ref()
            ).await?;
            
            Ok(Json(QueryResult {
                columns: None,
                rows: None,
                affected_rows: Some(affected),
                statement_type: "UPDATE".to_string(),
            }))
        },
        
        ParsedStatement::Delete(delete_stmt) => {
            let affected = repository.delete(
                &delete_stmt.table_name,
                delete_stmt.filter.as_ref()
            ).await?;
            
            Ok(Json(QueryResult {
                columns: None,
                rows: None,
                affected_rows: Some(affected),
                statement_type: "DELETE".to_string(),
            }))
        },
        
        ParsedStatement::DropTable(drop_stmt) => {
            repository.drop_table(&drop_stmt.table_name).await?;
            
            Ok(Json(QueryResult {
                columns: None,
                rows: None,
                affected_rows: None,
                statement_type: "DROP_TABLE".to_string(),
            }))
        },
    }
}