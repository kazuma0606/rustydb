use axum::{
    Router, 
    routing::{get, post},
    Extension,
    Server,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

use crate::domain::repository::TableRepository;
use crate::infrastructure::storage::MemoryStorage;
use crate::infrastructure::repository::MemoryTableRepository;
use crate::infrastructure::parser::SqlParser;
use crate::interface::api::handler::{
    health_check_handler, 
    get_tables_handler, 
    get_table_handler, 
    execute_sql_handler
};

#[derive(Clone)]
pub struct ServerConfig {
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 8080, // デフォルトポート番号
        }
    }
}

pub async fn start_server(config: ServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    // ストレージとリポジトリの初期化
    let storage = Arc::new(MemoryStorage::new());
    let repository: Arc<dyn TableRepository> = Arc::new(MemoryTableRepository::new(storage.clone()));
    
    // SQLパーサーの初期化
    let parser = Arc::new(SqlParser::new());

    // ルーターの設定
    let app = Router::new()
        .route("/health", get(health_check_handler))
        .route("/api/tables", get(get_tables_handler))
        .route("/api/tables/:table_name", get(get_table_handler))
        .route("/api/query", post(execute_sql_handler))
        .layer(Extension(repository))  // リポジトリの拡張
        .layer(Extension(parser));     // パーサーの拡張

    // サーバーのアドレス設定
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    
    info!("サーバーを{}で起動中...", addr);
    
    // サーバーの起動
    Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}