use tracing::info;
use rustydb::interface::api::{start_server, ServerConfig};
use rustydb::VERSION;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ロガーを初期化
    tracing_subscriber::fmt::init();
    
    info!("Starting RustyDB v{}", VERSION);
    
    // サーバー設定（デフォルト：localhost:8080）
    let config = ServerConfig::default();
    
    // サーバーの起動
    start_server(config).await?;
    
    Ok(())
}