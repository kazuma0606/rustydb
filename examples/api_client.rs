use reqwest::Client;
use serde_json::{json, Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let base_url = "http://localhost:8080";
    
    println!("=== RustyDB API テスト ===\n");
    
    // 1. ヘルスチェック
    println!("1. ヘルスチェック");
    let resp = client.get(format!("{}/health", base_url)).send().await?;
    println!("ステータス: {}", resp.status());
    println!("レスポンス: {}", resp.text().await?);
    println!();
    
    // 2. テーブル作成
    println!("2. テーブル作成");
    let create_query = json!({
        "sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER, active BOOLEAN DEFAULT true)"
    });
    
    let resp = client.post(format!("{}/api/query", base_url))
        .json(&create_query)
        .send()
        .await?;
    
    println!("ステータス: {}", resp.status());
    println!("レスポンス: {}", resp.text().await?);
    println!();
    
    // 3. データ挿入
    println!("3. データ挿入");
    let insert_query = json!({
        "sql": "INSERT INTO users (id, name, age, active) VALUES (1, 'Alice', 30, true), (2, 'Bob', 25, false), (3, 'Charlie', 35, true)"
    });
    
    let resp = client.post(format!("{}/api/query", base_url))
        .json(&insert_query)
        .send()
        .await?;
    
    println!("ステータス: {}", resp.status());
    println!("レスポンス: {}", resp.text().await?);
    println!();
    
    // 4. データ取得
    println!("4. データ取得");
    let select_query = json!({
        "sql": "SELECT * FROM users"
    });
    
    let resp = client.post(format!("{}/api/query", base_url))
        .json(&select_query)
        .send()
        .await?;
    
    println!("ステータス: {}", resp.status());
    let result_text = resp.text().await?;
    println!("レスポンス: {}", result_text);
    
    // JSON形式のレスポンスをきれいに表示
    if let Ok(result) = serde_json::from_str::<Value>(&result_text) {
        println!("整形レスポンス: {}", serde_json::to_string_pretty(&result)?);
    }
    println!();
    
    // 5. テーブル一覧取得
    println!("5. テーブル一覧取得");
    let resp = client.get(format!("{}/api/tables", base_url))
        .send()
        .await?;
    
    println!("ステータス: {}", resp.status());
    println!("レスポンス: {}", resp.text().await?);
    println!();
    
    // 6. テーブル詳細取得
    println!("6. テーブル詳細取得");
    let resp = client.get(format!("{}/api/tables/users", base_url))
        .send()
        .await?;
    
    println!("ステータス: {}", resp.status());
    let result_text = resp.text().await?;
    println!("レスポンス: {}", result_text);
    
    // JSON形式のレスポンスをきれいに表示
    if let Ok(result) = serde_json::from_str::<Value>(&result_text) {
        println!("整形レスポンス: {}", serde_json::to_string_pretty(&result)?);
    }
    println!();
    
    // 7. データ更新
    println!("7. データ更新");
    let update_query = json!({
        "sql": "UPDATE users SET age = 31 WHERE id = 1"
    });
    
    let resp = client.post(format!("{}/api/query", base_url))
        .json(&update_query)
        .send()
        .await?;
    
    println!("ステータス: {}", resp.status());
    println!("レスポンス: {}", resp.text().await?);
    println!();
    
    // 8. 更新後のデータ確認
    println!("8. 更新後のデータ確認");
    let select_query = json!({
        "sql": "SELECT * FROM users WHERE id = 1"
    });
    
    let resp = client.post(format!("{}/api/query", base_url))
        .json(&select_query)
        .send()
        .await?;
    
    println!("ステータス: {}", resp.status());
    let result_text = resp.text().await?;
    println!("レスポンス: {}", result_text);
    
    // JSON形式のレスポンスをきれいに表示
    if let Ok(result) = serde_json::from_str::<Value>(&result_text) {
        println!("整形レスポンス: {}", serde_json::to_string_pretty(&result)?);
    }
    println!();
    
    println!("APIテスト完了！");
    
    Ok(())
}