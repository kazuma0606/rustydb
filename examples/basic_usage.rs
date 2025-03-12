use rustydb::domain::entity::{ Table, Row};
use rustydb::domain::repository::TableRepository;
use rustydb::infrastructure::parser::SqlParser;
use rustydb::infrastructure::storage::MemoryStorage;
use rustydb::infrastructure::repository::MemoryTableRepository;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ストレージとリポジトリの初期化
    let storage = Arc::new(MemoryStorage::new());
    let repository = MemoryTableRepository::new(storage.clone());
    
    // SQLパーサーの初期化
    let parser = SqlParser::new();
    
    println!("=== RustyDB 基本動作チェック ===\n");
    
    // 1. テーブル作成
    println!("1. テーブルの作成");
    let create_table_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER, active BOOLEAN DEFAULT true)";
    println!("SQL: {}", create_table_sql);
    
    let parsed = parser.parse(create_table_sql)?;
    if let Some(rustydb::infrastructure::parser::ParsedStatement::CreateTable(stmt)) = parsed.first() {
        let mut table = Table::new(&stmt.table_name);
        for column in &stmt.columns {
            table.add_column(column.clone())?;
        }
        repository.create_table(&table).await?;
        println!("テーブル 'users' を作成しました\n");
    }
    
    // 2. データ挿入
    println!("2. データの挿入");
    let insert_sql = "INSERT INTO users (id, name, age, active) VALUES (1, 'Alice', 30, true), (2, 'Bob', 25, false), (3, 'Charlie', 35, true)";
    println!("SQL: {}", insert_sql);
    
    let parsed = parser.parse(insert_sql)?;
    if let Some(rustydb::infrastructure::parser::ParsedStatement::Insert(stmt)) = parsed.first() {
        for values in &stmt.values {
            let mut row = Row::new();
            for (i, value) in values.iter().enumerate() {
                if i < stmt.columns.len() {
                    row.set(stmt.columns[i].clone(), value.clone());
                }
            }
            repository.insert(&stmt.table_name, &row).await?;
        }
        println!("3行挿入しました\n");
    }
    
    // 3. データ取得
    println!("3. データの取得");
    let select_sql = "SELECT * FROM users";
    println!("SQL: {}", select_sql);
    
    let parsed = parser.parse(select_sql)?;
    if let Some(rustydb::infrastructure::parser::ParsedStatement::Select(stmt)) = parsed.first() {
        let cols = stmt.columns.as_ref().map_or(Vec::new(), |c| c.clone());
        let result = repository.select(&stmt.table_name, &cols, stmt.filter.as_ref()).await?;
        
        // 結果の表示
        println!("\n結果:");
        // ヘッダーの表示
        for col in &result.columns {
            print!("{}\t", col.name);
        }
        println!();
        
        // 行の表示
        for row in &result.rows {
            for col in &result.columns {
                match row.get(&col.name) {
                    Some(value) => print!("{}\t", value),
                    None => print!("NULL\t"),
                }
            }
            println!();
        }
        println!();
    }
    
    // 4. データ更新
    println!("4. データの更新");
    let update_sql = "UPDATE users SET age = 31 WHERE id = 1";
    println!("SQL: {}", update_sql);
    
    let parsed = parser.parse(update_sql)?;
    if let Some(rustydb::infrastructure::parser::ParsedStatement::Update(stmt)) = parsed.first() {
        let count = repository.update(&stmt.table_name, &stmt.updates, stmt.filter.as_ref()).await?;
        println!("{}行更新しました\n", count);
    }
    
    // 更新後のデータを表示
    println!("5. 更新後のデータ確認");
    let select_sql = "SELECT * FROM users WHERE id = 1";
    println!("SQL: {}", select_sql);
    
    let parsed = parser.parse(select_sql)?;
    if let Some(rustydb::infrastructure::parser::ParsedStatement::Select(stmt)) = parsed.first() {
        let cols = stmt.columns.as_ref().map_or(Vec::new(), |c| c.clone());
        let result = repository.select(&stmt.table_name, &cols, stmt.filter.as_ref()).await?;
        
        // 結果の表示
        println!("\n結果:");
        // ヘッダーの表示
        for col in &result.columns {
            print!("{}\t", col.name);
        }
        println!();
        
        // 行の表示
        for row in &result.rows {
            for col in &result.columns {
                match row.get(&col.name) {
                    Some(value) => print!("{}\t", value),
                    None => print!("NULL\t"),
                }
            }
            println!();
        }
        println!();
    }
    
    // 6. データ削除
    println!("6. データの削除");
    let delete_sql = "DELETE FROM users WHERE active = false";
    println!("SQL: {}", delete_sql);
    
    let parsed = parser.parse(delete_sql)?;
    if let Some(rustydb::infrastructure::parser::ParsedStatement::Delete(stmt)) = parsed.first() {
        let count = repository.delete(&stmt.table_name, stmt.filter.as_ref()).await?;
        println!("{}行削除しました\n", count);
    }
    
    // 削除後のデータを表示
    println!("7. 削除後のデータ確認");
    let select_sql = "SELECT * FROM users";
    println!("SQL: {}", select_sql);
    
    let parsed = parser.parse(select_sql)?;
    if let Some(rustydb::infrastructure::parser::ParsedStatement::Select(stmt)) = parsed.first() {
        let cols = stmt.columns.as_ref().map_or(Vec::new(), |c| c.clone());
        let result = repository.select(&stmt.table_name, &cols, stmt.filter.as_ref()).await?;
        
        // 結果の表示
        println!("\n結果:");
        // ヘッダーの表示
        for col in &result.columns {
            print!("{}\t", col.name);
        }
        println!();
        
        // 行の表示
        for row in &result.rows {
            for col in &result.columns {
                match row.get(&col.name) {
                    Some(value) => print!("{}\t", value),
                    None => print!("NULL\t"),
                }
            }
            println!();
        }
        println!();
    }
    
    println!("テスト完了！");
    
    Ok(())
}