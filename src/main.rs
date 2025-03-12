use tracing::info;
use rustydb::VERSION;

#[tokio::main]
async fn main()-> Result<(), Box<dyn std::error::Error>> {
   tracing_subscriber::fmt::init();
   info!("RustyDB version: {}", VERSION);
   info!("This is a minimal setup for the database. Implemetentaion will be expanded in subsequent steps.");


   println!("this app version is: {}", VERSION);
    Ok(())
}
