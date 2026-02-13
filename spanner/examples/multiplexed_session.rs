use gcloud_spanner::client::{Client, ClientConfig, Error};
use gcloud_spanner::mutation::insert_or_update;
use gcloud_spanner::statement::{Statement, ToKind};
use std::env;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// GCP Project ID
    #[arg(short, long, default_value = "span-cloud-testing")]
    project: String,

    /// Spanner Instance ID
    #[arg(short, long, default_value = "rajeshwarv-http-test-instance")]
    instance: String,

    /// Spanner Database ID
    #[arg(short, long, default_value = "rust-client-test")]
    database: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    rustls::crypto::aws_lc_rs::default_provider().install_default().expect("Failed to install rustls crypto provider");

    // Enable tracing to see what's happening
    tracing_subscriber::fmt::init();

    // Set environment variables to ensure multiplexed sessions are used
    env::set_var("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS", "true");
    env::set_var("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS_FOR_RW", "true");

    let database_path = format!(
        "projects/{}/instances/{}/databases/{}",
        args.project, args.instance, args.database
    );

    println!("Connecting to {}...", database_path);

    // Using ClientConfig::default().with_auth().await? will use Application Default Credentials (ADC)
    // which works with `gcloud auth login` or `gcloud auth application-default login`.
    let config = ClientConfig::default().with_auth().await?;
    let client = Client::new(database_path, config).await?;

    println!("Initial session count: {}", client.session_count());

    // Perform a Read-Write Transaction
    println!("Starting RW transaction using multiplexed session...");
    
    let result = client.read_write_transaction::<String, Error, _>(|tx| {
        Box::pin(async move {
            // 1. Read existing value
            let mut stmt = Statement::new("SELECT name_column FROM my_table WHERE id_column = @id");
            stmt.add_param("id", &1i64);
            
            let mut reader = tx.query(stmt).await?;
            let mut current_val = String::from("0");
            if let Some(row) = reader.next().await? {
                current_val = row.column_by_name::<String>("name_column")?;
                println!("Read current value: {}", current_val);
            } else {
                println!("No existing row found for id_column=1, starting from 0");
            }

            // 2. Increment and update
            let new_val_int: i64 = current_val.parse::<i64>().unwrap_or(0) + 1;
            let new_val = new_val_int.to_string();
            println!("Updating to new value: {}", new_val);
            
            let m = insert_or_update("my_table", &["id_column", "name_column"], &[&1i64 as &dyn ToKind, &new_val as &dyn ToKind]);
            tx.buffer_write(vec![m]);
            
            Ok(new_val)
        })
    }).await?;

    let (commit_result, updated_val) = result;
    if let Some(ts) = commit_result.timestamp {
        println!("Transaction committed at {}.{}", ts.seconds, ts.nanos);
    }
    println!("Updated value in DB: {}", updated_val);

    // Verify the update with a single read (also uses multiplexed session)
    println!("Verifying update with a single read...");
    let mut tx = client.single().await?;
    let mut stmt = Statement::new("SELECT name_column FROM my_table WHERE id_column = @id");
    stmt.add_param("id", &1i64);
    let mut reader = tx.query(stmt).await?;
    if let Some(row) = reader.next().await? {
        let val = row.column_by_name::<String>("name_column")?;
        println!("Verified value in DB: {}", val);
        assert_eq!(val, updated_val);
    }

    println!("Test completed successfully!");

    client.close().await;
    Ok(())
}
