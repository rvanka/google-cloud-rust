use google_cloud_spanner::client::{Client, ClientConfig, Error};
use google_cloud_spanner::mutation::insert_or_update;
use google_cloud_spanner::statement::Statement;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Enable tracing to see what's happening
    tracing_subscriber::fmt::init();

    // Set environment variables to ensure multiplexed sessions are used
    env::set_var("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS", "true");
    env::set_var("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS_FOR_RW", "true");

    let project_id = env::var("SPANNER_PROJECT_ID").expect("SPANNER_PROJECT_ID must be set");
    let instance_id = env::var("SPANNER_INSTANCE_ID").expect("SPANNER_INSTANCE_ID must be set");
    let database_id = env::var("SPANNER_DATABASE_ID").expect("SPANNER_DATABASE_ID must be set");
    let database = format!("projects/{}/instances/{}/databases/{}", project_id, instance_id, database_id);

    println!("Connecting to {}...", database);

    // This example assumes Application Default Credentials are set up
    let config = ClientConfig::default().with_auth().await?;
    let client = Client::new(database, config).await?;

    println!("Initial session count: {}", client.session_count());

    // Perform a Read-Write Transaction using multiplexed session
    println!("Starting RW transaction using multiplexed session...");
    
    let result = client.read_write_transaction::<String, Error, _>(|tx| {
        Box::pin(async move {
            // 1. Read existing value
            // Assumes a table 'my_table' with 'id_column' (INT64) and 'name_column' (STRING)
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
            
            let m = insert_or_update("my_table", &["id_column", "name_column"], &[&1i64, &new_val]);
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

    println!("Example completed successfully!");

    client.close().await;
    Ok(())
}
