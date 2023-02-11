use gluesql_core::prelude::Glue;
use gluesql_discord_storage::Discord;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    color_eyre::install().unwrap();
    dotenv::dotenv().unwrap();

    let discord = Discord::from_env().await;
    let guild_name = "GlueSQL Storage Test";

    let storage = discord.into_storage(guild_name).await.unwrap();
    let mut glue = Glue::new(storage);

    tracing::info!("CREATE TABLE");
    glue.execute_async("CREATE TABLE User (id Int, name Text);")
        .await
        .unwrap();

    tracing::info!("INSERT INTO");
    glue.execute_async("INSERT INTO User VALUES (1, 'glue');")
        .await
        .unwrap();

    tracing::info!("SELECT");
    let payloads = glue.execute_async("SELECT * FROM User;").await.unwrap();
    println!("{:?}", payloads[0]);

    tracing::info!("DROP TABLE");
    glue.execute_async("DROP TABLE User;").await.unwrap();

    tracing::info!("SELECT SCHEMALESS");
    glue.execute_async(r#"SELECT * FROM 'hello-world'"#)
        .await
        .unwrap();
}
