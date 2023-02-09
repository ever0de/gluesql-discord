use gluesql_core::prelude::Glue;
use gluesql_discord_storage::Discord;

#[tokio::main]
async fn main() {
    color_eyre::install().unwrap();
    dotenv::dotenv().ok();

    let discord = Discord::from_env().await;
    let guild_name = "GlueSQL Storage Test";

    let storage = discord.into_storage(guild_name).await.unwrap();
    let mut glue = Glue::new(storage);

    let now = std::time::Instant::now();
    glue.execute_async("CREATE TABLE User (id Int, name Text);")
        .await
        .unwrap();

    glue.execute_async("INSERT INTO User VALUES (1, 'glue');")
        .await
        .unwrap();

    let payloads = glue.execute_async("SELECT * FROM User;").await.unwrap();
    println!("{:?}", payloads[0]);

    glue.execute_async("DROP TABLE User;").await.unwrap();
    println!("{}ms", now.elapsed().as_millis());
}
