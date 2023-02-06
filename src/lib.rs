use eyre::Context;
use serenity::{
    builder::GetMessages,
    client::ClientBuilder,
    futures::Stream,
    http::CacheHttp,
    model::{
        prelude::{ChannelId, GuildId, Message, MessageId},
        user::CurrentUser,
    },
    prelude::GatewayIntents,
    Client,
};

pub struct Discord {
    pub client: Client,
    current_user: CurrentUser,
}

impl Discord {
    /// ### Required Discord Bot Settings
    ///
    /// Privileged Gateway Intents: [Server Members Intent, Message Content Intent]
    pub async fn new(token: impl AsRef<str>) -> Self {
        let client = ClientBuilder::new(token.as_ref(), GatewayIntents::MESSAGE_CONTENT)
            .await
            .expect("failed create client");

        let current_user = client
            .cache_and_http
            .http()
            .get_current_user()
            .await
            .expect("failed get_current_user");

        Self {
            client,
            current_user,
        }
    }

    pub async fn from_env() -> Self {
        Self::new(std::env::var("DISCORD_BOT_TOKEN").unwrap()).await
    }

    pub async fn latest_message_iter(
        &self,
        channel_id: ChannelId,
    ) -> impl Stream<Item = serenity::Result<Message>> + '_ {
        let http = self.client.cache_and_http.http();
        Box::pin(channel_id.messages_iter(http))
    }

    pub async fn get_messages(
        &self,
        channel_id: ChannelId,
        builder: impl FnOnce(&mut GetMessages),
    ) -> eyre::Result<Vec<Message>> {
        let http = self.client.cache_and_http.http();

        channel_id
            .messages(http, |retriever| {
                builder(retriever);
                retriever
            })
            .await
            .context("failed get_messages")
    }

    pub async fn send_message(
        &self,
        channel_id: ChannelId,
        content: impl ToString,
    ) -> eyre::Result<Message> {
        let http = self.client.cache_and_http.http();

        channel_id
            .send_message(http, |m| {
                m.content(content);
                m
            })
            .await
            .context("failed send_message")
    }

    pub async fn edit_message(
        &self,
        channel_id: ChannelId,
        message_id: impl Into<MessageId>,
        content: impl ToString,
    ) -> eyre::Result<Message> {
        let http = self.client.cache_and_http.http();

        channel_id
            .edit_message(http, message_id, |m| {
                m.content(content);
                m
            })
            .await
            .context("failed edit_message")
    }

    pub async fn get_guild_id(&self, guild_name: impl AsRef<str>) -> eyre::Result<GuildId> {
        let guild = self
            .current_user
            .guilds(&self.client.cache_and_http.http())
            .await
            .context("failed get_guild_id")?
            .into_iter()
            .find(|guild| guild.name == guild_name.as_ref())
            .ok_or(eyre::eyre!("not found guild_name: {}", guild_name.as_ref()))?;

        Ok(guild.id)
    }

    pub async fn get_channel_id(
        &self,
        guild_id: GuildId,
        channel_name: impl AsRef<str>,
    ) -> eyre::Result<ChannelId> {
        let channels = self
            .client
            .cache_and_http
            .http()
            .get_channels(guild_id.into())
            .await
            .context("failed get_channels")?;

        channels
            .into_iter()
            .find_map(|channel| (channel.name == channel_name.as_ref()).then_some(channel.id))
            .ok_or(eyre::eyre!(
                "not found channel_name: {}",
                channel_name.as_ref()
            ))
    }
}

#[cfg(test)]
mod tests {
    use serenity::futures::StreamExt;

    use super::*;

    async fn db() -> Discord {
        dotenv::dotenv().unwrap();

        Discord::from_env().await
    }

    #[ignore]
    #[tokio::test]
    async fn guild_id() {
        let db = db().await;
        let guild_id = db.get_guild_id("개발자 모임").await.unwrap();

        assert_eq!(guild_id, GuildId(771396144830873640));
    }

    #[ignore]
    #[tokio::test]
    async fn send_message() {
        let db = db().await;

        let guild_id = db.get_guild_id("개발자 모임").await.unwrap();
        let channel_id = db.get_channel_id(guild_id, "일반").await.unwrap();
        let message = db.send_message(channel_id, "hello").await.unwrap();

        println!("{message:?}");
    }

    #[ignore]
    #[tokio::test]
    async fn edit_message() {
        let db = db().await;

        let guild_id = db.get_guild_id("개발자 모임").await.unwrap();
        let channel_id = db.get_channel_id(guild_id, "일반").await.unwrap();
        let message = db.send_message(channel_id, "hello").await.unwrap();
        println!("{:?}", message.id);

        let message = db
            .edit_message(channel_id, message.id, "hello2")
            .await
            .unwrap();

        println!("{message:?}")
    }

    #[ignore]
    #[tokio::test]
    async fn get_channel_id() {
        let db = db().await;

        let guild_id = db.get_guild_id("개발자 모임").await.unwrap();
        let channel_id = db.get_channel_id(guild_id, "일반").await.unwrap();
        println!("{channel_id:?}",);
    }

    #[ignore]
    #[tokio::test]
    async fn messages_iter() {
        let db = db().await;

        let guild_id = db.get_guild_id("개발자 모임").await.unwrap();
        let channel_id = db.get_channel_id(guild_id, "일반").await.unwrap();
        let mut messages = db.latest_message_iter(channel_id).await.take(2);

        while let Some(messages) = messages.next().await {
            println!("{:#?}", messages.unwrap());
            println!()
        }
    }

    #[ignore]
    #[tokio::test]
    async fn channel_messages() {
        let db = db().await;

        let guild_id = db.get_guild_id("개발자 모임").await.unwrap();
        let channel_id = db.get_channel_id(guild_id, "일반").await.unwrap();

        let messages = db.get_messages(channel_id, |_| {}).await.unwrap();
        println!("{messages:#?}");
    }
}
