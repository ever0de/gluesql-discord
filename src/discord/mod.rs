use std::{collections::HashMap, sync::Arc};

use eyre::Context;
use serenity::{
    builder::CreateChannel,
    client::ClientBuilder,
    futures::Stream,
    http::{CacheHttp, Http, HttpBuilder},
    model::{
        prelude::{Channel, ChannelId, GuildChannel, GuildId, GuildInfo, Message, MessageId},
        user::CurrentUser,
    },
    prelude::GatewayIntents,
    Client,
};

use crate::{debug, storage};

pub struct Discord {
    pub client: Client,
    current_user: CurrentUser,
}

impl Discord {
    /// ### Required Discord Bot Settings(`/oauth2/url-generator`)
    ///
    /// SCOPES
    ///     - bot
    ///
    /// BOT PERMISSIONS
    ///     - Manage Channels
    ///     - Send Messages
    ///     - Manage Messages
    pub async fn new(token: impl AsRef<str>) -> Self {
        let http = HttpBuilder::new(token.as_ref()).build();

        let client = ClientBuilder::new_with_http(http, GatewayIntents::MESSAGE_CONTENT)
            .await
            .expect("check bot token / failed create client");

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

    pub async fn into_storage(self, guild_name: &str) -> eyre::Result<storage::DiscordStorage> {
        let storage_guild_id = self.get_guild_info(guild_name).await?.id;

        Ok(storage::DiscordStorage::new(self, storage_guild_id))
    }

    pub async fn from_env() -> Self {
        Self::new(std::env::var("DISCORD_BOT_TOKEN").unwrap()).await
    }

    pub fn latest_message_stream(
        &self,
        channel_id: ChannelId,
    ) -> impl Stream<Item = serenity::Result<Message>> + '_ {
        let http = self.http();
        Box::pin(channel_id.messages_iter(http))
    }

    pub fn http(&self) -> &Http {
        self.client.cache_and_http.http()
    }

    pub fn serenity_cache(&self) -> Arc<serenity::cache::Cache> {
        Arc::clone(&self.client.cache_and_http.cache)
    }

    pub async fn get_message(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
    ) -> eyre::Result<Message> {
        debug::time!("get_message", {
            self.http()
                .get_message(channel_id.into(), message_id.into())
                .await
                .context("failed get_message")
        })
    }

    pub async fn get_pins(&self, channel_id: ChannelId) -> eyre::Result<Vec<Message>> {
        debug::time!("get_pins", {
            self.http()
                .get_pins(channel_id.into())
                .await
                .context("failed get_pins")
        })
    }

    pub async fn set_pin(&self, channel_id: ChannelId, message_id: MessageId) -> eyre::Result<()> {
        debug::time!("set_pin", {
            self.http()
                .pin_message(
                    channel_id.into(),
                    message_id.into(),
                    Some("add table schema"),
                )
                .await
                .context("failed set_pin")
        })
    }

    pub async fn set_unpin(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
    ) -> eyre::Result<()> {
        debug::time!("set_unpin", {
            self.http()
                .unpin_message(
                    channel_id.into(),
                    message_id.into(),
                    Some("remove table schema"),
                )
                .await
                .context("failed set_unpin")
        })
    }

    pub async fn send_message(
        &self,
        channel_id: ChannelId,
        content: impl ToString,
    ) -> eyre::Result<Message> {
        debug::time!("send_message", {
            channel_id
                .send_message(self.http(), |m| m.content(content))
                .await
                .context("failed send_message")
        })
    }

    pub async fn edit_message(
        &self,
        channel_id: ChannelId,
        message_id: impl Into<MessageId>,
        content: impl ToString,
    ) -> eyre::Result<Message> {
        debug::time!("edit_message", {
            channel_id
                .edit_message(self.http(), message_id, |m| m.content(content))
                .await
                .context("failed edit_message")
        })
    }

    pub async fn delete_message(
        &self,
        channel_id: ChannelId,
        message_id: impl Into<MessageId>,
    ) -> eyre::Result<()> {
        debug::time!("delete_message", {
            channel_id
                .delete_message(self.http(), message_id)
                .await
                .context("failed delete_message")
        })
    }

    pub async fn get_guild_info(&self, guild_name: impl AsRef<str>) -> eyre::Result<GuildInfo> {
        debug::time!("get_guild_info", {
            self.current_user
                .guilds(&self.http())
                .await
                .context("failed get_guild_info")?
                .into_iter()
                .find(|guild| guild.name == guild_name.as_ref())
                .ok_or(eyre::eyre!("not found guild_name: {}", guild_name.as_ref()))
        })
    }

    pub async fn get_channels(
        &self,
        guild_id: GuildId,
    ) -> eyre::Result<HashMap<ChannelId, GuildChannel>> {
        debug::time!("get_channels", {
            guild_id
                .channels(&self.http())
                .await
                .context("failed get_channels")
        })
    }

    pub async fn get_channel_id(
        &self,
        guild_id: GuildId,
        channel_name: impl AsRef<str>,
    ) -> eyre::Result<Option<ChannelId>> {
        debug::time!("get_channel_id", {
            let channels = self.get_channels(guild_id).await?;

            Ok(channels.into_iter().find_map(|(channel_id, channel)| {
                (channel.name == channel_name.as_ref()).then_some(channel_id)
            }))
        })
    }

    /// required Manage Channels permission
    pub async fn create_channel(
        &self,
        guild_id: GuildId,
        builder: impl FnOnce(&mut CreateChannel) -> &mut CreateChannel,
    ) -> eyre::Result<GuildChannel> {
        debug::time!("create_channel", {
            let http = self.http();

            let guild = http
                .get_guild(guild_id.into())
                .await
                .context("failed get_guild")?;

            guild
                .create_channel(http, builder)
                .await
                .context("failed create_channel")
        })
    }

    pub async fn delete_channel(&self, channel_id: ChannelId) -> eyre::Result<Channel> {
        debug::time!("delete_channel", {
            channel_id
                .delete(self.http())
                .await
                .context("failed delete_channel")
        })
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
        let guild_info = db.get_guild_info("개발자 모임").await.unwrap();

        println!("{guild_info:?}");
    }

    #[ignore]
    #[tokio::test]
    async fn send_message() {
        let db = db().await;

        let guild_id = db.get_guild_info("개발자 모임").await.unwrap().id;
        let channel_id = db.get_channel_id(guild_id, "일반").await.unwrap().unwrap();
        let message = db.send_message(channel_id, "hello").await.unwrap();

        println!("{message:?}");
    }

    #[ignore]
    #[tokio::test]
    async fn edit_message() {
        let db = db().await;

        let guild_id = db.get_guild_info("개발자 모임").await.unwrap().id;
        let channel_id = db.get_channel_id(guild_id, "일반").await.unwrap().unwrap();
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

        let guild_id = db.get_guild_info("개발자 모임").await.unwrap().id;
        let channel_id = db.get_channel_id(guild_id, "일반").await.unwrap().unwrap();
        println!("{channel_id:?}",);
    }

    #[ignore]
    #[tokio::test]
    async fn messages_iter() {
        let db = db().await;

        let guild_id = db.get_guild_info("개발자 모임").await.unwrap().id;
        let channel_id = db.get_channel_id(guild_id, "일반").await.unwrap().unwrap();
        let mut messages = db.latest_message_stream(channel_id).take(2);

        while let Some(messages) = messages.next().await {
            println!("{:#?}", messages.unwrap());
            println!()
        }
    }

    #[ignore]
    #[tokio::test]
    async fn get_pins() {
        let db = db().await;

        let guild_id = db.get_guild_info("개발자 모임").await.unwrap().id;
        let channel_id = db.get_channel_id(guild_id, "일반").await.unwrap().unwrap();

        let messages = db.get_pins(channel_id).await.unwrap();
        println!("{:#?}", messages[0])
    }

    #[ignore]
    #[tokio::test]
    async fn get_message() {
        let db = db().await;

        let guild_id = db.get_guild_info("개발자 모임").await.unwrap().id;
        let channel_id = db.get_channel_id(guild_id, "일반").await.unwrap().unwrap();

        let err = db
            .get_message(
                channel_id,
                serenity::model::id::MessageId(881_000_000_000_000_000),
            )
            .await
            .unwrap_err();
        println!("{err}");
    }
}

// Pin Target Message
// Message {
//     ...
//     content: "..."",
//     kind: Regular,
//     ...
// }

// Pin Result Message
// Message {
//     ...
//     kind: PinsAdd,
//     ...
// }

// Pined Message
// Message {
//     ...
//     pinned: true,
//     kind: Regular,
//     ...
// }
