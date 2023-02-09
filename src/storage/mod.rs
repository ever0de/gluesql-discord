mod gluesql {
    pub use gluesql_core::result::Error;
    pub use gluesql_core::result::{MutResult, Result};
}
pub mod schema;

use std::str::FromStr;

use async_trait::async_trait;
use eyre::Context;
use gluesql_core::{
    ast::{ColumnOption, ColumnOptionDef},
    data::Schema,
    prelude::{Key, Row},
    result::TrySelf,
    store::{RowIter, Store, StoreMut},
};
use serenity::{
    futures::TryStreamExt,
    model::prelude::{ChannelId, GuildChannel, GuildId, MessageId, MessageType},
};

use crate::{utils, Discord};

use self::schema::DiscordSchema;

pub struct DiscordStorage {
    discord: Discord,
    storage_guild_id: GuildId,
}

impl DiscordStorage {
    pub fn new(discord: Discord, storage_guild_id: GuildId) -> Self {
        Self {
            discord,
            storage_guild_id,
        }
    }

    pub async fn get_schema(&self, channel: GuildChannel) -> eyre::Result<Option<Schema>> {
        let pins = self.discord.get_pins(channel.id).await?;

        let message = pins.into_iter().next();
        let message = match message {
            Some(msg) => msg,
            None => return Ok(None),
        };

        let cache = self.discord.cache();
        let content = message.content_safe(cache);

        let schema = DiscordSchema::from_str(&content)?;
        Ok(Some(schema.0))
    }
}

trait IntoStorageErr<T> {
    fn into_storage_err(self) -> gluesql::Result<T>;
}

impl<T, E: Into<Box<dyn std::error::Error + Send + Sync>>> IntoStorageErr<T> for Result<T, E> {
    fn into_storage_err(self) -> gluesql::Result<T> {
        self.map_err(|err| gluesql::Error::Storage(err.into()))
    }
}

#[async_trait(?Send)]
impl Store for DiscordStorage {
    async fn fetch_schema(&self, channel_name: &str) -> gluesql::Result<Option<Schema>> {
        let channel_name = channel_name.to_lowercase();

        let channels = self
            .discord
            .get_channels(self.storage_guild_id)
            .await
            .into_storage_err()?;

        let channel = channels
            .into_iter()
            .find(|channel| channel.name == channel_name);

        match channel {
            Some(channel) => self.get_schema(channel).await.into_storage_err(),
            None => Ok(None),
        }
    }

    async fn fetch_all_schemas(&self) -> gluesql::Result<Vec<Schema>> {
        let channels = self
            .discord
            .get_channels(self.storage_guild_id)
            .await
            .into_storage_err()?;

        let mut schemas = Vec::new();
        for channel in channels {
            let schema = self.get_schema(channel).await.into_storage_err()?;
            if let Some(schema) = schema {
                schemas.push(schema);
            }
        }

        Ok(schemas)
    }

    async fn fetch_data(&self, channel_name: &str, key: &Key) -> gluesql::Result<Option<Row>> {
        let channel_name = channel_name.to_lowercase();
        let message_id: u64 = match key {
            Key::Str(id) => id
                .parse()
                .map_err(|err| gluesql::Error::Storage(format!("invalid key: {err}").into()))?,
            _ => return Err(gluesql::Error::Storage("invalid key".into())),
        };
        let message_id = MessageId(message_id);

        let channel_id = self
            .discord
            .get_channel_id(self.storage_guild_id, channel_name)
            .await
            .into_storage_err()?
            .ok_or_else(|| gluesql::Error::Storage("fetch_data) not found channel".into()))?;

        let message = self.discord.get_message(channel_id, message_id).await.ok();
        let message = match message {
            Some(message) => message,
            None => return Ok(None),
        };

        let cache = self.discord.cache();
        let content = message.content_safe(cache);

        let row: Row = utils::from_discord_json(&content).into_storage_err()?;

        Ok(Some(row))
    }

    async fn scan_data(&self, channel_name: &str) -> gluesql::Result<RowIter> {
        let channel_name = channel_name.to_lowercase();
        let channel_id = self
            .discord
            .get_channel_id(self.storage_guild_id, channel_name)
            .await
            .into_storage_err()?
            .ok_or_else(|| gluesql::Error::Storage("scan_data) not found channel".into()))?;

        let messages = self
            .discord
            .latest_message_iter(channel_id)
            .await
            .map_ok(|message| {
                let message = match message.kind {
                    MessageType::Regular if !message.pinned => message,
                    _ => return Ok(None),
                };

                let cache = self.discord.cache();
                let content = message.content_safe(cache);

                let row: Row = utils::from_discord_json(&content).into_storage_err()?;
                let key = Key::Str(message.id.0.to_string());

                gluesql::Result::Ok(Some((key, row)))
            })
            .try_collect::<Vec<_>>()
            .await
            .into_storage_err()?;

        Ok(Box::new(
            messages.into_iter().filter_map(|row| row.transpose()).rev(),
        ))
    }
}

#[async_trait(?Send)]
impl StoreMut for DiscordStorage {
    async fn insert_schema(self, schema: &Schema) -> gluesql::MutResult<Self, ()> {
        if schema.column_defs.iter().any(|column_def| {
            column_def
                .options
                .iter()
                .any(|ColumnOptionDef { option, .. }| {
                    matches!(option, ColumnOption::Unique { is_primary: true })
                })
        }) {
            return Err((
                self,
                gluesql::Error::Storage("primary key is not supported".into()),
            ));
        }

        let storage = self;
        let channel_name = &schema.table_name.to_lowercase();

        let (storage, channel_id) = storage
            .discord
            .get_channel_id(storage.storage_guild_id, channel_name)
            .await
            .into_storage_err()
            .try_self(storage)?;

        let (storage, channel_id) = match channel_id {
            Some(channel_id) => (storage, channel_id),
            None => {
                let (storage, channel) = storage
                    .discord
                    .create_channel(storage.storage_guild_id, |f| f.name(&schema.table_name))
                    .await
                    .into_storage_err()
                    .try_self(storage)?;

                (storage, channel.id)
            }
        };

        let (storage, pin_messages) = storage
            .discord
            .get_pins(channel_id)
            .await
            .into_storage_err()
            .try_self(storage)?;

        if !pin_messages.is_empty() {
            return Err((
                storage,
                gluesql::Error::Storage(
                    format!("channel is already pinned: {channel_name}").into(),
                ),
            ));
        }

        let schema = DiscordSchema(schema.clone());
        let (storage, content) = utils::to_discord_json(&schema)
            .into_storage_err()
            .try_self(storage)?;

        let (storage, message) = storage
            .discord
            .send_message(channel_id, content)
            .await
            .into_storage_err()
            .try_self(storage)?;

        let (storage, _) = storage
            .discord
            .set_pin(channel_id, message.id)
            .await
            .into_storage_err()
            .try_self(storage)?;

        Ok((storage, ()))
    }

    async fn delete_schema(self, channel_name: &str) -> gluesql::MutResult<Self, ()> {
        let storage = self;
        let channel_name = &channel_name.to_lowercase();

        let (storage, channel_id) = storage
            .discord
            .get_channel_id(storage.storage_guild_id, channel_name)
            .await
            .into_storage_err()
            .try_self(storage)?;
        let (storage, channel_id) = channel_id
            .ok_or_else(|| gluesql::Error::Storage("delete_schema) not found channel".into()))
            .try_self(storage)?;

        let (storage, _) = storage
            .discord
            .delete_channel(channel_id)
            .await
            .into_storage_err()
            .try_self(storage)?;

        Ok((storage, ()))
    }

    async fn append_data(self, channel_name: &str, rows: Vec<Row>) -> gluesql::MutResult<Self, ()> {
        let storage = self;
        let channel_name = &channel_name.to_lowercase();

        let (storage, channel_id) = storage
            .discord
            .get_channel_id(storage.storage_guild_id, channel_name)
            .await
            .into_storage_err()
            .try_self(storage)?;
        let (storage, channel_id) = channel_id
            .ok_or_else(|| gluesql::Error::Storage("append_data) not found channel".into()))
            .try_self(storage)?;

        async fn append_row(
            storage: &DiscordStorage,
            channel_id: ChannelId,
            row: Row,
        ) -> eyre::Result<()> {
            let content = utils::to_discord_json(&row)?;

            storage
                .discord
                .send_message(channel_id, content)
                .await
                .into_storage_err()?;

            Ok(())
        }

        for row in rows {
            if let Err(err) = append_row(&storage, channel_id, row)
                .await
                .into_storage_err()
            {
                return Err((storage, err));
            }
        }

        Ok((storage, ()))
    }

    async fn insert_data(
        self,
        channel_name: &str,
        rows: Vec<(Key, Row)>,
    ) -> gluesql::MutResult<Self, ()> {
        let storage = self;
        let channel_name = &channel_name.to_lowercase();

        let (storage, channel_id) = storage
            .discord
            .get_channel_id(storage.storage_guild_id, channel_name)
            .await
            .into_storage_err()
            .try_self(storage)?;
        let (storage, channel_id) = channel_id
            .ok_or_else(|| gluesql::Error::Storage("insert_data) not found channel".into()))
            .try_self(storage)?;

        async fn update_or_insert_row(
            storage: &DiscordStorage,
            channel_id: ChannelId,
            row: (Key, Row),
        ) -> eyre::Result<()> {
            let (key, row) = row;

            let key = match key {
                Key::Str(key) => key,
                _ => return Err(eyre::eyre!("invalid key {key:?}")),
            };

            let message_id = MessageId(key.parse().context("failed key parsing")?);

            let content = utils::to_discord_json(&row)?;

            let message = storage
                .discord
                .get_message(channel_id, message_id)
                .await
                .ok();

            match message {
                Some(_) => {
                    storage
                        .discord
                        .edit_message(channel_id, message_id, content)
                        .await
                        .into_storage_err()?;
                }
                None => {
                    storage
                        .discord
                        .send_message(channel_id, content)
                        .await
                        .into_storage_err()?;
                }
            }

            Ok(())
        }

        for row in rows {
            if let Err(err) = update_or_insert_row(&storage, channel_id, row)
                .await
                .into_storage_err()
            {
                return Err((storage, err));
            }
        }

        Ok((storage, ()))
    }

    async fn delete_data(self, channel_name: &str, keys: Vec<Key>) -> gluesql::MutResult<Self, ()> {
        let storage = self;
        let channel_name = &channel_name.to_lowercase();

        let (storage, channel_id) = storage
            .discord
            .get_channel_id(storage.storage_guild_id, channel_name)
            .await
            .into_storage_err()
            .try_self(storage)?;
        let (storage, channel_id) = channel_id
            .ok_or_else(|| gluesql::Error::Storage("delete_data) not found channel".into()))
            .try_self(storage)?;

        async fn delete_row(
            storage: &DiscordStorage,
            channel_id: ChannelId,
            key: Key,
        ) -> eyre::Result<()> {
            let key = match key {
                Key::Str(key) => key,
                _ => return Err(eyre::eyre!("invalid key {key:?}")),
            };

            let message_id = MessageId(key.parse().context("failed key parsing")?);

            storage
                .discord
                .delete_message(channel_id, message_id)
                .await
                .into_storage_err()?;

            Ok(())
        }

        for key in keys {
            if let Err(err) = delete_row(&storage, channel_id, key)
                .await
                .into_storage_err()
            {
                return Err((storage, err));
            }
        }

        Ok((storage, ()))
    }
}
