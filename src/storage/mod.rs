mod gluesql {
    pub use gluesql_core::result::Error;
    pub use gluesql_core::result::Result;
}

use async_trait::async_trait;
use gluesql_core::{
    ast::{ColumnDef, ColumnUniqueOption},
    chrono::Utc,
    data::Schema,
    prelude::{Key, Value},
    store::{DataRow, RowIter, Store, StoreMut},
};
use serenity::{
    futures::TryStreamExt,
    model::prelude::{GuildChannel, GuildId, MessageId, MessageType},
};

use crate::{debug, discord::Discord, utils};

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

    pub async fn get_schema(&self, channel: GuildChannel) -> eyre::Result<Schema> {
        let pins = self.discord.get_pins(channel.id).await?;

        let message = pins.into_iter().next();
        let message = match message {
            Some(msg) => msg,
            None => {
                return Ok(Schema {
                    table_name: channel.name,
                    column_defs: None,
                    indexes: vec![],
                    engine: None,
                    created: Utc::now().naive_utc(),
                })
            }
        };

        let cache = self.discord.serenity_cache();
        let content = message.content_safe(cache);

        let schema: Schema = utils::from_discord_json(&content)?;
        Ok(schema)
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
        debug::time!("fetch_schema", {
            let channel_name = channel_name.to_lowercase();

            let channel = self
                .discord
                .get_channels(self.storage_guild_id)
                .await
                .into_storage_err()?
                .into_iter()
                .find_map(|(_, channel)| {
                    if channel.name == channel_name {
                        Some(channel)
                    } else {
                        None
                    }
                });

            match channel {
                Some(channel) => self.get_schema(channel).await.into_storage_err().map(Some),
                None => Ok(None),
            }
        })
    }

    async fn fetch_all_schemas(&self) -> gluesql::Result<Vec<Schema>> {
        debug::time!("fetch_all_schemas", {
            let channels = self
                .discord
                .get_channels(self.storage_guild_id)
                .await
                .into_storage_err()?;

            let mut schemas = Vec::new();
            for (_channel_id, channel) in channels {
                let schema = self.get_schema(channel).await.into_storage_err()?;
                schemas.push(schema);
            }

            Ok(schemas)
        })
    }

    async fn fetch_data(&self, channel_name: &str, key: &Key) -> gluesql::Result<Option<DataRow>> {
        debug::time!("fetch_data", {
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

            let cache = self.discord.serenity_cache();
            let content = message.content_safe(cache);

            let row: DataRow = utils::from_discord_json(&content).unwrap_or(DataRow::Map(
                [("content".to_owned(), Value::Str(content))].into(),
            ));
            Ok(Some(row))
        })
    }

    async fn scan_data(&self, channel_name: &str) -> gluesql::Result<RowIter> {
        debug::time!("scan_data", {
            let channel_name = channel_name.to_lowercase();
            let channel_id = self
                .discord
                .get_channel_id(self.storage_guild_id, channel_name)
                .await
                .into_storage_err()?
                .ok_or_else(|| gluesql::Error::Storage("scan_data) not found channel".into()))?;

            let messages = self
                .discord
                .latest_message_stream(channel_id)
                .map_ok(|message| {
                    let message = match message.kind {
                        MessageType::Regular if !message.pinned => message,
                        _ => return Ok(None),
                    };

                    let cache = self.discord.serenity_cache();
                    let content = message.content_safe(cache);

                    let row: DataRow = utils::from_discord_json(&content).unwrap_or(DataRow::Map(
                        [("content".to_owned(), Value::Str(content))].into(),
                    ));
                    let key = Key::Str(message.id.0.to_string());

                    Ok(Some((key, row)))
                })
                .try_collect::<Vec<_>>()
                .await
                .into_storage_err()?;

            Ok(Box::new(messages.into_iter().filter_map(|row| row.transpose()).rev()) as RowIter)
        })
    }
}

#[async_trait(?Send)]
impl StoreMut for DiscordStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> gluesql::Result<()> {
        debug::time!("insert_schema", {
            if schema.column_defs.iter().any(|column_def| {
                column_def.iter().any(|ColumnDef { unique, .. }| {
                    matches!(unique, Some(ColumnUniqueOption { is_primary: true }))
                })
            }) {
                return Err(gluesql::Error::Storage(
                    "primary key is not supported".into(),
                ));
            }

            let channel_name = &schema.table_name.to_lowercase();

            let channel_id = self
                .discord
                .get_channel_id(self.storage_guild_id, channel_name)
                .await
                .into_storage_err()?;

            let channel_id = match channel_id {
                Some(channel_id) => channel_id,
                None => {
                    let channel = self
                        .discord
                        .create_channel(self.storage_guild_id, |f| f.name(&schema.table_name))
                        .await
                        .into_storage_err()?;

                    channel.id
                }
            };

            let pin_messages = self.discord.get_pins(channel_id).await.into_storage_err()?;

            if !pin_messages.is_empty() {
                return Err(gluesql::Error::Storage(
                    format!("channel is already pinned: {channel_name}").into(),
                ));
            }

            let content = utils::to_discord_json(&schema).into_storage_err()?;

            let message = self
                .discord
                .send_message(channel_id, content)
                .await
                .into_storage_err()?;

            self.discord
                .set_pin(channel_id, message.id)
                .await
                .into_storage_err()?;

            Ok(())
        })
    }

    async fn delete_schema(&mut self, channel_name: &str) -> gluesql::Result<()> {
        debug::time!("delete_schema", {
            let channel_name = &channel_name.to_lowercase();

            let channel_id = self
                .discord
                .get_channel_id(self.storage_guild_id, channel_name)
                .await
                .into_storage_err()?;
            let channel_id = channel_id.ok_or_else(|| {
                gluesql::Error::Storage("delete_schema) not found channel".into())
            })?;

            self.discord
                .delete_channel(channel_id)
                .await
                .into_storage_err()?;

            Ok(())
        })
    }

    async fn append_data(&mut self, channel_name: &str, rows: Vec<DataRow>) -> gluesql::Result<()> {
        debug::time!("append_data", {
            let storage = self;
            let channel_name = &channel_name.to_lowercase();

            let channel_id = storage
                .discord
                .get_channel_id(storage.storage_guild_id, channel_name)
                .await
                .into_storage_err()?;
            let channel_id = channel_id
                .ok_or_else(|| gluesql::Error::Storage("append_data) not found channel".into()))?;

            for row in rows {
                let content = utils::to_discord_json(&row).into_storage_err()?;

                storage
                    .discord
                    .send_message(channel_id, content)
                    .await
                    .into_storage_err()?;
            }

            Ok(())
        })
    }

    async fn insert_data(
        &mut self,
        channel_name: &str,
        rows: Vec<(Key, DataRow)>,
    ) -> gluesql::Result<()> {
        debug::time!("insert_data", {
            let channel_name = &channel_name.to_lowercase();

            let channel_id = self
                .discord
                .get_channel_id(self.storage_guild_id, channel_name)
                .await
                .into_storage_err()?;
            let channel_id = channel_id
                .ok_or_else(|| gluesql::Error::Storage("insert_data) not found channel".into()))?;

            for row in rows {
                let (key, row) = row;

                let key = match key {
                    Key::Str(key) => key,
                    _ => {
                        return Err(gluesql::Error::Storage(
                            eyre::eyre!("invalid key {key:?}").into(),
                        ))
                    }
                };

                let message_id = MessageId(key.parse().map_err(|_| {
                    gluesql::Error::Storage("insert_data) failed key parsing".into())
                })?);

                let content = utils::to_discord_json(&row).into_storage_err()?;

                let message = self.discord.get_message(channel_id, message_id).await.ok();

                match message {
                    Some(_) => {
                        self.discord
                            .edit_message(channel_id, message_id, content)
                            .await
                            .into_storage_err()?;
                    }
                    None => {
                        self.discord
                            .send_message(channel_id, content)
                            .await
                            .into_storage_err()?;
                    }
                }
            }

            Ok(())
        })
    }

    async fn delete_data(&mut self, channel_name: &str, keys: Vec<Key>) -> gluesql::Result<()> {
        debug::time!("delete_data", {
            let channel_name = &channel_name.to_lowercase();

            let channel_id = self
                .discord
                .get_channel_id(self.storage_guild_id, channel_name)
                .await
                .into_storage_err()?;
            let channel_id = channel_id
                .ok_or_else(|| gluesql::Error::Storage("delete_data) not found channel".into()))?;

            for key in keys {
                let key = match key {
                    Key::Str(key) => key,
                    _ => {
                        return Err(gluesql::Error::Storage(
                            eyre::eyre!("invalid key {key:?}").into(),
                        ))
                    }
                };

                let message_id = MessageId(key.parse().map_err(|_| {
                    gluesql::Error::Storage("delete_data) failed key parsing".into())
                })?);

                self.discord
                    .delete_message(channel_id, message_id)
                    .await
                    .into_storage_err()?;
            }

            Ok(())
        })
    }
}
