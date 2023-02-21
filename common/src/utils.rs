use serde::{de::DeserializeOwned, Serialize};

pub fn to_discord_json<T: Serialize>(data: &T) -> eyre::Result<String> {
    let text = serde_json::to_string_pretty(data)?;

    Ok(format!(
        r#"
```json
{text}
```"#
    ))
}

pub fn from_discord_json<T: DeserializeOwned>(text: &str) -> eyre::Result<T> {
    let text = text.trim();
    let text = text
        .strip_prefix(r#"```json"#)
        .unwrap_or(text)
        .strip_suffix(r#"```"#)
        .unwrap_or(text);

    serde_json::from_str(text).map_err(Into::into)
}
