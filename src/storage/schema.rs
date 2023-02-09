use std::str::FromStr;

use gluesql_core::data::Schema;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct DiscordSchema(pub Schema);

impl FromStr for DiscordSchema {
    type Err = eyre::Report;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let text = text.trim();
        let text = text
            .strip_prefix(r#"```json"#)
            .unwrap_or(text)
            .strip_suffix(r#"```"#)
            .unwrap_or(text);

        serde_json::from_str(text).map_err(Into::into)
    }
}

impl DiscordSchema {
    pub fn to_codeblock(&self) -> eyre::Result<String> {
        let text = serde_json::to_string_pretty(&self)?;

        Ok(format!(
            r#"
```json
{text}
```"#
        ))
    }
}

#[cfg(test)]
mod tests {
    use gluesql_core::{
        ast::{AstLiteral, ColumnDef, ColumnOption, ColumnOptionDef, Expr},
        prelude::DataType,
    };

    use super::*;

    #[test]
    fn from_str() {
        let text = r#"
        ```json
        {
          "table_name": "User",
          "column_defs": [
            {
              "name": "id",
              "data_type": "Int",
              "options": []
            },
            {
              "name": "name",
              "data_type": "Text",
              "options": [
                {
                  "name": null,
                  "option": "Null"
                },
                {
                  "name": null,
                  "option": {
                    "Default": {
                      "Literal": {
                        "QuotedString": "glue"
                      }
                    }
                  }
                }
              ]
            }
          ],
          "indexes": []
        }
        ```
        "#;

        let schema = DiscordSchema::from_str(text).unwrap();

        println!("{schema:#?}");
    }

    #[test]
    fn to_codeblock() {
        let schema = DiscordSchema(Schema {
            table_name: "User".to_owned(),
            column_defs: vec![
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Int,
                    options: Vec::new(),
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    options: vec![
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Null,
                        },
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Default(Expr::Literal(AstLiteral::QuotedString(
                                "glue".to_owned(),
                            ))),
                        },
                    ],
                },
            ],
            indexes: Vec::new(),
        });

        let codeblock = schema.to_codeblock().unwrap();

        println!("{codeblock}");
    }
}
