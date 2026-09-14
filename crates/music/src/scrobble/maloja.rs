use std::sync::Arc;

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{Account, Link, Play, Scrobbler, Secret, Service};

/// Where the native API lives under a Maloja server's root.
const API: &str = "/apis/mlj_1";

pub struct Maloja;

#[async_trait]
impl Service for Maloja {
    fn id(&self) -> &'static str {
        "maloja"
    }

    fn link(&self) -> Link {
        Link::Server
    }

    async fn connect(&self, secret: Secret) -> Result<Account> {
        let Secret::Server { url, key } = secret else {
            bail!("maloja needs a server url and an api key");
        };
        let (url, key) = (root(&url), key.trim().to_owned());
        if url.is_empty() || key.is_empty() {
            bail!("maloja needs a server url and an api key");
        }

        let http = reqwest::Client::new();
        let answer = http
            .get(format!("{url}{API}/test"))
            .query(&[("key", &key)])
            .send()
            .await
            .context("cannot reach the maloja server")?;
        if !answer.status().is_success() {
            bail!(
                "the maloja server refused the api key ({})",
                answer.status()
            );
        }

        Ok(Account {
            server: url.clone(),
            session: key,
            name: name(&http, &url).await.unwrap_or_else(|| host(&url)),
            enabled: true,
            ..Account::default()
        })
    }

    fn scrobbler(&self, account: &Account) -> Result<Arc<dyn Scrobbler>> {
        if !account.linked() || account.server.is_empty() {
            bail!("maloja has no server and api key");
        }
        Ok(Arc::new(Sender {
            server: account.server.clone(),
            key: account.session.clone(),
            http: reqwest::Client::new(),
        }))
    }
}

/// Maloja has no now-playing concept, so this only ever submits finished listens, one request
/// each.
struct Sender {
    server: String,
    key: String,
    http: reqwest::Client,
}

#[async_trait]
impl Scrobbler for Sender {
    fn id(&self) -> &'static str {
        "maloja"
    }

    async fn scrobble(&self, plays: &[Play]) -> Result<()> {
        for play in plays {
            let answer: Answer = self
                .http
                .post(format!("{}{API}/newscrobble", self.server))
                .json(&New {
                    key: &self.key,
                    artists: std::slice::from_ref(&play.artist),
                    title: &play.title,
                    album: play.release(),
                    length: play.duration.as_secs(),
                    time: play.timestamp(),
                })
                .send()
                .await
                .context("cannot reach the maloja server")?
                .json()
                .await
                .context("cannot read the maloja answer")?;

            if answer.status != "success" && answer.status != "ok" {
                let reason = answer
                    .error
                    .map(|error| error.desc)
                    .unwrap_or_else(|| answer.status.clone());
                bail!("the maloja server refused the scrobble: {reason}");
            }
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct New<'a> {
    key: &'a str,
    artists: &'a [String],
    title: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    album: Option<&'a str>,
    length: u64,
    time: i64,
}

#[derive(Deserialize)]
struct Answer {
    status: String,
    error: Option<Refusal>,
}

#[derive(Deserialize)]
struct Refusal {
    desc: String,
}

#[derive(Deserialize)]
struct Info {
    name: Option<String>,
}

/// What the instance calls itself, which is friendlier in the settings row than its host.
async fn name(http: &reqwest::Client, url: &str) -> Option<String> {
    let info: Info = http
        .get(format!("{url}{API}/serverinfo"))
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    info.name.filter(|name| !name.is_empty())
}

/// The server url without its trailing slash, with a scheme added when the user left it out.
fn root(url: &str) -> String {
    let url = url.trim().trim_end_matches('/');
    match url.is_empty() || url.contains("://") {
        true => url.to_owned(),
        false => format!("https://{url}"),
    }
}

fn host(url: &str) -> String {
    url.split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(url)
        .split('/')
        .next()
        .unwrap_or(url)
        .to_owned()
}
