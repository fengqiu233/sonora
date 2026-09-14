use std::sync::Arc;

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use reqwest::header::AUTHORIZATION;
use serde::{Deserialize, Serialize};

use super::{Account, Link, Play, Scrobbler, Secret, Service};

/// The public instance. An account that names its own `server` is used instead, which is how a
/// self-hosted ListenBrainz is reached without a second settings field.
const API: &str = "https://api.listenbrainz.org";
const SETTINGS: &str = "https://listenbrainz.org/settings/";
/// How many listens the endpoint takes in one call.
const BATCH: usize = 50;
const CLIENT: &str = "Sonora";

pub struct ListenBrainz;

#[async_trait]
impl Service for ListenBrainz {
    fn id(&self) -> &'static str {
        "listenbrainz"
    }

    fn link(&self) -> Link {
        Link::Token
    }

    fn signup(&self) -> Option<&'static str> {
        Some(SETTINGS)
    }

    async fn connect(&self, secret: Secret) -> Result<Account> {
        let Secret::Token(token) = secret else {
            bail!("listenbrainz needs a user token");
        };
        let token = token.trim().to_owned();
        if token.is_empty() {
            bail!("listenbrainz needs a user token");
        }

        let answer: Validation = reqwest::Client::new()
            .get(format!("{API}/1/validate-token"))
            .header(AUTHORIZATION, format!("Token {token}"))
            .send()
            .await
            .context("cannot reach listenbrainz")?
            .json()
            .await
            .context("cannot read the listenbrainz answer")?;

        if !answer.valid {
            let message = answer.message.unwrap_or_default();
            bail!("listenbrainz refused the token: {message}");
        }

        Ok(Account {
            session: token,
            name: answer.user_name.unwrap_or_default(),
            enabled: true,
            ..Account::default()
        })
    }

    fn scrobbler(&self, account: &Account) -> Result<Arc<dyn Scrobbler>> {
        if !account.linked() {
            bail!("listenbrainz has no user token");
        }
        let api = match account.server.is_empty() {
            true => API.to_owned(),
            false => account.server.trim_end_matches('/').to_owned(),
        };
        Ok(Arc::new(Sender {
            api,
            token: account.session.clone(),
            http: reqwest::Client::new(),
        }))
    }
}

struct Sender {
    api: String,
    token: String,
    http: reqwest::Client,
}

#[async_trait]
impl Scrobbler for Sender {
    fn id(&self) -> &'static str {
        "listenbrainz"
    }

    async fn now_playing(&self, play: &Play) -> Result<()> {
        self.submit("playing_now", &[listen(play, false)]).await
    }

    async fn scrobble(&self, plays: &[Play]) -> Result<()> {
        for batch in plays.chunks(BATCH) {
            let listens: Vec<Listen<'_>> = batch.iter().map(|play| listen(play, true)).collect();
            self.submit("single", &listens).await?;
        }
        Ok(())
    }
}

impl Sender {
    async fn submit(&self, kind: &str, payload: &[Listen<'_>]) -> Result<()> {
        let answer = self
            .http
            .post(format!("{}/1/submit-listens", self.api))
            .header(AUTHORIZATION, format!("Token {}", self.token))
            .json(&Submission {
                listen_type: kind,
                payload,
            })
            .send()
            .await
            .context("cannot reach listenbrainz")?;

        if answer.status().is_success() {
            return Ok(());
        }
        let status = answer.status();
        let body = answer.text().await.unwrap_or_default();
        bail!("listenbrainz refused the request ({status}): {body}");
    }
}

/// Builds one listen. `timed` is false for the now-playing report, which carries no start time.
fn listen(play: &Play, timed: bool) -> Listen<'_> {
    Listen {
        listened_at: timed.then(|| play.timestamp()),
        track_metadata: Metadata {
            artist_name: &play.artist,
            track_name: &play.title,
            release_name: play.release(),
            additional_info: Extra {
                duration_ms: play.duration.as_millis().min(u64::MAX as u128) as u64,
                media_player: CLIENT,
                submission_client: CLIENT,
            },
        },
    }
}

#[derive(Deserialize)]
struct Validation {
    valid: bool,
    user_name: Option<String>,
    message: Option<String>,
}

#[derive(Serialize)]
struct Submission<'a> {
    listen_type: &'a str,
    payload: &'a [Listen<'a>],
}

#[derive(Serialize)]
struct Listen<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    listened_at: Option<i64>,
    track_metadata: Metadata<'a>,
}

#[derive(Serialize)]
struct Metadata<'a> {
    artist_name: &'a str,
    track_name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    release_name: Option<&'a str>,
    additional_info: Extra,
}

#[derive(Serialize)]
struct Extra {
    duration_ms: u64,
    media_player: &'static str,
    submission_client: &'static str,
}
