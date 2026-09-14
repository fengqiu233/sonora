use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

mod audioscrobbler;
mod listenbrainz;
mod maloja;

/// A track shorter than this never counts, and a play this long always does. Last.fm wrote both
/// rules and ListenBrainz and Maloja repeat them, so they belong to the play rather than to any
/// one service.
const MIN_LENGTH: Duration = Duration::from_secs(30);
const FULL_PLAY: Duration = Duration::from_secs(240);

/// Every scrobbling service the app offers, in the order the settings screen lists them.
pub fn services() -> Vec<Arc<dyn Service>> {
    vec![
        Arc::new(audioscrobbler::LASTFM),
        Arc::new(audioscrobbler::LIBREFM),
        Arc::new(listenbrainz::ListenBrainz),
        Arc::new(maloja::Maloja),
    ]
}

/// One listen: what played and when it started.
#[derive(Clone, Debug)]
pub struct Play {
    pub artist: String,
    pub title: String,
    pub album: Option<String>,
    pub duration: Duration,
    pub at: SystemTime,
}

impl Play {
    /// Whether `played` has earned the listen a submission. A track under thirty seconds never
    /// earns one, however long it ran.
    pub fn earned(&self, played: Duration) -> bool {
        self.duration >= MIN_LENGTH
            && (played >= FULL_PLAY || played.as_secs_f64() * 2. >= self.duration.as_secs_f64())
    }

    /// The start of the play in seconds since the epoch, which is how every service spells it.
    pub fn timestamp(&self) -> i64 {
        let seconds = self
            .at
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        i64::try_from(seconds).unwrap_or(i64::MAX)
    }

    /// The album, dropped when the provider left it empty.
    fn release(&self) -> Option<&str> {
        self.album.as_deref().filter(|album| !album.is_empty())
    }
}

/// How a service is linked, which is also what the settings row draws.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Link {
    /// Nothing to type. The app opens the browser and catches the approval. Libre.fm, which
    /// issues no API accounts of its own.
    Browser,
    /// An API key and secret the user created, then the same browser approval. Last.fm.
    Keys,
    /// A token copied from the service's settings page. ListenBrainz.
    Token,
    /// A server url and one of that server's API keys. Maloja.
    Server,
}

/// What the settings screen collected for a `Link`. A service refuses a variant it did not ask
/// for.
#[derive(Clone, Debug)]
pub enum Secret {
    None,
    Keys { key: String, secret: String },
    Token(String),
    Server { url: String, key: String },
}

/// A linked scrobbling account, both as `Service::connect` returns it and as `settings.json`
/// stores it. `session` is whatever authenticates a later submission, so an empty one means the
/// account is not linked.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Account {
    /// The API key, for the services that issue one.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub key: String,
    /// The shared secret that signs requests. Only the audioscrobbler protocol uses it.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub secret: String,
    /// The session key, user token or server key that authenticates a submission.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub session: String,
    /// The server this account lives on, for the services the user hosts themselves. Empty means
    /// the service's own public instance.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub server: String,
    /// What the settings row calls the account.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    pub enabled: bool,
}

impl Account {
    /// Whether the account carries enough to submit with.
    pub fn linked(&self) -> bool {
        !self.session.is_empty()
    }
}

/// A service that accepts listens. `Service::scrobbler` builds one from a linked account.
#[async_trait]
pub trait Scrobbler: Send + Sync {
    /// The service slug, which is also the log prefix.
    fn id(&self) -> &'static str;

    /// Tells the service what is playing right now. A service with no such concept keeps the
    /// default and makes no request.
    async fn now_playing(&self, play: &Play) -> Result<()> {
        let _ = play;
        Ok(())
    }

    /// Submits finished listens. Every service takes a batch, so one listen is a slice of one.
    async fn scrobble(&self, plays: &[Play]) -> Result<()>;
}

/// A scrobbling service the settings screen offers: how it is linked, and how a link becomes a
/// `Scrobbler`.
#[async_trait]
pub trait Service: Send + Sync {
    /// The slug that keys the account in `settings.json` and names the i18n keys of its row.
    fn id(&self) -> &'static str;

    /// What the settings row has to collect before `connect` can run.
    fn link(&self) -> Link;

    /// Where the user goes to create the credentials `link` asks for.
    fn signup(&self) -> Option<&'static str> {
        None
    }

    /// Turns collected input into a stored account. A `Browser` or `Keys` service opens the
    /// browser itself, once its callback listener is up, and resolves when the user approves or
    /// the wait runs out.
    async fn connect(&self, secret: Secret) -> Result<Account>;

    /// Builds the sender for an account `connect` returned.
    fn scrobbler(&self, account: &Account) -> Result<Arc<dyn Scrobbler>>;
}
