//! Deezer data access over two APIs: the public `api.deezer.com` for the catalog, and the
//! web gateway `gw-light.php` (what the browser player uses) for anything scoped to the
//! account — profile, favorites, playlists — plus the `media.deezer.com/v1/get_url` call that
//! hands out the encrypted stream urls.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tokio::task::JoinSet;

use crate::deezer::{decrypt, wire};
use crate::{
    Album, AlbumDetail, Artist, ArtistProfile, HomeFeed, MediaKind, MusicApi, Playlist,
    PlaylistDetail, SavedArtist, Track, UserProfile, distinct_covers,
};

const GATEWAY: &str = "https://www.deezer.com/ajax/gw-light.php";
const PUBLIC: &str = "https://api.deezer.com";
const MEDIA: &str = "https://media.deezer.com/v1/get_url";

/// The audio formats `get_url` knows, best first. The license decides which ones come back.
const FORMATS: [&str; 3] = ["FLAC", "MP3_320", "MP3_128"];

/// The one format user-uploaded files are served as.
const UPLOAD_FORMAT: &str = "MP3_MISC";

/// How many favorites a library page asks for at once.
const LIBRARY_PAGE: u32 = 2000;

const PORTRAIT_LIMIT: usize = 24;

/// The account-scoped half of the handshake `deezer.getUserData` answers.
struct Session {
    /// The `sid` session cookie `deezer.ping` hands out; the `checkForm` token is bound to
    /// it, so every gateway call has to carry both.
    sid: String,
    user_id: String,
    user_name: String,
    /// The `checkForm` token every later gateway call carries.
    api_token: String,
    license_token: String,
}

#[derive(Clone)]
pub struct DeezerClient {
    inner: Arc<Inner>,
}

struct Inner {
    http: reqwest::Client,
    arl: String,
    session: RwLock<Session>,
    /// The master stream secret, fetched once from the web player bundle at connect time.
    secret: OnceLock<decrypt::Secret>,
}

impl DeezerClient {
    /// Signs in with an `arl` cookie: fetches the user data (which validates the cookie) and
    /// the stream secret. An invalid or expired arl answers a user id of 0.
    pub async fn connect(arl: &str) -> Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent("Mozilla/5.0 (X11; Linux x86_64; rv:142.0) Gecko/20100101 Firefox/142.0")
            .build()
            .context("cannot build the deezer http client")?;
        let client = Self {
            inner: Arc::new(Inner {
                http,
                arl: arl.to_owned(),
                session: RwLock::new(Session {
                    sid: String::new(),
                    user_id: String::new(),
                    user_name: String::new(),
                    api_token: String::new(),
                    license_token: String::new(),
                }),
                secret: OnceLock::new(),
            }),
        };
        client.ping().await?;
        client.refresh().await?;
        if client.inner.session.read().await.user_id.is_empty() {
            bail!("the arl was refused; sign in to deezer.com again");
        }
        let secret = decrypt::secret(&client.inner.http).await;
        // set once here, before the client is cloned anywhere
        client.inner.secret.set(secret).ok();
        Ok(client)
    }

    /// Opens the session: `deezer.ping` answers a `SESSION` id that goes out as the `sid`
    /// cookie from here on. Without it the gateway refuses authenticated calls with a CSRF
    /// error, because the `checkForm` token is bound to that session.
    async fn ping(&self) -> Result<()> {
        let url = format!("{GATEWAY}?method=deezer.ping&input=3&api_version=1.0&api_token=");
        let text = self
            .inner
            .http
            .get(url)
            .header("Cookie", format!("arl={}", self.inner.arl))
            .send()
            .await
            .context("cannot reach the deezer gateway for deezer.ping")?
            .text()
            .await
            .context("cannot read the deezer gateway answer")?;
        let answer: Value = serde_json::from_str(&text)
            .context("the deezer gateway answered deezer.ping with no json")?;
        let sid = wire::text(&answer["results"], &["SESSION"])
            .unwrap_or_default()
            .to_owned();
        self.inner.session.write().await.sid = sid;
        Ok(())
    }

    /// Pulls a fresh `checkForm` api token and license token from `deezer.getUserData`.
    async fn refresh(&self) -> Result<()> {
        let results = self.gw_raw("deezer.getUserData", "", json!({})).await?;
        let user_id = wire::id(&results["USER"]["USER_ID"]).unwrap_or_default();
        let user_name = wire::text(&results["USER"], &["BLOG_NAME"])
            .unwrap_or("Deezer")
            .to_owned();
        let api_token = wire::text(&results, &["checkForm"])
            .unwrap_or_default()
            .to_owned();
        let license_token = wire::text(&results["USER"]["OPTIONS"], &["license_token"])
            .unwrap_or_default()
            .to_owned();
        let mut session = self.inner.session.write().await;
        session.user_id = user_id;
        session.user_name = user_name;
        session.api_token = api_token;
        session.license_token = license_token;
        Ok(())
    }

    async fn user_id(&self) -> String {
        self.inner.session.read().await.user_id.clone()
    }

    /// A gateway call with the current api token. One retry with a refreshed token when the
    /// first answer complains, which is how an expired `checkForm` shows up.
    async fn gw(&self, method: &str, body: Value) -> Result<Value> {
        let token = self.inner.session.read().await.api_token.clone();
        match self.gw_raw(method, &token, body.clone()).await {
            Ok(results) => Ok(results),
            Err(first) => {
                self.refresh().await.ok();
                let token = self.inner.session.read().await.api_token.clone();
                self.gw_raw(method, &token, body).await.map_err(|_| first)
            }
        }
    }

    /// One gateway round trip: POST JSON to `gw-light.php` with the `arl` and `sid` cookies.
    /// The gateway answers HTTP 200 to almost everything, so the `error` field is what
    /// actually decides.
    async fn gw_raw(&self, method: &str, api_token: &str, body: Value) -> Result<Value> {
        let cid = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|at| at.subsec_nanos())
            .unwrap_or(0);
        let url = format!(
            "{GATEWAY}?method={method}&input=3&api_version=1.0&api_token={api_token}&cid={cid}"
        );
        let sid = self.inner.session.read().await.sid.clone();
        let text = self
            .inner
            .http
            .post(url)
            .header("Cookie", format!("arl={}; sid={sid}", self.inner.arl))
            .header("Content-Type", "text/plain;charset=UTF-8")
            .body(body.to_string())
            .send()
            .await
            .with_context(|| format!("cannot reach the deezer gateway for {method}"))?
            .text()
            .await
            .context("cannot read the deezer gateway answer")?;
        let answer: Value = serde_json::from_str(&text)
            .with_context(|| format!("the deezer gateway answered {method} with no json"))?;
        if let Some(error) = answer.get("error") {
            let trouble = match error {
                Value::Object(map) if !map.is_empty() => Some(error.to_string()),
                Value::Array(list) if !list.is_empty() => Some(error.to_string()),
                _ => None,
            };
            if let Some(trouble) = trouble {
                bail!("deezer {method} refused the call: {trouble}");
            }
        }
        Ok(answer.get("results").cloned().unwrap_or(Value::Null))
    }

    /// A public REST call. Also answers HTTP 200 with an `error` object on failure.
    async fn public(&self, path: &str) -> Result<Value> {
        let url = format!("{PUBLIC}{path}");
        let text = self
            .inner
            .http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("cannot reach {url}"))?
            .text()
            .await
            .context("cannot read the deezer api answer")?;
        let answer: Value = serde_json::from_str(&text)
            .with_context(|| format!("the deezer api answered {path} with no json"))?;
        if let Some(error) = answer.get("error")
            && error.is_object()
        {
            bail!("deezer api {path} refused the call: {error}");
        }
        Ok(answer)
    }

    /// One `deezer.pageProfile` tab: the user's favorite albums, artists or playlists,
    /// including the private ones the public api hides.
    async fn profile_tab(&self, tab: &str) -> Result<Vec<Value>> {
        let user_id = self.user_id().await;
        let results = self
            .gw(
                "deezer.pageProfile",
                json!({ "user_id": user_id, "tab": tab, "nb": LIBRARY_PAGE }),
            )
            .await?;
        Ok(results
            .get("TAB")
            .and_then(|tabs| tabs.get(tab))
            .and_then(|section| section.get("data"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }

    async fn favorite(&self, method: &str, key: &str, id: &str) -> Result<()> {
        self.gw(method, json!({ key: id }))
            .await
            .with_context(|| format!("deezer {method} refused the change"))?;
        Ok(())
    }

    /// The full gateway record of one track: metadata plus the `TRACK_TOKEN` playback needs.
    /// A region-locked track comes with a `FALLBACK` record of another release of the same
    /// song, and that one is what plays.
    async fn track_data(&self, track_id: &str) -> Result<Value> {
        let results = self
            .gw("song.getData", json!({ "SNG_ID": track_id }))
            .await
            .with_context(|| format!("cannot load the deezer track {track_id}"))?;
        match results
            .get("FALLBACK")
            .filter(|fallback| !fallback.is_null())
        {
            Some(fallback) => Ok(fallback.clone()),
            None => Ok(results),
        }
    }

    /// Opens the audio of a track: track token, then a stream url from `get_url`, then the
    /// GET itself. Answers the response, the key that decrypts it, and the track's length.
    pub async fn open_stream(
        &self,
        track_id: &str,
    ) -> Result<(reqwest::Response, decrypt::Secret, Option<Duration>)> {
        let data = self.track_data(track_id).await?;
        let effective_id = wire::id(&data["SNG_ID"])
            .with_context(|| format!("the deezer track {track_id} names no id"))?;
        let token = wire::text(&data, &["TRACK_TOKEN"])
            .filter(|token| !token.is_empty())
            .with_context(|| format!("the deezer track {track_id} is not playable"))?;
        let duration = wire::number(&data, &["DURATION"]).map(Duration::from_secs);

        let license = self.inner.session.read().await.license_token.clone();
        let formats: &[&str] = match effective_id.starts_with('-') {
            true => &[UPLOAD_FORMAT],
            false => &FORMATS,
        };
        let mut last = anyhow::anyhow!("no format was offered");
        for format in formats {
            match self.stream_url(&license, token, format).await {
                Ok(Some(url)) => {
                    let response = self
                        .inner
                        .http
                        .get(&url)
                        .send()
                        .await
                        .context("cannot stream the deezer track")?
                        .error_for_status()
                        .context("the deezer cdn refused the stream")?;
                    let secret = self.inner.secret.get().copied().unwrap_or_default();
                    let key = decrypt::track_key(&effective_id, &secret);
                    return Ok((response, key, duration));
                }
                Ok(None) => last = anyhow::anyhow!("format {format} is not licensed"),
                Err(error) => last = error,
            }
        }
        Err(last.context("the deezer track has no playable stream"))
    }

    /// One `get_url` attempt for one format. None when the license does not cover it.
    async fn stream_url(&self, license: &str, token: &str, format: &str) -> Result<Option<String>> {
        let payload = json!({
            "license_token": license,
            "media": [{ "type": "FULL", "formats": [{ "cipher": "BF_CBC_STRIPE", "format": format }] }],
            "track_tokens": [token],
        });
        let answer: Value = self
            .inner
            .http
            .post(MEDIA)
            .json(&payload)
            .send()
            .await
            .context("cannot reach the deezer media api")?
            .json()
            .await
            .context("cannot read the deezer media api answer")?;
        let entry = &answer["data"][0];
        if let Some(errors) = entry.get("errors").and_then(Value::as_array)
            && !errors.is_empty()
        {
            return Ok(None);
        }
        Ok(entry["media"][0]["sources"][0]["url"]
            .as_str()
            .map(str::to_owned))
    }
}

#[async_trait]
impl MusicApi for DeezerClient {
    fn share_url(&self, kind: MediaKind, id: &str) -> Option<String> {
        let kind = match kind {
            MediaKind::Track => "track",
            MediaKind::Album => "album",
            MediaKind::Artist => "artist",
            MediaKind::Playlist => "playlist",
        };
        Some(format!("https://www.deezer.com/{kind}/{id}"))
    }

    async fn profile(&self) -> Result<UserProfile> {
        let session = self.inner.session.read().await;
        Ok(UserProfile {
            id: session.user_id.clone(),
            display_name: session.user_name.clone(),
        })
    }

    async fn artist(&self, artist_id: &str) -> Result<Artist> {
        let detail_path = format!("/artist/{artist_id}");
        let top_path = format!("/artist/{artist_id}/top?limit=20");
        let albums_path = format!("/artist/{artist_id}/albums?limit=50");
        let (detail, top, albums) = tokio::join!(
            self.public(&detail_path),
            self.public(&top_path),
            self.public(&albums_path),
        );
        let detail = detail.context("cannot load the artist")?;
        Ok(Artist {
            name: wire::text(&detail, &["name"])
                .unwrap_or_default()
                .to_owned(),
            cover_large: detail
                .get("picture_xl")
                .or_else(|| detail.get("picture_big"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            biography: None,
            monthly_listeners: wire::number(&detail, &["nb_fan"]),
            top_tracks: top.map(|page| wire::track_list(&page)).unwrap_or_default(),
            albums: albums
                .map(|page| {
                    page.get("data")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default()
                        .iter()
                        .filter_map(wire::album)
                        .collect()
                })
                .unwrap_or_default(),
        })
    }

    async fn artist_profile(&self, artist_id: &str) -> Result<ArtistProfile> {
        let detail = self
            .public(&format!("/artist/{artist_id}"))
            .await
            .context("cannot load the artist")?;
        Ok(ArtistProfile {
            name: wire::text(&detail, &["name"])
                .unwrap_or_default()
                .to_owned(),
            cover_large: detail
                .get("picture_xl")
                .or_else(|| detail.get("picture_big"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            biography: None,
        })
    }

    async fn artist_images(&self, ids: Vec<String>) -> Result<HashMap<String, String>> {
        let mut tasks = JoinSet::new();
        for id in ids.into_iter().take(PORTRAIT_LIMIT) {
            let client = self.clone();
            tasks.spawn(async move {
                let detail = client.public(&format!("/artist/{id}")).await.ok()?;
                let cover = detail
                    .get("picture_big")
                    .and_then(Value::as_str)
                    .map(str::to_owned)?;
                Some((id, cover))
            });
        }
        let mut images = HashMap::new();
        while let Some(result) = tasks.join_next().await {
            if let Ok(Some((id, image))) = result {
                images.insert(id, image);
            }
        }
        Ok(images)
    }

    async fn saved_tracks(&self) -> Result<Vec<Track>> {
        let user_id = self.user_id().await;
        let results = self
            .gw(
                "favorite_song.getList",
                json!({ "user_id": user_id, "start": 0, "nb": LIBRARY_PAGE }),
            )
            .await
            .context("cannot load the favorite tracks")?;
        Ok(wire::track_list(&results))
    }

    async fn set_track_saved(&self, track_id: &str, saved: bool) -> Result<()> {
        let method = match saved {
            true => "favorite_song.add",
            false => "favorite_song.remove",
        };
        self.favorite(method, "SNG_ID", track_id).await
    }

    async fn track(&self, track_id: &str) -> Result<Track> {
        let data = self.track_data(track_id).await?;
        wire::track(&data).with_context(|| format!("cannot read the deezer track {track_id}"))
    }

    async fn track_playcount(&self, _track_id: &str) -> Result<Option<u64>> {
        Ok(None)
    }

    async fn playlists(&self) -> Result<Vec<Playlist>> {
        let user_id = self.user_id().await;
        Ok(self
            .profile_tab("playlists")
            .await
            .context("cannot load the playlists")?
            .iter()
            .filter_map(|value| wire::playlist(value, &user_id))
            .collect())
    }

    async fn create_playlist(&self, name: &str) -> Result<String> {
        let results = self
            .gw(
                "playlist.create",
                json!({ "title": name, "status": 0, "description": "", "songs": [] }),
            )
            .await
            .context("cannot create the playlist")?;
        wire::id(&results)
            .or_else(|| wire::id(&results["PLAYLIST_ID"]))
            .context("the created playlist came back with no id")
    }

    async fn rename_playlist(&self, playlist_id: &str, name: &str) -> Result<()> {
        self.gw(
            "playlist.update",
            json!({ "playlist_id": playlist_id, "title": name }),
        )
        .await
        .context("cannot rename the playlist")?;
        Ok(())
    }

    async fn delete_playlist(&self, playlist_id: &str) -> Result<()> {
        self.gw("playlist.delete", json!({ "playlist_id": playlist_id }))
            .await
            .context("cannot delete the playlist")?;
        Ok(())
    }

    async fn remove_playlist_from_library(&self, _playlist_id: &str) -> Result<()> {
        bail!("deezer cannot unfollow a playlist")
    }

    async fn add_playlist_to_library(&self, _playlist_id: &str) -> Result<()> {
        bail!("deezer cannot follow a playlist")
    }

    async fn set_playlist_public(&self, _playlist_id: &str, _public: bool) -> Result<()> {
        bail!("deezer cannot change a playlist's visibility")
    }

    async fn add_track_to_playlist(&self, playlist_id: &str, track_id: &str) -> Result<()> {
        self.gw(
            "playlist.addSongs",
            json!({ "playlist_id": playlist_id, "songs": [[track_id, 0]] }),
        )
        .await
        .context("cannot add the track to the playlist")?;
        Ok(())
    }

    async fn remove_track_from_playlist(&self, playlist_id: &str, track_id: &str) -> Result<()> {
        self.gw(
            "playlist.deleteSongs",
            json!({ "playlist_id": playlist_id, "songs": [[track_id, 0]] }),
        )
        .await
        .context("cannot remove the track from the playlist")?;
        Ok(())
    }

    async fn saved_albums(&self) -> Result<Vec<Album>> {
        Ok(self
            .profile_tab("albums")
            .await
            .context("cannot load the favorite albums")?
            .iter()
            .filter_map(wire::album)
            .collect())
    }

    async fn set_album_saved(&self, album_id: &str, saved: bool) -> Result<()> {
        let method = match saved {
            true => "favorite_album.add",
            false => "favorite_album.remove",
        };
        self.favorite(method, "ALB_ID", album_id).await
    }

    async fn saved_artists(&self) -> Result<Vec<SavedArtist>> {
        Ok(self
            .profile_tab("artists")
            .await
            .context("cannot load the favorite artists")?
            .iter()
            .filter_map(wire::saved_artist)
            .collect())
    }

    async fn set_artist_saved(&self, artist_id: &str, saved: bool) -> Result<()> {
        let method = match saved {
            true => "artist.addFavorite",
            false => "artist.deleteFavorite",
        };
        self.favorite(method, "ART_ID", artist_id).await
    }

    async fn album(&self, album_id: &str) -> Result<AlbumDetail> {
        let detail = self
            .public(&format!("/album/{album_id}"))
            .await
            .with_context(|| format!("cannot load the album {album_id}"))?;
        let tracks = detail
            .get("tracks")
            .map(wire::track_list)
            .unwrap_or_default();
        let album =
            wire::album(&detail).with_context(|| format!("cannot read the album {album_id}"))?;
        Ok(AlbumDetail { album, tracks })
    }

    async fn album_tracks(&self, album_id: &str) -> Result<Vec<Track>> {
        Ok(self.album(album_id).await?.tracks)
    }

    async fn playlist(&self, playlist_id: &str) -> Result<PlaylistDetail> {
        // the gateway, not the public api: only it can see the user's private playlists
        let results = self
            .gw(
                "deezer.pagePlaylist",
                json!({
                    "playlist_id": playlist_id,
                    "start": 0,
                    "nb": LIBRARY_PAGE,
                    "lang": "en",
                    "tab": 0,
                    "tags": true,
                    "header": true,
                }),
            )
            .await
            .with_context(|| format!("cannot load the playlist {playlist_id}"))?;
        let user_id = self.user_id().await;
        let data = &results["DATA"];
        let tracks = results
            .get("SONGS")
            .map(wire::track_list)
            .unwrap_or_default();
        let mut playlist = wire::playlist(data, &user_id)
            .with_context(|| format!("cannot read the playlist {playlist_id}"))?;
        if playlist.track_count == 0 {
            playlist.track_count = tracks.len() as u32;
        }
        if playlist.cover.is_none() {
            playlist.cover = tracks.iter().find_map(|track| track.cover.clone());
        }
        Ok(PlaylistDetail {
            playlist,
            tracks,
            continuation: None,
        })
    }

    async fn playlist_tracks(&self, playlist_id: &str) -> Result<Vec<Track>> {
        Ok(self.playlist(playlist_id).await?.tracks)
    }

    async fn playlist_covers(&self, playlist_id: &str, wanted: usize) -> Result<Vec<String>> {
        let tracks = self.playlist_tracks(playlist_id).await?;
        Ok(distinct_covers(&tracks, wanted))
    }

    async fn track_radio(&self, track_id: &str) -> Result<Vec<Track>> {
        let results = self
            .gw(
                "song.getSearchTrackMix",
                json!({ "SNG_ID": track_id, "start": 0, "nb": 25 }),
            )
            .await;
        match results {
            Ok(results) => Ok(wire::track_list(&results)),
            Err(error) => {
                log::warn!("deezer: no track radio for {track_id}: {error:#}");
                Ok(Vec::new())
            }
        }
    }

    async fn search(&self, query: &str) -> Result<Vec<Track>> {
        let page = self
            .public(&format!("/search?q={}&limit=50", urlencoded(query)))
            .await
            .context("cannot search deezer")?;
        Ok(wire::track_list(&page))
    }

    async fn search_albums(&self, query: &str) -> Result<Vec<Album>> {
        let page = self
            .public(&format!("/search/album?q={}&limit=30", urlencoded(query)))
            .await
            .context("cannot search deezer albums")?;
        Ok(page
            .get("data")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .iter()
            .filter_map(wire::album)
            .collect())
    }

    async fn search_playlists(&self, query: &str) -> Result<Vec<Playlist>> {
        let page = self
            .public(&format!(
                "/search/playlist?q={}&limit=30",
                urlencoded(query)
            ))
            .await
            .context("cannot search deezer playlists")?;
        Ok(page
            .get("data")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .iter()
            .filter_map(|value| wire::playlist(value, ""))
            .collect())
    }

    async fn home(&self) -> Result<HomeFeed> {
        Ok(HomeFeed::default())
    }
}

/// Minimal percent-encoding for a search query, without growing the dependency tree.
fn urlencoded(query: &str) -> String {
    let mut encoded = String::with_capacity(query.len());
    for byte in query.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}
