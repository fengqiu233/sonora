//! Conversions from Deezer's wire shapes to the crate's models. The gateway answers in
//! SCREAMING_SNAKE and the public REST api in snake_case, and neither is a stable contract,
//! so every read goes through tolerant helpers rather than derived structs.

use std::time::Duration;

use serde_json::Value;

use crate::{Album, ArtistRef, Playlist, ReleaseType, SavedArtist, Track};

/// A track id is a numeric string. Zero and negative ids are user uploads, which the
/// streaming endpoints treat differently; the sign stays in the string.
pub fn id(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_i64().map(|number| number.to_string()))
        .filter(|id| !id.is_empty() && id != "0")
}

/// The first non-empty string under any of `keys`.
pub fn text<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|key| value.get(key))?.as_str()
}

/// The first number under any of `keys`. A key the response omits is skipped rather than
/// ending the search, because the two apis spell the same field differently and only one of
/// the spellings is ever present.
pub fn number(value: &Value, keys: &[&str]) -> Option<u64> {
    for key in keys {
        let Some(field) = value.get(key) else {
            continue;
        };
        if let Some(number) = field.as_u64() {
            return Some(number);
        }
        if let Some(text) = field.as_str()
            && let Ok(number) = text.parse()
        {
            return Some(number);
        }
    }
    None
}

/// The `https://cdn-images.dzcdn.net/images/<kind>/<md5>/<size>x<size>-000000-80-0-0.jpg`
/// cover url Deezer builds from a picture hash.
pub fn image(kind: &str, md5: Option<&str>, size: u32) -> Option<String> {
    let md5 = md5?.trim();
    if md5.is_empty() {
        return None;
    }
    Some(format!(
        "https://cdn-images.dzcdn.net/images/{kind}/{md5}/{size}x{size}-000000-80-0-0.jpg"
    ))
}

fn cover(value: &Value, size: u32) -> Option<String> {
    let md5 = text(value, &["md5_image", "ALB_PICTURE", "picture"]);
    image("cover", md5, size)
        .or_else(|| text(value, &["cover_big", "picture_big"]).map(str::to_owned))
}

fn artist_picture(value: &Value, size: u32) -> Option<String> {
    let md5 = text(value, &["picture", "ART_PICTURE"]);
    image("artist", md5, size).or_else(|| text(value, &["picture_big"]).map(str::to_owned))
}

fn artists_of(value: &Value) -> (String, Vec<ArtistRef>) {
    // a public-api track carries one `artist` object; a gateway track carries ART_NAME plus
    // optionally a SNG_CONTRIBUTORS.mainartist list
    if let Some(list) = value
        .get("SNG_CONTRIBUTORS")
        .and_then(|contributors| contributors.get("mainartist"))
        .and_then(Value::as_array)
        && !list.is_empty()
    {
        let names: Vec<String> = list
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect();
        let single = value
            .get("ART_ID")
            .and_then(id)
            .map(|artist_id| (names.clone(), artist_id));
        let refs = match (single, names.len()) {
            (Some((names, artist_id)), 1) => vec![ArtistRef {
                name: names[0].clone(),
                id: Some(artist_id),
            }],
            _ => names
                .iter()
                .map(|name| ArtistRef {
                    name: name.clone(),
                    id: None,
                })
                .collect(),
        };
        return (names.join(", "), refs);
    }
    if let Some(artist) = value.get("artist")
        && let Some(name) = artist.get("name").and_then(Value::as_str)
    {
        return (
            name.to_owned(),
            vec![ArtistRef {
                name: name.to_owned(),
                id: artist.get("id").and_then(id),
            }],
        );
    }
    let name = text(value, &["ART_NAME", "artist_name"]).unwrap_or_default();
    let refs = match name.is_empty() {
        true => Vec::new(),
        false => vec![ArtistRef {
            name: name.to_owned(),
            id: value.get("ART_ID").and_then(id),
        }],
    };
    (name.to_owned(), refs)
}

fn truthy(value: &Value, keys: &[&str]) -> bool {
    keys.iter()
        .find_map(|key| value.get(key))
        .map(|field| match field {
            Value::Bool(flag) => *flag,
            Value::Number(number) => number.as_i64() != Some(0),
            Value::String(text) => matches!(text.as_str(), "1" | "true"),
            _ => false,
        })
        .unwrap_or(false)
}

/// One track from either api. Anything the response omits falls back to a neutral default;
/// `duration` is the one field every listing answers.
pub fn track(value: &Value) -> Option<Track> {
    let track_id = value
        .get("SNG_ID")
        .or_else(|| value.get("id"))
        .and_then(id)?;
    let name = text(value, &["SNG_TITLE", "title", "TITLE"])
        .unwrap_or_default()
        .to_owned();
    let (artists, artist_refs) = artists_of(value);
    let album = value.get("album").cloned().unwrap_or(Value::Null);
    Some(Track {
        id: Some(track_id),
        name,
        playable: truthy(value, &["readable"])
            || !value
                .get("readable")
                .map(|flag| flag.is_boolean())
                .unwrap_or(false),
        artists,
        artist_refs,
        album: text(&album, &["title"])
            .or_else(|| text(value, &["ALB_TITLE"]))
            .unwrap_or_default()
            .to_owned(),
        album_id: album
            .get("id")
            .and_then(id)
            .or_else(|| value.get("ALB_ID").and_then(id)),
        cover: cover(&album, 300).or_else(|| cover(value, 300)),
        duration: Duration::from_secs(number(value, &["DURATION", "duration"]).unwrap_or(0)),
        added_at: number(value, &["ADDED_AT", "time_add"]).map(|at| at as i64),
        added_by: None,
        playcount: None,
        popularity: number(value, &["RANK", "rank"])
            .map(|rank| (rank / 10_000).min(100) as u32)
            .unwrap_or(0),
        explicit: truthy(value, &["explicit_lyrics"])
            || text(value, &["EXPLICIT_LYRICS"]) == Some("1"),
        track_number: number(value, &["TRACK_NUMBER", "track_position"]).unwrap_or(0) as u32,
        disc_number: number(value, &["DISK_NUMBER"]).unwrap_or(1) as u32,
        tags: Vec::new(),
        languages: Vec::new(),
        credits: Vec::new(),
    })
}

pub fn track_list(value: &Value) -> Vec<Track> {
    value
        .get("data")
        .and_then(Value::as_array)
        .map(|list| list.iter().filter_map(track).collect())
        .unwrap_or_default()
}

/// One album from either api.
pub fn album(value: &Value) -> Option<Album> {
    let album_id = value
        .get("ALB_ID")
        .or_else(|| value.get("id"))
        .and_then(id)?;
    let (artists, artist_refs) = artists_of(value);
    let release = text(
        value,
        &[
            "release_date",
            "PHYSICAL_RELEASE_DATE",
            "DIGITAL_RELEASE_DATE",
        ],
    )
    .unwrap_or_default();
    let year = release
        .split('-')
        .next()
        .and_then(|year| year.parse().ok())
        .unwrap_or(0);
    Some(Album {
        id: album_id,
        name: text(value, &["ALB_TITLE", "title"])
            .unwrap_or_default()
            .to_owned(),
        artists,
        artist_refs,
        cover: cover(value, 300),
        cover_large: cover(value, 1000),
        release_type: match text(value, &["record_type"]) {
            Some("single") => ReleaseType::Single,
            Some("ep") => ReleaseType::Ep,
            Some("compile") => ReleaseType::Compilation,
            _ => ReleaseType::Album,
        },
        year,
        track_count: number(value, &["NB_TRAK", "nb_tracks"]).unwrap_or(0) as u32,
        release_date: release.to_owned(),
        label: text(value, &["label", "LABEL"])
            .unwrap_or_default()
            .to_owned(),
        copyrights: Vec::new(),
        added_at: number(value, &["ADDED_AT", "time_add"]).map(|at| at as i64),
    })
}

/// One playlist, from `deezer.pageProfile`, `deezer.pagePlaylist` or the public api.
pub fn playlist(value: &Value, user_id: &str) -> Option<Playlist> {
    let playlist_id = value
        .get("PLAYLIST_ID")
        .or_else(|| value.get("id"))
        .and_then(id)?;
    let owner = text(value, &["PARENT_USERNAME", "CREATOR_NAME"])
        .or_else(|| {
            value
                .get("creator")
                .and_then(|creator| creator.get("name"))
                .and_then(Value::as_str)
        })
        .unwrap_or_default();
    let owner_id = text(value, &["PARENT_USER_ID"])
        .map(str::to_owned)
        .or_else(|| {
            value
                .get("creator")
                .and_then(|creator| creator.get("id"))
                .and_then(id)
        })
        .unwrap_or_default();
    let md5 = text(value, &["PLAYLIST_PICTURE", "picture"]);
    Some(Playlist {
        id: playlist_id,
        name: text(value, &["TITLE", "title"])
            .unwrap_or_default()
            .to_owned(),
        owner: owner.to_owned(),
        owner_id: owner_id.clone(),
        owned: !owner_id.is_empty() && owner_id == user_id,
        collaborative: false,
        blend: false,
        public: number(value, &["STATUS"])
            .map(|status| status == 1)
            .unwrap_or_else(|| truthy(value, &["public"])),
        cover: image("playlist", md5, 300)
            .or_else(|| text(value, &["picture_medium"]).map(str::to_owned)),
        track_count: number(value, &["NB_SONG", "nb_tracks"]).unwrap_or(0) as u32,
        modified_at: number(value, &["DATE_MOD"]).map(|at| at as i64),
    })
}

/// One saved (favorite) artist.
pub fn saved_artist(value: &Value) -> Option<SavedArtist> {
    let artist_id = value
        .get("ART_ID")
        .or_else(|| value.get("id"))
        .and_then(id)?;
    Some(SavedArtist {
        id: artist_id,
        name: text(value, &["ART_NAME", "name"])
            .unwrap_or_default()
            .to_owned(),
        cover: artist_picture(value, 300),
        added_at: number(value, &["ADDED_AT", "time_add"]).map(|at| at as i64),
    })
}
