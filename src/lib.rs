//! Shared types and helpers for the MoQ live-caption translation PoC.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use url::Url;

/// The track name every caption broadcast carries.
pub const TRACK: &str = "cues";

/// One caption cue: a line of text with its presentation window.
///
/// The same shape flows through every language broadcast; only `text` and `lang`
/// differ between the English source and a translated stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cue {
    pub seq: u64,
    pub start_ms: u64,
    pub end_ms: u64,
    pub text: String,
    #[serde(default)]
    pub lang: String,
    /// Wall-clock time (Unix epoch millis) when the *source* text was produced. Set by the
    /// publisher and preserved unchanged through translation, so a consumer can compute the
    /// end-to-end latency from production to display as `now - origin_ts_ms`.
    #[serde(default)]
    pub origin_ts_ms: u64,
    /// Milliseconds spent inside the LLM translation call (0 for native/untranslated cues).
    /// Lets a consumer separate the model cost from total end-to-end latency.
    #[serde(default)]
    pub translate_ms: u64,
}

/// Current Unix time in milliseconds.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Broadcast path for a native source caption stream, e.g. `live1/captions/es`.
pub fn caption_path(video_id: &str, lang: &str) -> String {
    format!("{video_id}/captions/{lang}")
}

/// Broadcast path for a translated stream, kept in a separate namespace from native sources and
/// keyed by BOTH the source and target language so any source→target pair is distinct, e.g.
/// `live1/translations/es-en` (Spanish source translated into English).
pub fn translation_path(video_id: &str, source: &str, target: &str) -> String {
    format!("{video_id}/translations/{source}-{target}")
}

/// Default relay + account. `cdn.moq.pro` gates every session behind an HS256 JWT (unlike the old
/// anonymous `cdn.moq.dev/anon`), so a token must be minted from the account key and carried in
/// the connect URL's `?jwt=` query — see [`signed_relay`].
pub const DEFAULT_RELAY: &str = "https://cdn.moq.pro/pk54jf2kv65wn";

/// Default path to the account's symmetric signing key (relative to the project root).
pub const DEFAULT_AUTH_KEY: &str = "keys/pk54jf2kv65wn-default.jwk";

/// Return `relay` with a freshly minted `?jwt=` for the account it dials.
///
/// The token's root is taken from the relay path (`/pk54jf2kv65wn` → root `pk54jf2kv65wn`) and it
/// grants publish + subscribe on everything beneath it (`**`), which is all this PoC needs. Pass an
/// empty `key_path` to skip signing entirely (for an anonymous relay such as `cdn.moq.dev/anon`).
pub fn signed_relay(relay: &Url, key_path: &str) -> Result<Url> {
    if key_path.is_empty() {
        return Ok(relay.clone());
    }
    let key = moq_auth::Key::from_file(key_path)
        .with_context(|| format!("loading signing key {key_path}"))?;
    let root = relay.path().trim_matches('/').to_string();
    let claims = moq_auth::Claims::default()
        .with_root(root)
        .with_publish(["**".parse::<moq_pattern::Pattern>()?])
        .with_subscribe(["**".parse::<moq_pattern::Pattern>()?])
        .with_issued(std::time::SystemTime::now())
        .with_expires(std::time::SystemTime::now() + std::time::Duration::from_secs(7 * 24 * 3600));
    let token = key.sign(&claims).context("signing relay token")?;
    let mut url = relay.clone();
    url.query_pairs_mut().clear().append_pair("jwt", &token);
    Ok(url)
}

/// Build a MoQ client dialing `relay` (e.g. `https://cdn.moq.dev/anon`).
pub fn client(relay: &Url) -> Result<moq_tokio::Client> {
    let mut config = moq_tokio::connect::Config::default();
    config.url = Some(relay.clone());
    config
        .init(moq_tokio::quic::Config::default())
        .context("building moq client")
}

/// Minimal Ollama chat client for translation.
pub struct Ollama {
    http: reqwest::Client,
    base: Url,
    model: String,
}

impl Ollama {
    pub fn new(base: Url, model: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            base,
            model,
        }
    }

    /// Translate one line from `source_lang` into `target_lang` (full language names like
    /// "Spanish" give the model better grounding than ISO codes).
    pub async fn translate(&self, text: &str, source_lang: &str, target_lang: &str) -> Result<String> {
        let url = self.base.join("/api/generate")?;
        let system = format!(
            "You are a professional subtitle translator. Translate the user's {source_lang} \
             subtitle line into {target_lang}. Translate faithfully and literally: never add, \
             omit, invent, continue, or explain anything beyond the given line. Keep proper nouns, \
             names, and numbers, but you MUST translate everything else into {target_lang} — never \
             copy the {source_lang} words back. Write only in the {target_lang} script; do not mix \
             in any other language or script. If a word has no {target_lang} equivalent, \
             transliterate it into {target_lang} script. Output ONLY the {target_lang} translation \
             as a single line — no quotes, no notes, no romanization, no source text."
        );
        let body = serde_json::json!({
            "model": self.model,
            "system": system,
            "prompt": text,
            "stream": false,
            // Keep the model pinned in VRAM between cues so we never pay the multi-second cold
            // reload. Options are kept constant across every call — changing them (num_ctx in
            // particular) makes Ollama spin up a fresh runner and reload the weights.
            "keep_alive": "1h",
            "options": { "temperature": 0, "num_predict": 100 }
        });
        let resp: OllamaGenerate = self
            .http
            .post(url)
            .json(&body)
            .send()
            .await
            .context("ollama request")?
            .error_for_status()
            .context("ollama status")?
            .json()
            .await
            .context("ollama decode")?;
        Ok(resp.response.trim().to_string())
    }
}

#[derive(Deserialize)]
struct OllamaGenerate {
    response: String,
}

/// Human-readable language name for common ISO codes, used in the LLM prompt.
pub fn lang_name(code: &str) -> String {
    match code {
        "es" => "Spanish",
        "tr" => "Turkish",
        "fr" => "French",
        "de" => "German",
        "ja" => "Japanese",
        "zh" => "Chinese (Simplified)",
        "hi" => "Hindi",
        "ar" => "Arabic",
        "pt" => "Portuguese",
        "si" => "Sinhala",
        "ru" => "Russian",
        other => other,
    }
    .to_string()
}
