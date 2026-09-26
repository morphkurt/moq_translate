//! Caption subscriber / demo consumer.
//!
//! Subscribes to `<video_id>/captions/<lang>` and prints each cue as it arrives.
//! Subscribing here is what raises demand on the translator and makes it start
//! producing that language.

use anyhow::Result;
use clap::Parser;
use moq_translate::{caption_path, client, translation_path, Cue, TRACK};
use url::Url;

#[derive(Parser)]
#[command(about = "Subscribe to a caption language over MoQ")]
struct Args {
    #[arg(long, default_value = moq_translate::DEFAULT_RELAY)]
    relay: Url,
    /// Symmetric JWK used to mint the relay's `?jwt=`. Empty string = anonymous relay.
    #[arg(long, default_value = moq_translate::DEFAULT_AUTH_KEY)]
    auth_key: String,
    #[arg(long, default_value = "live1")]
    video_id: String,
    /// Language code to consume, e.g. en, es, tr.
    #[arg(long, default_value = "es")]
    lang: String,
    /// Which namespace to read: `captions` (native source) or `translations` (on-demand).
    #[arg(long, default_value = "captions")]
    kind: String,
    /// For `--kind translations`, the source language to translate from (e.g. en, es).
    #[arg(long, default_value = "en")]
    from: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let relay = moq_translate::signed_relay(&args.relay, &args.auth_key)?;
    let client = client(&relay)?;

    let origin = moq_tokio::origin::spawn();
    let _conn = client
        .clone()
        .consume(origin.clone())
        .expect("relay url configured");

    let path = if args.kind == "translations" {
        translation_path(&args.video_id, &args.from, &args.lang)
    } else {
        caption_path(&args.video_id, &args.lang)
    };
    println!("subscribing to {path} ...");

    // Announcements take a moment to propagate over a fresh session, so retry until
    // the broadcast becomes routable rather than failing on the first miss.
    let broadcast = loop {
        match origin.consume().request_broadcast(&path).await {
            Ok(b) => break b,
            Err(err) => {
                eprintln!("not routable yet ({err}); retrying");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    };
    let track = broadcast.track(TRACK)?;
    let mut consumer = moq_json::stream::Consumer::<Cue>::new(
        track.subscribe(None).await?,
        moq_json::stream::Config::default(),
    );

    while let Some(cue) = consumer.next().await? {
        if cue.origin_ts_ms > 0 {
            let latency = moq_translate::now_ms().saturating_sub(cue.origin_ts_ms);
            let llm = if cue.translate_ms > 0 {
                format!(" llm {}ms", cue.translate_ms)
            } else {
                String::new()
            };
            println!("[{} #{}] (e2e +{latency}ms{llm}) {}", cue.lang, cue.seq, cue.text);
        } else {
            println!("[{} #{}] {}", cue.lang, cue.seq, cue.text);
        }
    }

    println!("stream ended");
    Ok(())
}
