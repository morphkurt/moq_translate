//! Synthetic multi-language caption publisher.
//!
//! Publishes several independent source streams, each with its OWN distinct native content
//! (not a translation of one another): English tech news, Spanish cooking, French travel,
//! German weather. Each language is announced at `<video_id>/captions/<lang>` and appends
//! cues to its `cues` JSON-stream track on a timer.

use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use moq_translate::{caption_path, client, now_ms, Cue, TRACK};
use url::Url;

#[derive(Parser)]
#[command(about = "Publish distinct native captions per language over MoQ")]
struct Args {
    #[arg(long, default_value = moq_translate::DEFAULT_RELAY)]
    relay: Url,
    /// Symmetric JWK used to mint the relay's `?jwt=`. Empty string = anonymous relay.
    #[arg(long, default_value = moq_translate::DEFAULT_AUTH_KEY)]
    auth_key: String,
    #[arg(long, default_value = "live1")]
    video_id: String,
    /// Comma-separated source languages to publish natively.
    #[arg(long, default_value = "en,es,fr,de", value_delimiter = ',')]
    langs: Vec<String>,
    /// Milliseconds between cues.
    #[arg(long, default_value_t = 3000)]
    interval_ms: u64,
}

/// A distinct native script per language — different subject matter each, so the streams are
/// genuinely independent sources rather than the same sentence in different languages.
fn script(lang: &str) -> Vec<&'static str> {
    match lang {
        "en" => vec![
            "Good evening. Here are tonight's top technology headlines.",
            "Engineers demonstrated a new low-latency streaming protocol today.",
            "The open standard runs directly over QUIC for faster delivery.",
            "Analysts expect wider adoption across live platforms this year.",
            "That's all from the tech desk. Back to you in the studio.",
        ],
        "es" => vec![
            "Buenas noches. Bienvenidos al programa de cocina de hoy.",
            "Hoy preparamos una paella de mariscos bien tradicional.",
            "Primero sofreímos el ajo y el pimiento en aceite de oliva.",
            "Añadimos el arroz, el azafrán y un buen caldo de pescado.",
            "Servimos bien caliente y acompañamos con un poco de limón.",
        ],
        "fr" => vec![
            "Bonjour et bienvenue dans votre magazine de voyage.",
            "Aujourd'hui, nous partons à la découverte de la Provence.",
            "Le train à grande vitesse relie Paris à Marseille en trois heures.",
            "Ne manquez pas les marchés colorés et les champs de lavande.",
            "Bon voyage, et rendez-vous la semaine prochaine.",
        ],
        "de" => vec![
            "Guten Tag. Hier ist der Wetterbericht für heute.",
            "Im Norden ziehen dichte Wolken mit leichtem Regen auf.",
            "Der Süden bleibt sonnig bei angenehmen zwanzig Grad.",
            "Am Abend frischt der Wind an der Küste spürbar auf.",
            "Morgen wird es verbreitet freundlich und trocken.",
        ],
        _ => vec![
            "This language has no authored script; emitting placeholder lines.",
            "Configure a native script for it in publish.rs.",
        ],
    }
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

    for lang in &args.langs {
        let path = caption_path(&args.video_id, lang);
        let broadcast = origin.create_broadcast(&path)?;
        broadcast.announce(Default::default())?;
        let track = broadcast.create_track(TRACK, None)?;
        let producer = moq_json::stream::Producer::new(track, moq_json::stream::Config::default());

        let lang = lang.clone();
        let lines = script(&lang);
        let interval_ms = args.interval_ms;
        println!("publishing native {lang} captions on {path}");

        tokio::spawn(async move {
            let _broadcast = broadcast;
            let mut producer = producer;
            let mut seq = 0u64;
            let mut clock_ms = 0u64;
            let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
            loop {
                interval.tick().await;
                let text = lines[(seq as usize) % lines.len()].to_string();
                let cue = Cue {
                    seq,
                    start_ms: clock_ms,
                    end_ms: clock_ms + interval_ms,
                    text,
                    lang: lang.clone(),
                    origin_ts_ms: now_ms(),
                    translate_ms: 0,
                };
                println!("[{lang} #{seq}] {}", cue.text);
                if let Err(err) = producer.append(&cue) {
                    eprintln!("[{lang}] append error: {err:#}");
                    break;
                }
                seq += 1;
                clock_ms += interval_ms;
            }
        });
    }

    let _conn = client
        .publish(origin.consume())
        .expect("relay url configured");

    // Keep the process alive; the per-language tasks do the publishing.
    std::future::pending::<()>().await;
    Ok(())
}
