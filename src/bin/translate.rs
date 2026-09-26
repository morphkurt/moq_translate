//! Pivot translation service.
//!
//! Subscribes to several native source streams (`<video_id>/captions/<src>`) and serves
//! `<video_id>/translations/<src>-<tgt>` on demand. Any language can be a target, including English
//! — e.g. read the Spanish cooking stream in English via `translations/es-en`.
//!
//! Nothing speculative is advertised. Instead of announcing every source×target pair up front, the
//! publish side advertises a single dynamic route for the `<video_id>/translations` subtree and
//! **materializes a pair only when a consumer actually subscribes to it** — so a pair we are not
//! translating (e.g. `pt-fr` that nobody is watching) never appears in the namespace at all. On top
//! of that, even a materialized pair only runs the LLM while it has a live subscriber
//! (`demand.is_used()`), and the GPU is driven by a single serialized worker.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use moq_translate::{caption_path, client, lang_name, Cue, Ollama, TRACK};
use tokio::sync::{broadcast, mpsc, oneshot};
use url::Url;

/// A translation request handed to the singleton GPU worker.
struct Job {
    text: String,
    source: String,
    target: String,
    /// Replies with `(translated_text, inference_ms)` or an error string.
    reply: oneshot::Sender<Result<(String, u64), String>>,
}

#[derive(Parser)]
#[command(about = "Pivot-translate native caption streams into other languages over MoQ")]
struct Args {
    #[arg(long, default_value = moq_translate::DEFAULT_RELAY)]
    relay: Url,
    /// Symmetric JWK used to mint the relay's `?jwt=`. Empty string = anonymous relay.
    #[arg(long, default_value = moq_translate::DEFAULT_AUTH_KEY)]
    auth_key: String,
    #[arg(long, default_value = "live1")]
    video_id: String,
    /// Native source languages to translate FROM (each must be published natively).
    #[arg(long, default_value = "en,es,fr,de,pt,si", value_delimiter = ',')]
    sources: Vec<String>,
    /// Target languages to translate INTO (a target equal to the source is skipped).
    #[arg(long, default_value = "en,es,fr,de,tr,ja,pt,si", value_delimiter = ',')]
    targets: Vec<String>,
    #[arg(long, default_value = "http://192.168.88.144:11434")]
    ollama: Url,
    #[arg(long, default_value = "gemma3:4b")]
    model: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let relay = moq_translate::signed_relay(&args.relay, &args.auth_key)?;
    let client = client(&relay)?;
    let ollama = Arc::new(Ollama::new(args.ollama.clone(), args.model.clone()));

    // Singleton GPU translator: a single worker drains this queue and runs ONE inference at a
    // time, so the local GPU is never hit by concurrent requests no matter how many source→target
    // pairs are active. Queue-wait shows up in end-to-end latency; `translate_ms` stays pure model time.
    let (job_tx, mut job_rx) = mpsc::channel::<Job>(1024);
    {
        let ollama = ollama.clone();
        tokio::spawn(async move {
            while let Some(job) = job_rx.recv().await {
                let t0 = std::time::Instant::now();
                let res = ollama.translate(&job.text, &job.source, &job.target).await;
                let dt = t0.elapsed().as_millis() as u64;
                let _ = job.reply.send(res.map(|s| (s, dt)).map_err(|e| format!("{e:#}")));
            }
        });
    }

    // One fan-out channel per source, carrying that source's native cues to its translation tasks.
    let mut senders: HashMap<String, broadcast::Sender<Cue>> = HashMap::new();
    for src in &args.sources {
        senders.insert(src.clone(), broadcast::channel::<Cue>(256).0);
    }

    // Publish side: advertise the translations subtree as ONE dynamic route and serve each pair on
    // demand. Nothing under it exists until a consumer subscribes to a specific `<src>-<tgt>` path.
    let pub_origin = moq_tokio::origin::spawn();
    let _conn_pub = client
        .clone()
        .publish(pub_origin.consume())
        .expect("relay url configured");

    {
        let prefix = format!("{}/translations", args.video_id);
        let dynamic = pub_origin.dynamic(&prefix, Default::default())?;
        let senders = senders.clone();
        let job_tx = job_tx.clone();
        let sources = args.sources.clone();
        let targets = args.targets.clone();
        tokio::spawn(async move {
            loop {
                let req = match dynamic.requested_broadcast().await {
                    Ok(req) => req,
                    Err(_) => break, // origin driver gone
                };
                let path = req.path().to_string();
                // Path is `<video_id>/translations/<src>-<tgt>`; pull the `<src>-<tgt>` off the end.
                let pair = path.rsplit('/').next().unwrap_or("");
                let (src, tgt) = match pair.split_once('-') {
                    Some((s, t)) => (s.to_string(), t.to_string()),
                    None => continue, // malformed → drop the request (auto-rejects)
                };
                // Only serve pairs we actually support; anything else is dropped (auto-rejected).
                if src == tgt || !sources.contains(&src) || !targets.contains(&tgt) {
                    continue;
                }
                let sender = match senders.get(&src) {
                    Some(s) => s,
                    None => continue,
                };

                let broadcast = moq_net::broadcast::Info::new().produce();
                let track = match broadcast.create_track(TRACK, None) {
                    Ok(t) => t,
                    Err(err) => {
                        eprintln!("serve {path} track error: {err:#}");
                        continue;
                    }
                };
                let mut producer =
                    moq_json::stream::Producer::new(track, moq_json::stream::Config::default());
                let demand = producer.demand();
                req.accept(&broadcast); // hand the requester a consumer; we keep producing into it
                println!("serving {path}");

                let mut rx = sender.subscribe();
                let jobs = job_tx.clone();
                let src_name = lang_name(&src);
                let tgt_name = lang_name(&tgt);
                tokio::spawn(async move {
                    let _broadcast = broadcast; // keep the served broadcast alive
                    // Prime so the relay advertises a route and the viewer sees immediate feedback.
                    let _ = producer.append(&Cue {
                        seq: 0,
                        start_ms: 0,
                        end_ms: 0,
                        text: format!("[{src_name}→{tgt_name} ready]"),
                        lang: tgt.clone(),
                        origin_ts_ms: 0,
                        translate_ms: 0,
                    });
                    loop {
                        let cue = match rx.recv().await {
                            Ok(cue) => cue,
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => break,
                        };
                        if !demand.is_used() {
                            continue; // lazy: subscriber went away
                        }
                        let (reply_tx, reply_rx) = oneshot::channel();
                        let job = Job {
                            text: cue.text.clone(),
                            source: src_name.clone(),
                            target: tgt_name.clone(),
                            reply: reply_tx,
                        };
                        if jobs.send(job).await.is_err() {
                            break; // worker gone
                        }
                        let (translated, translate_ms) = match reply_rx.await {
                            Ok(Ok(v)) => v,
                            Ok(Err(err)) => {
                                eprintln!("[{src}->{tgt}] translate error: {err}");
                                continue;
                            }
                            Err(_) => continue,
                        };
                        let out = Cue {
                            seq: cue.seq,
                            start_ms: cue.start_ms,
                            end_ms: cue.end_ms,
                            text: translated,
                            lang: tgt.clone(),
                            origin_ts_ms: cue.origin_ts_ms,
                            translate_ms,
                        };
                        println!("[{src}->{tgt} #{} {}ms] {}", out.seq, translate_ms, out.text);
                        if let Err(err) = producer.append(&out) {
                            eprintln!("[{src}->{tgt}] append error: {err:#}");
                            break;
                        }
                    }
                });
            }
        });
    }

    // Consume side: subscribe to each native source and fan its cues into that source's channel.
    let sub_origin = moq_tokio::origin::spawn();
    let _conn_sub = client
        .consume(sub_origin.clone())
        .expect("relay url configured");

    for src in &args.sources {
        let path = caption_path(&args.video_id, src);
        let sender = senders[src].clone();
        let sub_origin = sub_origin.clone();
        let src = src.clone();
        tokio::spawn(async move {
            // Self-healing: a browser publisher that reloads/seeks/reconnects aborts the group it
            // streams into, surfacing here as a read/subscribe error. Re-request and resume rather
            // than ending the task (which would stall every translation from this source).
            loop {
                let broadcast = loop {
                    match sub_origin.consume().request_broadcast(&path).await {
                        Ok(b) => break b,
                        Err(err) => {
                            eprintln!("source {src} not ready ({err}); retrying");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                };
                let track = match broadcast.track(TRACK) {
                    Ok(t) => t,
                    Err(err) => {
                        eprintln!("source {src} track error: {err:#}; retrying");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                let sub = match track.subscribe(None).await {
                    Ok(s) => s,
                    Err(err) => {
                        eprintln!("source {src} subscribe error: {err:#}; retrying");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                let mut consumer =
                    moq_json::stream::Consumer::<Cue>::new(sub, moq_json::stream::Config::default());
                println!("subscribed to source {path}");
                loop {
                    match consumer.next().await {
                        Ok(Some(cue)) => {
                            let _ = sender.send(cue);
                        }
                        Ok(None) => {
                            eprintln!("source {src} stream ended; re-subscribing");
                            break;
                        }
                        Err(err) => {
                            eprintln!("source {src} read error: {err:#}; re-subscribing");
                            break;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    // Keep the process alive; the spawned tasks do the work.
    std::future::pending::<()>().await;
    Ok(())
}
