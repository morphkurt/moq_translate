//! Pivot translation service.
//!
//! Subscribes to several native source streams (`<video_id>/captions/<src>`) and, for every
//! source→target pair, serves `<video_id>/translations/<src>-<tgt>`. Any language can be a
//! target, including English — e.g. read the Spanish cooking stream in English via
//! `translations/es-en`.
//!
//! Two levels of laziness keep the relay and GPU quiet:
//! - **Announce is lazy**: a pair's translation broadcast is only created, announced, and primed
//!   once its *source* actually starts publishing. Nothing is advertised for a source nobody is
//!   sending (so `pt-en` never appears unless something publishes `captions/pt`).
//! - **Translation is lazy**: even for an announced pair, the LLM only runs while that pair has a
//!   live subscriber (`demand.is_used()`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use moq_translate::{caption_path, client, lang_name, translation_path, Cue, Ollama, TRACK};
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

    // Publish side. Translation broadcasts are added to this origin LAZILY, from inside each source's
    // consume task, only once the source is live — so we never advertise pairs for an absent source.
    let pub_origin = moq_tokio::origin::spawn();
    let _conn_pub = client
        .clone()
        .publish(pub_origin.consume())
        .expect("relay url configured");

    // Consume side: subscribe to each native source and fan its cues into that source's channel.
    let sub_origin = moq_tokio::origin::spawn();
    let _conn_sub = client
        .consume(sub_origin.clone())
        .expect("relay url configured");

    for src in &args.sources {
        let path = caption_path(&args.video_id, src);
        let sender = senders[src].clone();
        let sub_origin = sub_origin.clone();
        let pub_origin = pub_origin.clone();
        let job_tx = job_tx.clone();
        let targets = args.targets.clone();
        let video_id = args.video_id.clone();
        let src = src.clone();
        tokio::spawn(async move {
            let mut announced = false;
            // Outer loop makes the subscription self-healing: a browser publisher that reloads,
            // seeks, or reconnects aborts the single MoQ group it streams into, which surfaces here
            // as a read/subscribe error. Rather than ending the task (which permanently halts every
            // translation for this source), re-request the broadcast and resume.
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

                // The source is routable now → lazily announce this source's translation pairs, once.
                // A source that never appears advertises nothing.
                if !announced {
                    announced = true;
                    let src_name = lang_name(&src);
                    for tgt in &targets {
                        if tgt == &src {
                            continue;
                        }
                        let tpath = translation_path(&video_id, &src, tgt);
                        let broadcast = match pub_origin.create_broadcast(&tpath) {
                            Ok(b) => b,
                            Err(err) => {
                                eprintln!("pair {src}->{tgt} create error: {err:#}");
                                continue;
                            }
                        };
                        if let Err(err) = broadcast.announce(Default::default()) {
                            eprintln!("pair {src}->{tgt} announce error: {err:#}");
                            continue;
                        }
                        let track = match broadcast.create_track(TRACK, None) {
                            Ok(t) => t,
                            Err(err) => {
                                eprintln!("pair {src}->{tgt} track error: {err:#}");
                                continue;
                            }
                        };
                        let producer =
                            moq_json::stream::Producer::new(track, moq_json::stream::Config::default());
                        let demand = producer.demand();

                        let mut rx = sender.subscribe();
                        let jobs = job_tx.clone();
                        let src = src.clone();
                        let src_name = src_name.clone();
                        let tgt = tgt.clone();
                        let tgt_name = lang_name(&tgt);
                        println!("announced {tpath}");

                        tokio::spawn(async move {
                            let _broadcast = broadcast;
                            let mut producer = producer;
                            // Prime the broadcast so the relay advertises a route (see README).
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
                                    continue; // lazy: nobody watching this pair
                                }
                                // Hand the work to the singleton GPU worker and wait our turn.
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
                                    Err(_) => continue, // worker dropped the reply
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
                }

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
                        // Stream ended or errored: fall out to the outer loop and re-subscribe so a
                        // republished source (new group) is picked up instead of stalling here.
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
