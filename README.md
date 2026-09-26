# moq-translate — live caption translation over MoQ (PoC)

Consume live English captions from a MoQ relay, translate them into other languages
with a local LLM (Ollama), and re-publish each language as its own MoQ broadcast —
**only while a subscriber is actually watching that language**.

## Architecture

```
publish (en)  ──►  cdn.moq.dev/anon  ──►  translate  ──►  cdn.moq.dev/anon  ──►  subscribe (es/tr/…)
  demo/captions/en                         (Ollama: gemma3:4b)   demo/captions/es
                                                                 demo/captions/tr
```

- **Namespace:** `<video_id>/captions/<lang>`, each broadcast carries one JSON-stream
  track named `cues`. A cue is `{ seq, start_ms, end_ms, text, lang, origin_ts_ms }`.
- **Native sources vs translations (separate namespaces):** the publisher emits several
  *independent* native source streams under `captions/<lang>`, each with its own distinct content
  (en = tech news, es = cooking, fr = travel, de = weather) — not translations of one another.
  Translations live under `translations/<src>-<tgt>` so they never collide with native sources.
- **Pivot translation:** the translator subscribes to every native source and serves every
  source→target pair on demand. Any language can be a target, **including English** — e.g.
  `translations/es-en` is the Spanish cooking stream translated into English. In the web UI the
  left panel picks the source, the right picks the target.
- **End-to-end latency:** `origin_ts_ms` is stamped at production and preserved through
  translation, so consumers show `now − origin_ts_ms` per cue. Native cues land in tens of ms;
  translated cues include the LLM round-trip (~1 s).
- **Lazy translation:** the translator announces every target language up front, but a
  per-language task only calls the LLM and appends cues while `track::Demand` reports a
  live subscriber (`demand.is_used()`). No subscriber ⇒ no LLM work.
- **Routability primer:** the relay only advertises a route to a broadcast once it carries
  at least one record, so each language track is seeded with a single `[<Lang> ready]` cue.
  This is not an LLM call and is not pulled over the wire until someone subscribes.
- **LLM:** Ollama at `http://192.168.88.144:11434`, model `gemma3:4b`.

## Web demo

A browser-native MoQ viewer (Vite + `@moq/net` + `@moq/json`) that connects straight to the
relay over WebTransport. The **language dropdown sends the intent to receive** — selecting a
language subscribes to `<video_id>/captions/<lang>`, which raises demand and triggers
translation. English source and the chosen translation render side by side.

```bash
cd web
npm install
npm run dev      # http://localhost:5173 (or next free port)
```

Open in Chrome (WebTransport). With the translator + publisher running, pick a language and
watch translations stream in. Headless smoke test: `node web/check.mjs`.

## Run

Three terminals, in this order:

```bash
# 1. Publisher — distinct native captions for several languages
cargo run --bin publish -- --video-id live1 --langs en,es,fr,de

# 2. Translator — pivot: every source -> every target on demand
cargo run --bin translate -- --video-id live1 --sources en,es,fr,de --targets en,es,fr,de,tr,ja

# 3. A viewer — e.g. the Spanish source translated into English (es -> en)
cargo run --bin subscribe -- --video-id live1 --kind translations --from es --lang en
```

Stop the subscriber and the translator goes quiet for that language; start another
`--lang tr` and Turkish spins up on demand.

### Useful flags

- `--relay <url>` (default `https://cdn.moq.dev/anon`)
- `translate --ollama <url> --model <name>` (defaults: the guns host + `gemma3:4b`)
- `publish --interval-ms <n>` cue cadence

Set `RUST_LOG=info` for MoQ transport logs.
