import * as Moq from "@moq/net";
import { Stream } from "@moq/json";
import { createFile, type Movie, type Sample, type MP4BoxBuffer } from "mp4box";
import { defaultRelay, resolveRelay, getStoredJwk, setStoredJwk, setStoredRelay } from "./relay";

/** One caption cue, wire-identical to the Rust `Cue` struct. */
interface Cue {
  seq: number;
  start_ms: number;
  end_ms: number;
  text: string;
  lang: string;
  origin_ts_ms?: number;
  translate_ms?: number;
}

/** A parsed WebVTT cue: just timing + text, before it becomes a wire `Cue`. */
interface VttCue {
  start_ms: number;
  end_ms: number;
  text: string;
}

const LANG_NAMES: Record<string, string> = {
  en: "English",
  es: "Spanish",
  tr: "Turkish",
  fr: "French",
  de: "German",
  ja: "Japanese",
  hi: "Hindi",
  ar: "Arabic",
  pt: "Portuguese",
  si: "Sinhala",
};

// A caption language can be any language the translator subscribes to (`--sources`); keep this in
// sync with the translator's default. Any language can be a translation target.
const SOURCES = ["en", "es", "fr", "de", "pt", "si"];
const TARGETS = ["en", "es", "fr", "de", "tr", "ja", "pt", "si"];

// MP4 tracks carry ISO-639-2/T (3-letter) language codes; map the common ones to our 2-letter codes.
const ISO3_TO_2: Record<string, string> = {
  eng: "en", spa: "es", fra: "fr", fre: "fr", deu: "de", ger: "de", por: "pt",
  sin: "si", tur: "tr", jpn: "ja", hin: "hi", ara: "ar", rus: "ru", zho: "zh", chi: "zh",
};
const iso2 = (lang: string) => ISO3_TO_2[lang?.toLowerCase()] ?? lang;

/** A subtitle track discovered inside a loaded MP4. */
interface EmbeddedTrack {
  id: number;
  codec: string;
  lang: string; // mapped 2-letter code (or the raw code if unknown)
  raw: string; // original ISO-639-2 code from the container
}

const $ = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
const relayInput = $<HTMLInputElement>("relay");
const jwkInput = $<HTMLInputElement>("jwk");
relayInput.value = defaultRelay();
jwkInput.value = getStoredJwk();
relayInput.addEventListener("change", () => setStoredRelay(relayInput.value));
jwkInput.addEventListener("change", () => setStoredJwk(jwkInput.value));
const videoIdInput = $<HTMLInputElement>("video-id");
const videoUrlInput = $<HTMLInputElement>("video-url");
const videoFileInput = $<HTMLInputElement>("video-file");
const vttFileInput = $<HTMLInputElement>("vtt-file");
const embeddedField = $<HTMLDivElement>("embedded-field");
const embeddedSel = $<HTMLSelectElement>("embedded-track");
const srcSel = $<HTMLSelectElement>("src-lang");
const tgtSel = $<HTMLSelectElement>("tgt-lang");
const connectBtn = $<HTMLButtonElement>("connect");
const statusEl = $<HTMLDivElement>("status");
const runNote = $<HTMLDivElement>("run-note");

const video = $<HTMLVideoElement>("video");
const srcPath = $<HTMLSpanElement>("src-path");
const tgtPath = $<HTMLSpanElement>("tgt-path");
const tgtLat = $<HTMLSpanElement>("tgt-lat");
const srcFeed = $<HTMLDivElement>("src-feed");
const clearLeftBtn = $<HTMLButtonElement>("clear-left");
const clearRightBtn = $<HTMLButtonElement>("clear-right");
const fullscreenBtn = $<HTMLButtonElement>("fullscreen");
const stage = $<HTMLDivElement>("stage");
const tgtFeed = $<HTMLDivElement>("tgt-feed");
const ovSrc = $<HTMLDivElement>("ov-src");
const ovTrans = $<HTMLDivElement>("ov-trans");

// ---- state -------------------------------------------------------------------

let origin: Moq.Origin.Producer | undefined;
let connection: Awaited<ReturnType<typeof Moq.Connection.connect>> | undefined;

let srcBroadcast: Moq.Broadcast.Producer | undefined;
let srcProducer: Stream.Producer<Cue> | undefined;

let cues: VttCue[] = [];
let lastPublishedIdx = -1; // index of the cue currently published, to avoid re-sending it every tick
let videoFile: File | undefined; // the loaded MP4, kept so we can demux embedded subtitle tracks
let embeddedTracks: EmbeddedTrack[] = [];

// Playhead time (ms) at which to hide the translated overlay, or -1 when nothing is shown. Set from
// each translation's original `end_ms` plus the latency it took to arrive, so the translated caption
// clears during silent gaps just like the original — only shifted later by the round-trip + LLM time.
let transHideAt = -1;
const MIN_TRANS_SHOW_MS = 700; // floor so a short or late-arriving line is still readable

let tgtGen = 0; // bumps to cancel a stale translation pump
let tgtSub: Moq.Track.Subscriber | undefined;

const videoId = () => videoIdInput.value.trim() || "vtt1";

function setStatus(text: string, connected: boolean) {
  statusEl.classList.toggle("connected", connected);
  (statusEl.querySelector(".txt") as HTMLElement).textContent = text;
}

function updateRunNote() {
  runNote.textContent =
    `translator must be running for this video id, e.g.:  ` +
    `./target/debug/translate --video-id ${videoId()}`;
}

function escapeHtml(s: string) {
  const d = document.createElement("div");
  d.textContent = s;
  return d.innerHTML;
}

function showHint(feed: HTMLDivElement, text: string) {
  feed.innerHTML = `<div class="hint">${escapeHtml(text)}</div>`;
}

function latencyClass(ms: number) {
  if (ms < 1500) return "lat-good";
  if (ms < 3000) return "lat-ok";
  return "lat-slow";
}

function appendCue(feed: HTMLDivElement, cue: Cue, latEl?: HTMLSpanElement) {
  feed.querySelector(".hint")?.remove();
  feed.querySelectorAll(".cue.latest").forEach((el) => el.classList.remove("latest"));

  // Both publisher and subscriber are this browser, so origin_ts_ms shares our clock:
  // the e2e number here is a true wall-clock latency, not a cross-host estimate.
  const lat = cue.origin_ts_ms ? Math.max(0, Date.now() - cue.origin_ts_ms) : undefined;
  const llm = cue.translate_ms && cue.translate_ms > 0 ? cue.translate_ms : undefined;
  const latBadge =
    lat === undefined ? "" : `<span class="lat-badge ${latencyClass(lat)}">e2e +${lat} ms</span>`;
  const llmBadge =
    llm === undefined ? "" : `<span class="lat-badge llm-badge">llm ${llm} ms</span>`;

  const row = document.createElement("div");
  row.className = "cue latest";
  row.innerHTML = `<span class="seq">#${cue.seq}</span>${escapeHtml(cue.text)}${latBadge}${llmBadge}`;
  feed.insertBefore(row, feed.firstChild);
  while (feed.childElementCount > 60) feed.lastElementChild?.remove();
  feed.scrollTop = 0;

  if (latEl && lat !== undefined) {
    latEl.textContent = llm !== undefined ? `e2e ${lat} ms · llm ${llm} ms` : `e2e ${lat} ms`;
    latEl.className = `lat ${latencyClass(lat)}`;
  }
}

// ---- WebVTT parsing ----------------------------------------------------------

/** Parse a timestamp like `00:01:02.500`, `01:02.500`, or with a `,` decimal. */
function parseTs(t: string): number {
  const parts = t.trim().split(":");
  let h = 0,
    m = 0,
    s = 0;
  if (parts.length === 3) {
    h = +parts[0];
    m = +parts[1];
    s = parseFloat(parts[2].replace(",", "."));
  } else if (parts.length === 2) {
    m = +parts[0];
    s = parseFloat(parts[1].replace(",", "."));
  } else {
    s = parseFloat(parts[0].replace(",", "."));
  }
  return Math.round((h * 3600 + m * 60 + s) * 1000);
}

/** Minimal WebVTT parser: enough for standard cue blocks (ignores NOTE/STYLE/regions). */
function parseVtt(text: string): VttCue[] {
  const out: VttCue[] = [];
  const blocks = text.replace(/\r\n?/g, "\n").split(/\n\n+/);
  for (const block of blocks) {
    const lines = block.split("\n").map((l) => l.trim());
    const tIdx = lines.findIndex((l) => l.includes("-->"));
    if (tIdx === -1) continue; // WEBVTT header, NOTE, STYLE, blank, etc.
    const [l, r] = lines[tIdx].split("-->");
    if (l === undefined || r === undefined) continue;
    const start = parseTs(l);
    const end = parseTs(r.trim().split(/\s+/)[0]); // drop any cue settings after the end ts
    const body = lines
      .slice(tIdx + 1)
      .filter((x) => x.length > 0)
      .join(" ")
      .replace(/<[^>]+>/g, "") // strip inline VTT tags (<c>, <i>, <00:00:00.000>, …)
      .trim();
    if (!body || !isFinite(start) || !isFinite(end)) continue;
    out.push({ start_ms: start, end_ms: end, text: body });
  }
  out.sort((a, b) => a.start_ms - b.start_ms);
  return out;
}

function maybeEnableConnect() {
  connectBtn.disabled = !(video.src && cues.length > 0);
}

async function loadVtt(file: File) {
  try {
    cues = parseVtt(await file.text());
    lastPublishedIdx = -1;
    embeddedSel.value = ""; // an explicit .vtt overrides any embedded track
    showHint(srcFeed, `${cues.length} caption cues loaded — press ▶ to start publishing.`);
  } catch (err) {
    cues = [];
    showHint(srcFeed, `failed to parse VTT: ${err}`);
  }
  maybeEnableConnect();
}

// ---- Embedded MP4 subtitles (tx3g / wvtt) ------------------------------------

/** tx3g / mov_text: a 2-byte big-endian length prefix, then that many UTF-8 bytes, then styling. */
function decodeTx3g(data: Uint8Array): string {
  if (data.length < 2) return "";
  const len = (data[0] << 8) | data[1];
  if (len === 0) return "";
  return new TextDecoder("utf-8").decode(data.subarray(2, 2 + len)).trim();
}

/** WebVTT-in-MP4: walk boxes for each `vttc` cue and pull the `payl` (payload) text out of it. */
function decodeWvtt(data: Uint8Array): string {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const fourcc = (o: number) => String.fromCharCode(data[o + 4], data[o + 5], data[o + 6], data[o + 7]);
  const parts: string[] = [];
  let off = 0;
  while (off + 8 <= data.length) {
    const size = view.getUint32(off);
    if (size < 8 || off + size > data.length) break;
    if (fourcc(off) === "vttc") {
      let io = off + 8;
      while (io + 8 <= off + size) {
        const isize = view.getUint32(io);
        if (isize < 8 || io + isize > off + size) break;
        if (fourcc(io) === "payl") {
          parts.push(new TextDecoder("utf-8").decode(data.subarray(io + 8, io + isize)).trim());
        }
        io += isize;
      }
    }
    off += size;
  }
  return parts.join(" ").trim();
}

function decodeSample(codec: string, data?: Uint8Array): string {
  if (!data) return "";
  if (codec.startsWith("tx3g") || codec === "text") return decodeTx3g(data);
  if (codec.startsWith("wvtt")) return decodeWvtt(data);
  return ""; // stpp/TTML and others not decoded
}

const CODEC_SUPPORTED = (c: string) => c.startsWith("tx3g") || c === "text" || c.startsWith("wvtt");

// How much of the file to hand mp4box at a time. The file is streamed through in windows this size
// (never held whole in RAM), so multi-GB files work with bounded memory.
const CHUNK_BYTES = 4 * 1024 * 1024;

/** Read `[start, start+size)` of `file` as an mp4box buffer tagged with its file offset. */
async function readChunk(file: File, start: number, size: number): Promise<MP4BoxBuffer> {
  const ab = await file.slice(start, start + size).arrayBuffer();
  const buf = ab as ArrayBuffer & { fileStart: number };
  buf.fileStart = start;
  return buf as unknown as MP4BoxBuffer;
}

/**
 * Feed `file` to a fresh mp4box instance in windows, honoring the next-position it returns so it can
 * skip past media it doesn't need. `stopWhen` ends the stream early (used by detection, which only
 * needs the front `moov`). Never loads the whole file into memory.
 */
async function streamToMp4(
  file: File,
  mp4: ReturnType<typeof createFile>,
  stopWhen: () => boolean,
): Promise<void> {
  let pos = 0;
  let guard = 0;
  while (!stopWhen() && pos < file.size) {
    const size = Math.min(CHUNK_BYTES, file.size - pos);
    const next = mp4.appendBuffer(await readChunk(file, pos, size));
    if (stopWhen()) break;
    // Honor mp4box's requested next position: it jumps FORWARD past media it doesn't need, and
    // BACKWARD to sample bytes when the moov is at the end of the file (non-faststart). If it asks
    // for the same spot, advance a chunk so we never spin.
    pos = next !== pos ? next : pos + size;
    if (++guard > 20000) {
      console.warn("mp4 stream: iteration guard hit; stopping");
      break;
    }
  }
  if (!stopWhen()) mp4.flush();
}

/** Demux an MP4 file and list its subtitle tracks (reads only up to the `moov`; any file size). */
function detectEmbeddedSubs(file: File): Promise<EmbeddedTrack[]> {
  return new Promise((resolve) => {
    const mp4 = createFile();
    let done = false;
    const finish = (tracks: EmbeddedTrack[]) => {
      if (!done) {
        done = true;
        resolve(tracks);
      }
    };
    mp4.onError = (module: string, message: string) => {
      console.warn("mp4box parse error:", module, message);
      finish([]);
    };
    mp4.onReady = (info: Movie) => {
      finish(
        (info.subtitleTracks ?? []).map((t) => ({
          id: t.id,
          codec: t.codec,
          lang: iso2(t.language),
          raw: t.language,
        })),
      );
    };
    // Stop as soon as onReady fires (moov parsed); for a moov-at-front file that's ~the first chunks.
    void streamToMp4(file, mp4, () => done).then(() => finish([]));
  });
}

/** Extract one subtitle track's cues by streaming the file through mp4box with bounded memory. */
function extractEmbeddedTrack(
  file: File,
  track: EmbeddedTrack,
  onProgress?: (cues: number) => void,
): Promise<VttCue[]> {
  return new Promise((resolve) => {
    const mp4 = createFile();
    const out: VttCue[] = [];
    let errored = false;
    mp4.onError = (module: string, message: string) => {
      console.warn("mp4box extract error:", module, message);
      errored = true;
    };
    mp4.onReady = () => {
      mp4.setExtractionOptions(track.id, null, { nbSamples: 500 });
      mp4.start();
    };
    mp4.onSamples = (_id: number, _user: unknown, samples: Sample[]) => {
      let lastNum = -1;
      for (const s of samples) {
        const text = decodeSample(track.codec, s.data);
        if (text) {
          out.push({
            start_ms: Math.round((s.cts / s.timescale) * 1000),
            end_ms: Math.round(((s.cts + s.duration) / s.timescale) * 1000),
            text,
          });
        }
        lastNum = s.number;
      }
      if (lastNum >= 0) mp4.releaseUsedSamples(track.id, lastNum); // free the sample data we've read
      onProgress?.(out.length);
    };
    void streamToMp4(file, mp4, () => errored).then(() => {
      out.sort((a, b) => a.start_ms - b.start_ms);
      resolve(out);
    });
  });
}

/** Make sure the source-language dropdown has (and selects) `code`. */
function ensureSrcLang(code: string) {
  if (!Array.from(srcSel.options).some((o) => o.value === code)) {
    const o = document.createElement("option");
    o.value = code;
    o.textContent = LANG_NAMES[code] ?? code;
    srcSel.appendChild(o);
  }
  srcSel.value = code;
  fillSelect(tgtSel, TARGETS.filter((c) => c !== code));
}

/** Populate the embedded-subtitle track picker from a freshly loaded MP4. */
function fillEmbedded() {
  const supported = embeddedTracks.filter((t) => CODEC_SUPPORTED(t.codec));
  if (embeddedTracks.length === 0) {
    embeddedField.style.display = "none";
    embeddedSel.innerHTML = "";
    return;
  }
  embeddedField.style.display = "";
  embeddedSel.innerHTML =
    `<option value="">— use .vtt file —</option>` +
    embeddedTracks
      .map((t) => {
        const name = LANG_NAMES[t.lang] ?? t.raw ?? t.lang;
        const ok = CODEC_SUPPORTED(t.codec) ? "" : " (unsupported)";
        return `<option value="${t.id}"${CODEC_SUPPORTED(t.codec) ? "" : " disabled"}>${name} — ${t.codec}${ok}</option>`;
      })
      .join("");
  // Auto-select the first supported track so it works out of the box.
  if (supported[0]) {
    embeddedSel.value = String(supported[0].id);
    void loadEmbedded(supported[0].id);
  }
}

async function loadEmbedded(trackId: number) {
  const track = embeddedTracks.find((t) => t.id === trackId);
  if (!track || !videoFile) return;
  const name = LANG_NAMES[track.lang] ?? track.raw;
  const big = videoFile.size > 500 * 1024 * 1024;
  showHint(srcFeed, `extracting embedded ${name} subtitles${big ? " (streaming — may take a moment for a large file)" : ""}…`);
  cues = await extractEmbeddedTrack(videoFile, track, (n) => {
    if (big) showHint(srcFeed, `extracting embedded ${name} subtitles… ${n} cues`);
  });
  lastPublishedIdx = -1;
  ensureSrcLang(track.lang);
  showHint(srcFeed, `${cues.length} embedded ${name} cues loaded — press ▶.`);
  maybeEnableConnect();
  if (origin) void connectAndPublish(); // source language changed → fresh publish session
}

// ---- MoQ publish (source) + subscribe (translation) --------------------------

function fillSelect(sel: HTMLSelectElement, codes: string[]) {
  sel.innerHTML = codes.map((c) => `<option value="${c}">${LANG_NAMES[c] ?? c}</option>`).join("");
  sel.value = sel.dataset.default ?? codes[0];
}

/** Resolve a broadcast path to a subscribed `cues` track, waiting until it is announced. */
async function openCuesTrack(path: string): Promise<Moq.Track.Subscriber> {
  const request = origin!.request(Moq.Path.from(path));
  let active = request.active.peek();
  while (!active) {
    await request.active.changed();
    active = request.active.peek();
  }
  return active.track("cues").subscribe({ priority: 0 });
}

async function pumpTranslation(sub: Moq.Track.Subscriber, gen: number) {
  const consumer = new Stream.Consumer<Cue>({ track: sub });
  try {
    for (;;) {
      const cue = await consumer.next();
      if (!cue || gen !== tgtGen) break;
      appendCue(tgtFeed, cue, tgtLat);
      if (!/\bready\]$/.test(cue.text)) {
        setOverlay(ovTrans, cue.text);
        // Hide it when the playhead passes the original end time, delayed by however long this line
        // took to arrive, with a readable floor. Cleared by `publishDue` on the next tick/seek.
        const lat = cue.origin_ts_ms ? Math.max(0, Date.now() - cue.origin_ts_ms) : 0;
        const nowMs = video.currentTime * 1000;
        transHideAt = Math.max((cue.end_ms || nowMs) + lat, nowMs + MIN_TRANS_SHOW_MS);
      }
    }
  } catch (err) {
    if (gen === tgtGen) console.error("translation pump error:", err);
  }
}

async function subscribeTranslation() {
  tgtGen++;
  const gen = tgtGen;
  tgtSub?.close();
  tgtSub = undefined;
  tgtLat.textContent = "";
  tgtLat.className = "lat";
  setOverlay(ovTrans, "");
  if (!origin) return;

  const src = srcSel.value;
  const tgt = tgtSel.value;
  const path = `${videoId()}/translations/${src}-${tgt}`;
  tgtPath.textContent = path;
  showHint(tgtFeed, "waiting for translator (first LLM output may take a moment)…");
  try {
    const sub = await openCuesTrack(path);
    if (gen !== tgtGen) return sub.close();
    tgtSub = sub;
    void pumpTranslation(sub, gen);
  } catch (err) {
    if (gen === tgtGen) showHint(tgtFeed, `translator not available (${err})`);
  }
}

async function connectAndPublish() {
  disconnect();
  const src = srcSel.value;

  setStatus("connecting…", false);
  connectBtn.disabled = true;
  let url: URL;
  try {
    // Mint a fresh short-lived token in-browser if a signing key is stored (GitHub Pages case).
    url = new URL(await resolveRelay(relayInput.value));
  } catch (err) {
    setStatus(`bad relay/key: ${err}`, false);
    connectBtn.disabled = false;
    return;
  }
  try {
    origin = new Moq.Origin.Producer();
    // One origin, both directions: publish our source and consume the peer's translations.
    connection = await Moq.Connection.connect({ url, publish: origin.consume(), consume: origin });
  } catch (err) {
    setStatus(`connect failed: ${err}`, false);
    connectBtn.disabled = false;
    return;
  }

  // Publish side: our caption source broadcast.
  const path = `${videoId()}/captions/${src}`;
  srcPath.textContent = path;
  srcBroadcast = origin.createBroadcast(Moq.Path.from(path));
  const track = srcBroadcast.createTrack("cues");
  srcProducer = new Stream.Producer<Cue>({ track });
  srcBroadcast.announce();
  // Prime the source so the relay advertises a route immediately — this lets the translator
  // connect (and our translation subscription's demand start propagating) before playback,
  // so the first real cue isn't dropped by the translator's lazy demand gate.
  srcProducer.append({
    seq: 0,
    start_ms: 0,
    end_ms: 0,
    text: `[${LANG_NAMES[src] ?? src} source ready]`,
    lang: src,
    origin_ts_ms: 0,
    translate_ms: 0,
  });

  setStatus(`connected to ${url.host}`, true);
  connectBtn.textContent = "Reconnect";
  connectBtn.disabled = false;

  // Publish is driven purely by the current playhead, so nothing to seed here.
  lastPublishedIdx = -1;
  await subscribeTranslation();
  publishDue(); // send the caption for the current position immediately, even while paused
}

function disconnect() {
  tgtGen++;
  tgtSub?.close();
  tgtSub = undefined;
  srcProducer = undefined;
  srcBroadcast?.close();
  srcBroadcast = undefined;
  connection?.close();
  connection = undefined;
  origin = undefined;
}

// ---- playback → publish ------------------------------------------------------

function setOverlay(el: HTMLElement, text: string) {
  el.textContent = text;
}

/** Index of the last cue that has started by `ms` (greatest `start_ms <= ms`), or -1. */
function startedIdx(ms: number): number {
  let lo = 0,
    hi = cues.length - 1,
    res = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (cues[mid].start_ms <= ms) {
      res = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return res;
}

/**
 * Publish only the cue that belongs to the current playhead — never a backlog.
 *
 * Keyed off `video.currentTime`, so it stays synced to playback: seeking forward skips the cues in
 * between instead of flooding them onto the wire (which would queue on the singleton GPU worker and
 * make every translation lag further behind), and seeking backward just re-sends the cue at the new
 * position. Each cue is sent once, when it becomes the active one.
 */
function publishDue() {
  const nowMs = video.currentTime * 1000;

  // Clear the translated overlay once the playhead passes its (latency-shifted) end time, so it
  // disappears during silent gaps instead of lingering until the next line arrives.
  if (transHideAt >= 0 && nowMs >= transHideAt) {
    setOverlay(ovTrans, "");
    transHideAt = -1;
  }

  const idx = startedIdx(nowMs);
  const active = idx >= 0 && nowMs < cues[idx].end_ms;

  // Show the source caption for the current window regardless of publish/connection state.
  setOverlay(ovSrc, active ? cues[idx].text : "");

  if (!srcProducer) return;
  if (idx === lastPublishedIdx) return; // already sent the current cue
  lastPublishedIdx = idx; // advance even when skipping, so a scrubbed-past cue is never sent late
  if (!active) return; // in a gap between cues (or scrubbed past one that already ended)

  const c = cues[idx];
  const cue: Cue = {
    seq: idx,
    start_ms: c.start_ms,
    end_ms: c.end_ms,
    text: c.text,
    lang: srcSel.value,
    origin_ts_ms: Date.now(),
    translate_ms: 0,
  };
  try {
    srcProducer.append(cue);
  } catch (err) {
    console.error("source append error:", err);
    return;
  }
  appendCue(srcFeed, cue);
}

// ---- wiring ------------------------------------------------------------------

fillSelect(srcSel, SOURCES);
fillSelect(tgtSel, TARGETS.filter((c) => c !== srcSel.value));
updateRunNote();

videoIdInput.addEventListener("input", updateRunNote);

videoUrlInput.addEventListener("change", () => {
  const u = videoUrlInput.value.trim();
  if (u) {
    video.src = u;
    maybeEnableConnect();
  }
});

videoFileInput.addEventListener("change", () => {
  const f = videoFileInput.files?.[0];
  if (!f) return;
  videoFile = f;
  video.src = URL.createObjectURL(f);
  videoUrlInput.value = "";
  maybeEnableConnect();
  embeddedTracks = [];
  fillEmbedded();
  // Scan the container for subtitle tracks (tx3g / wvtt) — streams only the front, so any size works.
  showHint(srcFeed, "scanning for embedded subtitle tracks…");
  void detectEmbeddedSubs(f).then((tracks) => {
    embeddedTracks = tracks;
    fillEmbedded();
    if (tracks.length === 0) {
      showHint(srcFeed, "no readable embedded subtitles — load a .vtt file, or press ▶ if using one.");
    }
  });
});

vttFileInput.addEventListener("change", () => {
  const f = vttFileInput.files?.[0];
  if (f) void loadVtt(f);
});

embeddedSel.addEventListener("change", () => {
  const id = embeddedSel.value;
  if (id) void loadEmbedded(Number(id));
});

srcSel.addEventListener("change", () => {
  const prev = tgtSel.value;
  fillSelect(tgtSel, TARGETS.filter((c) => c !== srcSel.value));
  if (prev !== srcSel.value) tgtSel.value = prev;
  // Source path changes → a fresh publish session is required.
  if (origin) void connectAndPublish();
});

tgtSel.addEventListener("change", () => {
  if (origin) void subscribeTranslation();
});

video.addEventListener("timeupdate", publishDue);
// Publish is playhead-driven, so a seek just needs an immediate re-evaluation at the new position.
video.addEventListener("seeked", publishDue);

// Clearing the source also clears the translation on the video: the original line is gone, so its
// translated overlay should go with it. In-flight (delayed) translations still arrive normally.
clearLeftBtn.addEventListener("click", () => {
  showHint(srcFeed, "cleared.");
  setOverlay(ovSrc, "");
  setOverlay(ovTrans, "");
  transHideAt = -1;
});

clearRightBtn.addEventListener("click", () => {
  showHint(tgtFeed, "cleared.");
  setOverlay(ovTrans, "");
  transHideAt = -1;
});

// Fullscreen the whole stage (video + overlay) so both captions remain visible, rather than the
// bare <video> element which would hide the overlay.
fullscreenBtn.addEventListener("click", () => {
  if (document.fullscreenElement) void document.exitFullscreen();
  else void stage.requestFullscreen();
});

connectBtn.addEventListener("click", () => void connectAndPublish());
