import * as Moq from "@moq/net";
import { Stream } from "@moq/json";
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

// Native source languages (published directly) and the languages we can translate INTO.
// Translation is a pivot: FROM the left/source language TO the right/target language, so any
// language — English included — can be a target.
const SOURCES = ["en", "es", "fr", "de"];
const TARGETS = ["en", "es", "fr", "de", "tr", "ja", "pt", "si"];

const $ = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
const relayInput = $<HTMLInputElement>("relay");
const jwkInput = $<HTMLInputElement>("jwk");
relayInput.value = defaultRelay();
jwkInput.value = getStoredJwk();
relayInput.addEventListener("change", () => setStoredRelay(relayInput.value));
jwkInput.addEventListener("change", () => setStoredJwk(jwkInput.value));
const videoInput = $<HTMLInputElement>("video");
const connectBtn = $<HTMLButtonElement>("connect");
const statusEl = $<HTMLDivElement>("status");

const leftSel = $<HTMLSelectElement>("left-lang");
const rightSel = $<HTMLSelectElement>("right-lang");
const leftFeed = $<HTMLDivElement>("left-feed");
const rightFeed = $<HTMLDivElement>("right-feed");
const leftPath = $<HTMLSpanElement>("left-path");
const rightPath = $<HTMLSpanElement>("right-path");
const leftLat = $<HTMLSpanElement>("left-lat");
const rightLat = $<HTMLSpanElement>("right-lat");

let origin: Moq.Origin.Producer | undefined;
let connection: Awaited<ReturnType<typeof Moq.Connection.connect>> | undefined;

let leftGen = 0;
let rightGen = 0;
let leftSub: Moq.Track.Subscriber | undefined;
let rightSub: Moq.Track.Subscriber | undefined;

function setStatus(text: string, connected: boolean) {
  statusEl.classList.toggle("connected", connected);
  (statusEl.querySelector(".txt") as HTMLElement).textContent = text;
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
  if (ms < 400) return "lat-good";
  if (ms < 2000) return "lat-ok";
  return "lat-slow";
}

function appendCue(feed: HTMLDivElement, cue: Cue, latEl?: HTMLSpanElement) {
  feed.querySelector(".hint")?.remove();
  feed.querySelectorAll(".cue.latest").forEach((el) => el.classList.remove("latest"));

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
  while (feed.childElementCount > 40) feed.lastElementChild?.remove();
  feed.scrollTop = 0;

  if (latEl && lat !== undefined) {
    latEl.textContent = llm !== undefined ? `e2e ${lat} ms · llm ${llm} ms` : `e2e ${lat} ms`;
    latEl.className = `lat ${latencyClass(lat)}`;
  }
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

async function pump(
  sub: Moq.Track.Subscriber,
  feed: HTMLDivElement,
  isStale: () => boolean,
  latEl?: HTMLSpanElement,
) {
  const consumer = new Stream.Consumer<Cue>({ track: sub });
  try {
    for (;;) {
      const cue = await consumer.next();
      if (!cue || isStale()) break;
      appendCue(feed, cue, latEl);
    }
  } catch (err) {
    if (!isStale()) console.error("pump error:", err);
  }
}

function fillSources(sel: HTMLSelectElement) {
  sel.innerHTML = SOURCES.map(
    (c) => `<option value="${c}">${LANG_NAMES[c]} — source</option>`,
  ).join("");
  sel.value = sel.dataset.default ?? SOURCES[0];
}

/** Rebuild the target dropdown, excluding the current source (no point translating x→x). */
function fillTargets(sel: HTMLSelectElement, exclude: string) {
  const prev = sel.value;
  sel.innerHTML =
    `<option value="">— none —</option>` +
    TARGETS.filter((c) => c !== exclude)
      .map((c) => `<option value="${c}">${LANG_NAMES[c]}</option>`)
      .join("");
  const want = prev || sel.dataset.default || "";
  sel.value = want !== exclude ? want : "";
}

const videoId = () => videoInput.value.trim() || "live1";

async function subscribeLeft() {
  leftGen++;
  const gen = leftGen;
  leftSub?.close();
  leftSub = undefined;
  leftLat.textContent = "";
  leftLat.className = "lat";
  if (!origin) return;

  const src = leftSel.value;
  const path = `${videoId()}/captions/${src}`;
  leftPath.textContent = path;
  showHint(leftFeed, "waiting for source…");
  try {
    const sub = await openCuesTrack(path);
    if (gen !== leftGen) return sub.close();
    leftSub = sub;
    pump(sub, leftFeed, () => gen !== leftGen, leftLat);
  } catch (err) {
    if (gen === leftGen) showHint(leftFeed, `source not available (${err})`);
  }
}

async function subscribeRight() {
  rightGen++;
  const gen = rightGen;
  rightSub?.close();
  rightSub = undefined;
  rightLat.textContent = "";
  rightLat.className = "lat";
  if (!origin) return;

  const src = leftSel.value;
  const tgt = rightSel.value;
  if (!tgt) {
    rightPath.textContent = "";
    showHint(rightFeed, "Pick a language to translate into.");
    return;
  }
  const path = `${videoId()}/translations/${src}-${tgt}`;
  rightPath.textContent = path;
  showHint(rightFeed, "sent intent — waiting for translator (first LLM output may take a moment)…");
  try {
    const sub = await openCuesTrack(path);
    if (gen !== rightGen) return sub.close();
    rightSub = sub;
    pump(sub, rightFeed, () => gen !== rightGen, rightLat);
  } catch (err) {
    if (gen === rightGen) showHint(rightFeed, `translator not available (${err})`);
  }
}

async function connect() {
  disconnect();

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
    connection = await Moq.Connection.connect({ url, consume: origin });
  } catch (err) {
    setStatus(`connect failed: ${err}`, false);
    connectBtn.disabled = false;
    return;
  }
  setStatus(`connected to ${url.host}`, true);
  connectBtn.textContent = "Reconnect";
  connectBtn.disabled = false;

  await Promise.all([subscribeLeft(), subscribeRight()]);
}

function disconnect() {
  leftGen++;
  rightGen++;
  leftSub?.close();
  rightSub?.close();
  leftSub = undefined;
  rightSub = undefined;
  connection?.close();
  origin = undefined;
  connection = undefined;
}

// Wire up. The source (left) drives the target list, since target excludes the source and each
// translation pivots off it.
fillSources(leftSel);
fillTargets(rightSel, leftSel.value);
leftSel.addEventListener("change", () => {
  fillTargets(rightSel, leftSel.value);
  void subscribeLeft();
  void subscribeRight();
});
rightSel.addEventListener("change", () => void subscribeRight());
connectBtn.addEventListener("click", connect);
