// Headless smoke test: pivot translation. left=source(native), right=translation FROM source.
// Verifies English is available as a translated OUTPUT (es -> en).
import { chromium } from "playwright";

const URL = process.env.URL || "http://localhost:5174/";
const browser = await chromium.launch({ channel: "chrome", headless: true });
const page = await browser.newPage();

const logs = [];
page.on("console", (m) => logs.push(`[console.${m.type()}] ${m.text()}`));
page.on("pageerror", (e) => logs.push(`[pageerror] ${e.message}`));

const fail = async (msg) => {
  console.log("RESULT: FAIL —", msg);
  console.log(logs.slice(-25).join("\n"));
  await browser.close();
  process.exit(1);
};
const firstReal = (sel) =>
  page.$$eval(sel, (n) => n.map((e) => e.textContent).find((t) => t && !/ready/i.test(t)));
const waitReal = (sel, t) =>
  page.waitForFunction(
    (s) => [...document.querySelectorAll(s)].some((e) => !/ready/i.test(e.textContent || "")),
    sel,
    { timeout: t },
  );

await page.goto(URL, { waitUntil: "load" });
await page.click("#connect");
try {
  await page.waitForFunction(
    () => document.querySelector("#status")?.classList.contains("connected"),
    { timeout: 20000 },
  );
} catch {
  await fail("did not connect");
}
console.log("connected ✓");

// Default: left=en (source), right=tr (translation en->tr).
try {
  await page.waitForFunction(() => document.querySelectorAll("#left-feed .cue").length >= 1, {
    timeout: 20000,
  });
} catch {
  await fail("no native en source cues");
}
console.log("left native en ✓:", await firstReal("#left-feed .cue"));

try {
  await waitReal("#right-feed .cue", 90000);
} catch {
  await fail("no en->tr translation");
}
console.log("right en->tr ✓:", await firstReal("#right-feed .cue"), "| path:", await page.textContent("#right-path"));

// Now the key ask: English as a TRANSLATED OUTPUT. Set source=es, target=en => es->en.
await page.selectOption("#left-lang", "es");
await page.selectOption("#right-lang", "en");
console.log("set source=es, target=en (es->en)…");
try {
  await waitReal("#right-feed .cue", 90000);
} catch {
  await fail("no es->en translation (English as output)");
}
const enOut = await firstReal("#right-feed .cue");
const rpath = await page.textContent("#right-path");
console.log("English as translated output ✓:", enOut, "| path:", rpath);
if (!/translations\/es-en/.test(rpath || "")) await fail(`wrong path for es->en: ${rpath}`);

// Latency + LLM time reported.
const lat = await page.textContent("#right-lat");
if (!/e2e .* ms/.test(lat || "")) await fail("no e2e latency on translation");
if (!/llm .* ms/.test(lat || "")) await fail(`no LLM translate time reported: "${lat}"`);
const llmBadge = await page.$eval("#right-feed .llm-badge", (e) => e.textContent).catch(() => null);
if (!llmBadge) await fail("no per-cue llm badge");
console.log("latency + llm time ✓:", lat, "| badge:", llmBadge);

await browser.close();
console.log("RESULT: PASS");
