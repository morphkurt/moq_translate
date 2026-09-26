// Client-side relay auth for static hosting (e.g. GitHub Pages), where there is no server to hold
// the account secret. The operator pastes their signing key (JWK) once; it is kept in localStorage
// on THIS browser only (never in the repo or the bundle), and short-lived tokens are minted here on
// each connect. Only someone who has the key can mint — a visitor without it can do nothing.
//
// Security note: localStorage is readable by any script on this origin, so only paste the key on a
// machine you trust and keep the site's dependencies trusted.
import { Key, type Claims } from "@moq/auth";
import DEV_RELAY from "virtual:relay";

const LS_RELAY = "moq.relay"; // base relay URL, e.g. https://cdn.moq.pro/<account>
const LS_JWK = "moq.jwk"; // the HS256/asymmetric signing key (JWK JSON)
const TOKEN_TTL_SECONDS = 10 * 60; // mint short-lived tokens: a leaked one dies quickly

export const getStoredRelay = () => localStorage.getItem(LS_RELAY) ?? "";
export const setStoredRelay = (v: string) => localStorage.setItem(LS_RELAY, v.trim());
export const getStoredJwk = () => localStorage.getItem(LS_JWK) ?? "";
export const hasStoredKey = () => getStoredJwk().trim().length > 0;
export function setStoredJwk(v: string) {
  const t = v.trim();
  if (t) localStorage.setItem(LS_JWK, t);
  else localStorage.removeItem(LS_JWK);
}

/** Default for the relay field: stored value → dev-minted URL (has a token) → the moq.pro base. */
export function defaultRelay(): string {
  const stored = getStoredRelay();
  if (stored) return stored;
  if (DEV_RELAY && DEV_RELAY.includes("jwt=")) return DEV_RELAY; // local `npm run dev` self-mint
  return "https://cdn.moq.pro/pk54jf2kv65wn";
}

/** The account root a token is scoped under: the relay URL's path (e.g. `/pk54jf2kv65wn`). */
function rootOf(relay: string): string {
  try {
    return new URL(relay).pathname.replace(/^\/+|\/+$/g, "");
  } catch {
    return "";
  }
}

async function mint(jwk: string, root: string): Promise<string> {
  const now = Math.floor(Date.now() / 1000);
  const claims: Claims = {
    root,
    publish: ["**"],
    subscribe: ["**"],
    iat: now,
    exp: now + TOKEN_TTL_SECONDS,
  };
  return Key.sign(Key.parse(jwk), claims);
}

/**
 * Turn the base relay URL into a connectable one. If a signing key is stored, mint a fresh
 * short-lived token in-browser and attach it as `?jwt=`. Otherwise return the URL as-is (which may
 * already carry a token — the local dev mint, or one the operator pasted directly).
 */
export async function resolveRelay(base: string): Promise<string> {
  const b = base.trim();
  const jwk = getStoredJwk().trim();
  if (!jwk || !b) return b;
  const url = new URL(b);
  url.search = "";
  url.searchParams.set("jwt", await mint(jwk, rootOf(b)));
  return url.toString();
}
