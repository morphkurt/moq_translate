import { defineConfig, type Plugin } from "vite";
import { resolve } from "path";
import { execFileSync } from "child_process";

/**
 * Mint a relay URL with a fresh `?jwt=` at dev/build time by running the Rust `token` binary.
 *
 * The browser must never hold the account's symmetric signing key, so we sign out-of-band here and
 * expose the result through a virtual module. Falls back to the old anonymous relay if the binary
 * hasn't been built (`cargo build --bin token`).
 */
function mintRelay(): string {
  const bin = resolve(__dirname, "../target/debug/token");
  try {
    return execFileSync(bin, [], { cwd: resolve(__dirname, ".."), encoding: "utf8" }).trim();
  } catch (err) {
    console.warn(`[vite] could not mint relay token via ${bin}; falling back to anon.`, err);
    return "https://cdn.moq.dev/anon";
  }
}

/** Serve the minted relay URL as `import RELAY from "virtual:relay"` — DEV ONLY. */
function relayPlugin(): Plugin {
  const virtualId = "virtual:relay";
  const resolvedId = "\0" + virtualId;
  let isServe = false;
  return {
    name: "moq-relay-token",
    configResolved(cfg) {
      isServe = cfg.command === "serve";
    },
    resolveId(id) {
      if (id === virtualId) return resolvedId;
    },
    load(id) {
      if (id !== resolvedId) return;
      // Only self-mint for local dev. A production build must NEVER bake a token — the deployed
      // static site mints client-side from the operator's pasted key (stored in localStorage).
      const relay = isServe ? mintRelay() : "";
      if (isServe) console.log(`[vite] virtual:relay = ${relay.slice(0, 48)}…`);
      return `export default ${JSON.stringify(relay)};`;
    },
  };
}

export default defineConfig({
  // On GitHub Pages a project site is served under /<repo>/, so the CI build sets VITE_BASE.
  base: process.env.VITE_BASE ?? "/",
  plugins: [relayPlugin()],
  build: {
    rollupOptions: {
      input: {
        main: resolve(__dirname, "index.html"),
        video: resolve(__dirname, "video.html"),
      },
    },
  },
});
