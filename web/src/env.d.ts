// The minted relay URL (with a fresh `?jwt=`) for the authed relay, provided by the
// `moq-relay-token` plugin in vite.config.ts.
declare module "virtual:relay" {
  const url: string;
  export default url;
}
