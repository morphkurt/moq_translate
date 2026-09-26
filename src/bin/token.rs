//! Mint a relay URL carrying a signed `?jwt=` for the browser demos.
//!
//! The Rust services self-mint on startup (they hold the key locally), but the web pages cannot be
//! trusted with the symmetric key, so mint a token here and paste the printed URL into the page's
//! "Relay" field:
//!
//! ```bash
//! cargo run --bin token
//! ```

use anyhow::Result;
use clap::Parser;
use url::Url;

#[derive(Parser)]
#[command(about = "Print a relay URL with a freshly signed ?jwt= for the web demos")]
struct Args {
    #[arg(long, default_value = moq_translate::DEFAULT_RELAY)]
    relay: Url,
    #[arg(long, default_value = moq_translate::DEFAULT_AUTH_KEY)]
    auth_key: String,
    /// Print only the bare token instead of the full URL.
    #[arg(long)]
    bare: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let url = moq_translate::signed_relay(&args.relay, &args.auth_key)?;
    if args.bare {
        let token = url
            .query_pairs()
            .find(|(k, _)| k == "jwt")
            .map(|(_, v)| v.into_owned())
            .unwrap_or_default();
        println!("{token}");
    } else {
        println!("{url}");
    }
    Ok(())
}
