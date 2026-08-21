mod store;

use std::collections::HashMap;
use std::io::Write as _;
use std::sync::OnceLock;

use chrono::{Duration as ChronoDuration, Local, TimeZone};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::COOKIE;

use store::{ChannelRow, MessageRef, Store, StoredMessage};

const USAGE: &str = "\
usage: myslackcli [options]

  -t, --time <duration>   load history this far back first (e.g. 45s, 30m, 2h, 3d)
      --sql <file>        record every message in an sqlite file (also --sql=<file>)
      --local-no-sync     render the --sql file and exit: no Slack calls, no live stream
      --to-app-url=<id>   print the slack:// deep link for a message id and exit
      --to-web-url=<id>   print the https:// permalink for a message id and exit
      --env <file>        read SLACK_TOKEN/SLACK_COOKIE from this file
                          (default: ~/.config/myslackcli/.env)
      --no-color          plain output without ANSI colours
  -v, --verbose           report progress while running
  -h, --help              show this message
";

const GREEN: &str = "\x1b[32m";
const BLUE: &str = "\x1b[34m";
const CYAN: &str = "\x1b[36m";
const YELLOW: &str = "\x1b[33m";
const MAGENTA: &str = "\x1b[35m";
const RESET: &str = "\x1b[0m";

// Set once at startup: color is on by default, off with --no-color. (Tools
// like fzf can display ANSI-colored input fine via their own --ansi flag,
// so this doesn't need to auto-disable based on whether stdout is a TTY.)
static COLOR_ENABLED: OnceLock<bool> = OnceLock::new();

fn color_enabled() -> bool {
    *COLOR_ENABLED.get().unwrap_or(&false)
}

// Returns `code` if color is enabled, otherwise "" — wrap every color/reset
// constant with this instead of using them directly.
fn c(code: &'static str) -> &'static str {
    if color_enabled() { code } else { "" }
}

#[derive(Debug, Deserialize)]
struct RtmConnectResponse {
    ok: bool,
    error: Option<String>,
    url: Option<String>,
}

struct HistMessage {
    ts: f64,
    // The unparsed "1690000000.000100" form. Kept alongside the f64 because
    // thread_tag() hashes this string verbatim: a parent identified via its
    // own ts must hash identically to the thread_ts its replies carry, and a
    // round-trip through f64 doesn't reliably reproduce the original digits.
    ts_raw: String,
    channel_id: String,
    user_id: String,
    text: String,
    note: Option<String>,
    thread_ts: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut verbose = false;
    let mut no_color = false;
    let mut backfill_arg: Option<String> = None;
    let mut sql_path: Option<String> = None;
    let mut local_no_sync = false;
    let mut env_path: Option<String> = None;
    // (message id, web form?) — the two link flags differ only in the spelling.
    let mut link_for: Option<(String, bool)> = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "-v" | "--verbose" => verbose = true,
            "--no-color" => no_color = true,
            "-t" | "--time" => {
                backfill_arg = Some(args.next().ok_or("missing value for -t (e.g. -t 1h)")?);
            }
            "--sql" => {
                sql_path = Some(args.next().ok_or("missing value for --sql (e.g. --sql=slack.db)")?);
            }
            other if other.starts_with("--sql=") => {
                sql_path = Some(other.trim_start_matches("--sql=").to_string());
            }
            "--local-no-sync" => local_no_sync = true,
            "--env" => {
                env_path = Some(args.next().ok_or("missing value for --env (e.g. --env=/path/.env)")?);
            }
            other if other.starts_with("--env=") => {
                env_path = Some(other.trim_start_matches("--env=").to_string());
            }
            "--to-app-url" | "--to-web-url" => {
                let web = a == "--to-web-url";
                let id = args.next().ok_or_else(|| format!("missing message id for {a}"))?;
                link_for = Some((id, web));
            }
            other if other.starts_with("--to-app-url=") || other.starts_with("--to-web-url=") => {
                let (flag, id) = other.split_once('=').expect("checked by the guard");
                link_for = Some((id.to_string(), flag == "--to-web-url"));
            }
            "-h" | "--help" => {
                print!("{USAGE}");
                return Ok(());
            }
            // Silently ignoring these hid typos: a mistyped --sql= meant a run
            // that looked fine and stored nothing. Printed rather than returned
            // as an Err because main's error reporting uses Debug formatting,
            // which would render the usage text with literal escapes.
            other => {
                eprintln!("unknown argument: {other}\n");
                eprint!("{USAGE}");
                std::process::exit(2);
            }
        }
    }
    COLOR_ENABLED.set(!no_color).ok();

    match &env_path {
        // Named explicitly, so a path that doesn't load is an error rather than
        // something to shrug off — the alternative is failing later with a
        // confusing "SLACK_TOKEN not set".
        Some(path) => dotenvy::from_path(path).map_err(|e| format!("could not read {path}: {e}"))?,
        // Deliberately not the working directory: these are full session
        // credentials for the account, and a file that sits next to the source
        // is one stray `git add -f` away from being published. Absent is fine —
        // the vars may already be in the environment, and the offline modes need
        // neither.
        None => {
            if let Some(path) = default_env_path() {
                dotenvy::from_path(path).ok();
            }
        }
    }

    // Both of these read the database and stop, so they run before the token is
    // even read: with no request to make, credentials aren't a prerequisite.
    if let Some((id, web)) = &link_for {
        let path = sql_path
            .as_deref()
            .ok_or("--to-app-url/--to-web-url look the message up in the database, so they need --sql=<file>")?;
        let id: i64 = id.parse().map_err(|_| format!("message id must be a number, got {id:?}"))?;
        return print_link(path, id, *web);
    }

    // Offline mode renders the database and stops.
    if local_no_sync {
        let path = sql_path
            .as_deref()
            .ok_or("--local-no-sync reads from the database, so it needs --sql=<file>")?;
        if !std::path::Path::new(path).exists() {
            return Err(format!("no such database: {path}").into());
        }
        return replay_local(path);
    }

    let token = std::env::var("SLACK_TOKEN").map_err(|_| "SLACK_TOKEN not set — put it in ~/.config/myslackcli/.env (see .env.example)")?;
    let cookie = std::env::var("SLACK_COOKIE").map_err(|_| "SLACK_COOKIE not set — put it in ~/.config/myslackcli/.env (see .env.example)")?;

    let client = reqwest::Client::new();

    if verbose {
        println!("Resolving users/channels...");
    }
    let mut users = fetch_users(&client, &token, &cookie).await?;
    // This workspace mentions a group as <@S…> — the very same syntax as a
    // person, and resolved against the same map — so the groups belong in it.
    // Not fatal: without them a group mention just stays an id, as it did
    // before, which is no reason to give up the run.
    match fetch_user_groups(&client, &token, &cookie).await {
        Ok(groups) => users.extend(groups),
        Err(e) => eprintln!("could not resolve user groups ({e}) — they stay as ids"),
    }
    let users = users;
    let channel_rows = fetch_channels(&client, &token, &cookie, &users).await?;
    let channels: HashMap<String, String> =
        channel_rows.iter().map(|c| (c.id.clone(), c.name.clone())).collect();
    if verbose {
        println!("{} user(s), {} channel(s)/DM(s) resolved", users.len(), channels.len());
    }

    // Without --sql this stays None and not a single write happens.
    let store = match &sql_path {
        Some(path) => {
            let store = Store::open(path)?;
            store.sync_users(&users)?;
            store.sync_channels(&channel_rows)?;
            // Deliberately not fatal: these two values only power the link
            // flags, so failing to fetch them costs a convenience, not the run.
            // Aborting here meant a rate-limited auth.test threw away the whole
            // session — and behind fzf the error wasn't even visible, since the
            // pager repaints over stderr.
            if let Err(e) = ensure_workspace_meta(&client, &token, &cookie, &store).await {
                eprintln!("could not record workspace details ({e}) — link flags stay unavailable");
            }
            if verbose {
                println!("Storing messages in {path}");
            }
            Some(store)
        }
        None => None,
    };

    // Ctrl+C races the actual work instead of killing the process, so the run
    // ends by falling out of main: the database gets closed properly and a
    // half-written statement can't be left behind. The backfill is covered too,
    // not just the stream — its paginated search is where a run spends the most
    // time, and so where an interrupt is most likely to land.
    let mut interrupted = false;

    if let Some(t_str) = &backfill_arg {
        let duration = parse_duration(t_str).ok_or_else(|| format!("invalid -t value: {t_str:?} (try e.g. 1h, 30m, 2d)"))?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                // The window is written in one transaction at the end, so an
                // interrupt here stores nothing rather than a partial window.
                eprintln!("\nInterrupted during backfill — nothing stored.");
                interrupted = true;
            }
            result = backfill(&client, &token, &cookie, &channels, &users, duration, t_str, store.as_ref()) => result?,
        }
    }

    if !interrupted {
        let ws_url = fetch_rtm_url(&client, &token, &cookie).await?;
        if verbose {
            println!("Connecting to {ws_url}...");
        }

        let mut request = ws_url.clone().into_client_request()?;
        request.headers_mut().insert(COOKIE, format!("d={cookie}").parse()?);

        let (ws_stream, _) = connect_async(request).await?;
        if verbose {
            println!("Connected. Streaming messages from all channels/DMs (Ctrl+C to quit)...\n");
        }

        let (_, mut read) = ws_stream.split();

        loop {
            let msg = tokio::select! {
                _ = tokio::signal::ctrl_c() => break,
                msg = read.next() => msg,
            };
            // None means Slack closed the socket on us — also a reason to stop.
            let Some(msg) = msg else { break };
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("WebSocket error: {e}");
                    break;
                }
            };

            let Ok(text) = msg.to_text() else { continue };
            let Ok(event) = serde_json::from_str::<Value>(text) else { continue };

            handle_event(&event, verbose, &channels, &users, store.as_ref());
        }
    }

    // Closing by hand rather than leaving it to drop, so a failure to flush is
    // reported instead of silently swallowed.
    if let Some(store) = store {
        if let Err(e) = store.close() {
            eprintln!("closing the database failed: {e}");
        }
    }
    if verbose {
        println!("Stopped.");
    }

    Ok(())
}

// $XDG_CONFIG_HOME/myslackcli/.env, falling back to ~/.config/myslackcli/.env.
// Override with --env.
fn default_env_path() -> Option<std::path::PathBuf> {
    let config_home = match std::env::var_os("XDG_CONFIG_HOME") {
        Some(dir) if !dir.is_empty() => std::path::PathBuf::from(dir),
        _ => std::path::PathBuf::from(std::env::var_os("HOME")?).join(".config"),
    };
    Some(config_home.join("myslackcli").join(".env"))
}

// Parses "1h", "30m", "2d", "45s" (bare numbers are treated as seconds).
fn parse_duration(s: &str) -> Option<std::time::Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let last = s.chars().last()?;
    let (value_str, unit) = if last.is_ascii_digit() {
        (s, 's')
    } else {
        (&s[..s.len() - last.len_utf8()], last)
    };
    let value: u64 = value_str.parse().ok()?;
    let secs = match unit {
        's' => value,
        'm' => value.checked_mul(60)?,
        'h' => value.checked_mul(3600)?,
        'd' => value.checked_mul(86400)?,
        _ => return None,
    };
    Some(std::time::Duration::from_secs(secs))
}

// The team id and the workspace domain are what turn a stored message into a
// link, and neither ever changes — so they're fetched once and kept in the
// database. That's what lets --to-app-url work offline later, straight from the
// file, and it costs one extra request per database rather than per run.
async fn ensure_workspace_meta(
    client: &reqwest::Client,
    token: &str,
    cookie: &str,
    store: &Store,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if store.get_meta("team_id")?.is_some() && store.get_meta("workspace_domain")?.is_some() {
        return Ok(());
    }
    let resp = slack_get(client, token, cookie, "https://slack.com/api/auth.test").await?;
    if let Some(team_id) = resp.get("team_id").and_then(Value::as_str) {
        store.set_meta("team_id", team_id)?;
    }
    // auth.test reports the full workspace URL; the permalink wants just the host.
    if let Some(url) = resp.get("url").and_then(Value::as_str) {
        let domain = url
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/');
        store.set_meta("workspace_domain", domain)?;
    }
    Ok(())
}

// Resolves one stored message to a link and prints it. Printing rather than
// opening keeps the caller free to do either — pipe it to xdg-open, or to a
// clipboard — without this needing to know which.
fn print_link(path: &str, id: i64, web: bool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !std::path::Path::new(path).exists() {
        return Err(format!("no such database: {path}").into());
    }
    let store = Store::open(path)?;
    let message = store
        .message_by_id(id)?
        .ok_or_else(|| format!("no message with id {id} in {path}"))?;

    let key = if web { "workspace_domain" } else { "team_id" };
    let value = store.get_meta(key)?.ok_or_else(|| {
        format!("{key} isn't recorded in {path} yet — run once with --sql (online) so it can be fetched")
    })?;

    println!("{}", if web { web_url(&value, &message) } else { app_url(&value, &message) });
    store.close()?;
    Ok(())
}

// Slack addresses a message by conversation plus its raw ts; the two link forms
// only differ in how they spell that. The deep link keeps the dotted ts and
// needs the team id, the permalink drops the dot behind a "p" and needs the
// domain. Both take the thread parent when there is one — without it a reply
// opens the channel rather than the thread it belongs to.
fn app_url(team_id: &str, m: &MessageRef) -> String {
    let mut url = format!("slack://channel?team={team_id}&id={}&message={}", m.channel_id, m.ts);
    if let Some(parent) = &m.thread_ts {
        url += &format!("&thread_ts={parent}");
    }
    url
}

fn web_url(domain: &str, m: &MessageRef) -> String {
    let mut url = format!("https://{domain}/archives/{}/p{}", m.channel_id, m.ts.replace('.', ""));
    if let Some(parent) = &m.thread_ts {
        url += &format!("?thread_ts={parent}&cid={}", m.channel_id);
    }
    url
}

// Reproduces the terminal output from the database alone — no Slack calls, no
// live stream. Because `text` is stored exactly as Slack sent it and the users
// and channels tables hold the same id-to-name mapping the live run resolved
// against, this drives the very same print_message and comes out identical to
// what the message printed when it first arrived.
fn replay_local(path: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let store = Store::open(path)?;
    let users = store.load_users()?;
    let channels = store.load_channels()?;
    let messages = store.load_messages()?;

    println!("--- {} stored message(s) from {path} ---", messages.len());
    for m in &messages {
        print_message(
            Some(m.id),
            &m.ts,
            &m.channel_id,
            // Mirrors the live path, which prints "?" when an event named no user.
            m.user_id.as_deref().unwrap_or("?"),
            &m.text,
            m.note.as_deref(),
            m.thread_ts.as_deref(),
            &channels,
            &users,
        );
    }
    store.close()?;
    Ok(())
}

// Backfills via search.messages instead of iterating conversations.history
// per-channel: a single global, paginated query instead of ~200 individual
// calls, and — crucially — search indexes thread replies too. (History-per-
// channel only surfaces a thread if its *parent's own* timestamp falls in the
// requested window; an old thread with a brand-new reply would be invisible
// to it, since the parent that carries reply_count never gets returned.)
async fn backfill(
    client: &reqwest::Client,
    token: &str,
    cookie: &str,
    channels: &HashMap<String, String>,
    users: &HashMap<String, String>,
    duration: std::time::Duration,
    label: &str,
    store: Option<&Store>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let oldest = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .checked_sub(duration)
        .ok_or("duration too large")?
        .as_secs_f64();

    // A failure part-way through — a rate limit is the likely one — must not
    // discard the pages that already came back. The search appends into this Vec
    // as it goes, so whatever arrived before the error is still here, and the
    // run carries on with it: print it, store it, then go live. Aborting would
    // throw away real work over a transient API error.
    let mut all_messages = Vec::new();
    let outcome = search_messages_since(client, token, cookie, oldest, label, users, &mut all_messages).await;
    eprintln!(); // finish the progress line
    if let Err(e) = outcome {
        eprintln!(
            "History load stopped early ({e}) — continuing with the {} message(s) already fetched.",
            all_messages.len()
        );
    }

    all_messages.sort_by(|a, b| a.ts.partial_cmp(&b.ts).unwrap());
    tag_thread_parents(&mut all_messages);

    // Stored before printing, because the row ids are the first column and only
    // the insert can tell us what they are. Without --sql there are none, and the
    // lines print unprefixed.
    let ids: Vec<Option<i64>> = match store {
        Some(store) => {
            // thread_tag() results have to outlive the borrows held by
            // StoredMessage, so they're materialised before the rows are built.
            let tags: Vec<Option<String>> = all_messages
                .iter()
                .map(|m| m.thread_ts.as_deref().map(|t| thread_tag(&m.channel_id, t)))
                .collect();
            let rows: Vec<StoredMessage> = all_messages
                .iter()
                .zip(&tags)
                .map(|(m, tag)| StoredMessage {
                    channel_id: &m.channel_id,
                    ts: &m.ts_raw,
                    user_id: Some(m.user_id.as_str()),
                    text: &m.text,
                    note: m.note.as_deref(),
                    thread_ts: m.thread_ts.as_deref(),
                    thread_tag: tag.as_deref(),
                    mentions: extract_mentions(&m.text),
                })
                .collect();
            // Same reasoning as above: report and carry on into live mode rather
            // than exiting and losing the session over one failed write. The
            // history still prints, just without ids to link back to.
            match store.save_batch(&rows) {
                Ok(ids) => ids.into_iter().map(Some).collect(),
                Err(e) => {
                    eprintln!("Storing the history failed: {e}");
                    vec![None; all_messages.len()]
                }
            }
        }
        None => vec![None; all_messages.len()],
    };

    println!("--- {} historical message(s) since {label} ---", all_messages.len());
    for (m, id) in all_messages.iter().zip(&ids) {
        print_message(
            *id,
            &m.ts_raw,
            &m.channel_id,
            &m.user_id,
            &m.text,
            m.note.as_deref(),
            m.thread_ts.as_deref(),
            channels,
            users,
        );
    }
    Ok(())
}

// A thread parent carries no thread_ts of its own — Slack identifies it purely
// by the fact that the replies' thread_ts equals the parent's ts. Nothing in a
// single match reveals that, but across the whole result set it's recoverable:
// collect every (channel, thread_ts) the replies point at, then tag any message
// whose own (channel, ts) is one of them. Purely local, no extra API calls.
//
// A parent older than the backfill window simply isn't in `messages` at all, so
// there's nothing to tag — its replies still get the ID and stay groupable.
fn tag_thread_parents(messages: &mut [HistMessage]) {
    let threads: std::collections::HashSet<(&str, &str)> = messages
        .iter()
        .filter_map(|m| m.thread_ts.as_deref().map(|t| (m.channel_id.as_str(), t)))
        .collect();
    // Same borrow of `messages` can't be held while mutating, so decide first.
    let parents: Vec<usize> = messages
        .iter()
        .enumerate()
        .filter(|(_, m)| m.thread_ts.is_none() && threads.contains(&(m.channel_id.as_str(), m.ts_raw.as_str())))
        .map(|(i, _)| i)
        .collect();
    for i in parents {
        messages[i].thread_ts = Some(messages[i].ts_raw.clone());
    }
}

// Slack's search "after:" modifier only has day granularity, so we ask for
// one extra day of margin and then filter precisely by `oldest` ourselves —
// exact regardless of timezone/day-boundary edges.
async fn search_messages_since(
    client: &reqwest::Client,
    token: &str,
    cookie: &str,
    oldest: f64,
    label: &str,
    users: &HashMap<String, String>,
    out: &mut Vec<HistMessage>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let oldest_dt = Local.timestamp_opt(oldest as i64, 0).single().ok_or("bad oldest timestamp")?;
    let after_date = (oldest_dt - ChronoDuration::days(1)).format("%Y-%m-%d").to_string();
    let query = format!("after:{after_date}");

    let mut page = 1u32;
    loop {
        eprint!("\rSearching history since {label}: page {page}, {} message(s) so far...", out.len());
        std::io::stderr().flush().ok();

        let url = format!(
            "https://slack.com/api/search.messages?query={}&count=100&page={page}&sort=timestamp&sort_dir=asc",
            urlencoding::encode(&query),
        );
        let resp = slack_get(client, token, cookie, &url).await?;

        let matches = resp
            .get("messages")
            .and_then(|m| m.get("matches"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if matches.is_empty() {
            break;
        }

        for item in &matches {
            let ts_str = item.get("ts").and_then(Value::as_str).unwrap_or_default();
            let Some(ts) = ts_str.parse::<f64>().ok() else { continue };
            if ts < oldest {
                continue; // outside the exact window — the day-granularity query is coarser
            }
            // A bot post has no `user` and used to be dropped here entirely;
            // message_author falls back to the bot's own name instead.
            let user_id = message_author(item);
            let channel_id = item.get("channel").and_then(|c| c.get("id")).and_then(Value::as_str).unwrap_or_default();
            let text = message_text(item);
            let note = describe_payload(item, users, text.is_empty());
            let thread_ts = item
                .get("thread_ts")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| item.get("permalink").and_then(Value::as_str).and_then(thread_ts_from_permalink));
            out.push(HistMessage {
                ts,
                ts_raw: ts_str.to_string(),
                channel_id: channel_id.to_string(),
                user_id: user_id.to_string(),
                text,
                note,
                thread_ts,
            });
        }

        let page_count = resp
            .get("messages")
            .and_then(|m| m.get("pagination"))
            .and_then(|p| p.get("page_count"))
            .and_then(Value::as_i64)
            .unwrap_or(1);
        if (page as i64) >= page_count {
            break;
        }
        page += 1;
    }

    Ok(())
}

// How often a single request is allowed to wait out a rate limit before giving
// up. Bounded so a limit that isn't clearing can't stall the run indefinitely —
// once exhausted the error propagates, and the backfill keeps whatever it has.
const MAX_RATE_LIMIT_WAITS: u32 = 5;
// Used when a rate limit arrives without a Retry-After we can read.
const FALLBACK_RETRY_AFTER: u64 = 5;
// Slack's suggested wait is honoured, but not blindly: a very long value would
// look like a hang. Capping just means the next attempt may be limited again,
// which the retry budget already accounts for.
const MAX_RETRY_AFTER: u64 = 60;

// Every Slack call goes through here, so this is also where rate limiting is
// handled. A 429 isn't a real failure: the request is well-formed and succeeds
// once the per-method window rolls over, so waiting the stated time and retrying
// is what actually finishes a large backfill.
async fn slack_get(
    client: &reqwest::Client,
    token: &str,
    cookie: &str,
    url: &str,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let mut waits = 0u32;
    loop {
        let response = client
            .get(url)
            .bearer_auth(token)
            .header(reqwest::header::COOKIE, format!("d={cookie}"))
            .send()
            .await?;

        // Read both before `json()` consumes the response. On a 429 the body
        // isn't necessarily JSON, so the status has to be checked first.
        let http_rate_limited = response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS;
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());

        if !http_rate_limited {
            let resp: Value = response.json().await?;
            if resp.get("ok").and_then(Value::as_bool) == Some(true) {
                return Ok(resp);
            }
            let err = resp.get("error").and_then(Value::as_str).unwrap_or("unknown_error");
            // Slack reports the limit in the body on some paths rather than as a
            // 429, so that spelling has to be treated the same way.
            if err != "ratelimited" {
                return Err(format!("Slack API error: {err}").into());
            }
        }

        waits += 1;
        if waits > MAX_RATE_LIMIT_WAITS {
            return Err(format!("Slack API error: ratelimited (gave up after {MAX_RATE_LIMIT_WAITS} waits)").into());
        }
        let secs = retry_after.unwrap_or(FALLBACK_RETRY_AFTER).clamp(1, MAX_RETRY_AFTER);
        // Leading newline so this doesn't land on top of the progress line.
        eprintln!("\nRate limited by Slack — waiting {secs}s ({waits}/{MAX_RATE_LIMIT_WAITS})...");
        tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
    }
}

fn next_cursor(resp: &Value) -> Option<String> {
    resp.get("response_metadata")?
        .get("next_cursor")?
        .as_str()
        .filter(|c| !c.is_empty())
        .map(str::to_string)
}

async fn fetch_users(
    client: &reqwest::Client,
    token: &str,
    cookie: &str,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error + Send + Sync>> {
    let mut users = HashMap::new();
    let mut cursor: Option<String> = None;

    loop {
        let mut url = "https://slack.com/api/users.list?limit=200".to_string();
        if let Some(c) = &cursor {
            url += &format!("&cursor={}", urlencoding::encode(c));
        }

        let resp = slack_get(client, token, cookie, &url).await?;
        let members = resp.get("members").and_then(Value::as_array).cloned().unwrap_or_default();
        if members.is_empty() {
            break;
        }

        for m in &members {
            let id = m.get("id").and_then(Value::as_str).unwrap_or_default();
            if id.is_empty() {
                continue;
            }
            let real_name = m.get("real_name").and_then(Value::as_str).filter(|s| !s.is_empty());
            let name = m.get("name").and_then(Value::as_str);
            let display = real_name.or(name).unwrap_or(id).to_string();
            users.insert(id.to_string(), display);
        }

        cursor = next_cursor(&resp);
        if cursor.is_none() {
            break;
        }
    }

    Ok(users)
}

// Returns rows rather than a name map so the display name and the channel's
// kind stay together: a DM is recorded as a channel of kind 'dm' rather than as
// a separate concept, which keeps every message referencing exactly one channel.
// User groups (@team-qa and the like) are their own kind of id, but they are
// mentioned exactly like people, so they're returned in the same shape and
// merged into the same map. The handle is what Slack displays, not the name:
// "gd-dev-dgk" rather than "Developer digitalklang (GD)".
async fn fetch_user_groups(
    client: &reqwest::Client,
    token: &str,
    cookie: &str,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error + Send + Sync>> {
    let resp = slack_get(client, token, cookie, "https://slack.com/api/usergroups.list").await?;
    let mut groups = HashMap::new();
    for g in resp.get("usergroups").and_then(Value::as_array).unwrap_or(&Vec::new()) {
        let id = g.get("id").and_then(Value::as_str).unwrap_or_default();
        let handle = g
            .get("handle")
            .and_then(Value::as_str)
            .filter(|h| !h.is_empty())
            .or_else(|| g.get("name").and_then(Value::as_str));
        if let (false, Some(handle)) = (id.is_empty(), handle) {
            groups.insert(id.to_string(), handle.to_string());
        }
    }
    Ok(groups)
}

async fn fetch_channels(
    client: &reqwest::Client,
    token: &str,
    cookie: &str,
    users: &HashMap<String, String>,
) -> Result<Vec<ChannelRow>, Box<dyn std::error::Error + Send + Sync>> {
    let mut channels = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let mut url = "https://slack.com/api/conversations.list?types=public_channel,private_channel,im&exclude_archived=true&limit=200".to_string();
        if let Some(c) = &cursor {
            url += &format!("&cursor={}", urlencoding::encode(c));
        }

        let resp = slack_get(client, token, cookie, &url).await?;
        let items = resp.get("channels").and_then(Value::as_array).cloned().unwrap_or_default();
        if items.is_empty() {
            break;
        }

        for item in &items {
            let id = item.get("id").and_then(Value::as_str).unwrap_or_default();
            if id.is_empty() {
                continue;
            }
            let name = item.get("name").and_then(Value::as_str).filter(|s| !s.is_empty());
            let (display, kind) = if let Some(n) = name {
                (format!("#{n}"), "channel")
            } else if let Some(dm_user) = item.get("user").and_then(Value::as_str) {
                let uname = users.get(dm_user).cloned().unwrap_or_else(|| dm_user.to_string());
                (format!("DM: {uname}"), "dm")
            } else {
                (id.to_string(), "unknown")
            };
            channels.push(ChannelRow { id: id.to_string(), name: display, kind });
        }

        cursor = next_cursor(&resp);
        if cursor.is_none() {
            break;
        }
    }

    Ok(channels)
}

// rtm.connect is the officially documented way to get a ready-to-use wss://
// URL (with an embedded, short-lived ticket) for an RTM session. It's the
// simplest option to try first since it reuses the same Bearer+cookie auth
// as every other Slack Web API call. If Slack ever rejects this for
// session tokens, the fallback is wee-slack's approach: resolve team info,
// then build the wss://wss-primary.slack.com/?token=... URL directly.
async fn fetch_rtm_url(
    client: &reqwest::Client,
    token: &str,
    cookie: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let resp: RtmConnectResponse = client
        .post("https://slack.com/api/rtm.connect")
        .bearer_auth(token)
        .header(reqwest::header::COOKIE, format!("d={cookie}"))
        .send()
        .await?
        .json()
        .await?;

    if !resp.ok {
        return Err(format!("rtm.connect failed: {}", resp.error.unwrap_or_default()).into());
    }
    resp.url.ok_or_else(|| "rtm.connect response had no url".into())
}

// Slack timestamps look like "1690000000.000100" (seconds.microseconds).
// Falls back to the current local time if `ts` is missing or unparseable.
fn format_slack_ts(ts: &str) -> String {
    let seconds = ts.parse::<f64>().ok();
    let datetime = seconds.and_then(|s| Local.timestamp_opt(s as i64, 0).single());
    datetime.unwrap_or_else(Local::now).format("%d.%m.%y %H:%M:%S").to_string()
}

// Not every message carries a `user`: a bot post identifies itself with
// `username`/`bot_id` instead, which is why those lines printed as "?". A
// huddle is posted by USLACKBOT, so the person who started it — in
// `room.created_by` — is the useful attribution there rather than "Slackbot".
fn message_author(msg: &Value) -> &str {
    if msg.get("subtype").and_then(Value::as_str) == Some("huddle_thread") {
        if let Some(creator) = msg.get("room").and_then(|r| r.get("created_by")).and_then(Value::as_str) {
            return creator;
        }
    }
    msg.get("user")
        .and_then(Value::as_str)
        .or_else(|| msg.get("username").and_then(Value::as_str))
        .or_else(|| msg.get("bot_id").and_then(Value::as_str))
        .filter(|s| !s.is_empty())
        .unwrap_or("?")
}

// Bot integrations (Jira, Confluence, alerting) post Block Kit payloads and
// leave `text` empty, which is why those lines showed only a name and a colon.
// The body has to be reassembled from the block tree instead.
fn message_text(msg: &Value) -> String {
    let text = msg.get("text").and_then(Value::as_str).unwrap_or_default();
    if !text.is_empty() {
        return text.to_string();
    }
    text_from_blocks(msg).unwrap_or_default()
}

// Leaves within one top-level block are inline runs and get concatenated;
// separate blocks become separate lines.
fn text_from_blocks(msg: &Value) -> Option<String> {
    let blocks = msg.get("blocks")?.as_array()?;
    let mut lines: Vec<String> = Vec::new();
    for block in blocks {
        let mut parts = Vec::new();
        collect_block_text(block, &mut parts);
        if !parts.is_empty() {
            lines.push(parts.concat());
        }
    }
    let joined = lines.join("\n");
    (!joined.trim().is_empty()).then_some(joined)
}

fn collect_block_text(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::Object(map) => {
            // A text leaf is {"type": "mrkdwn"|"plain_text"|"text", "text": "…"};
            // anything else is structure worth descending into.
            let is_text_leaf = matches!(
                map.get("type").and_then(Value::as_str),
                Some("mrkdwn" | "plain_text" | "text")
            );
            if is_text_leaf {
                if let Some(t) = map.get("text").and_then(Value::as_str) {
                    if !t.is_empty() {
                        out.push(t.to_string());
                    }
                    return;
                }
            }
            for child in map.values() {
                collect_block_text(child, out);
            }
        }
        Value::Array(items) => items.iter().for_each(|i| collect_block_text(i, out)),
        _ => {}
    }
}

// Says what a message *is* when its text doesn't. Measured against this
// workspace, an empty text means one of four things: a huddle (by far the most
// common), a link Slack unfurled, an uploaded image, or some other uploaded
// file. Each keeps its substance in a field of its own, and none of it survives
// in `text` — so without this the line showed a name, a colon and nothing else.
//
// An unfurl is only described when there's no text of its own, since the
// message normally repeats the link anyway; an upload is always described,
// because a caption doesn't tell you what was attached.
fn describe_payload(msg: &Value, users: &HashMap<String, String>, text_is_empty: bool) -> Option<String> {
    if msg.get("subtype").and_then(Value::as_str) == Some("huddle_thread") {
        let room = msg.get("room")?;
        let ids: Vec<&str> = room
            .get("participant_history")
            .and_then(Value::as_array)
            .map(|ps| ps.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let named: Vec<String> = ids
            .iter()
            .take(3)
            .map(|id| users.get(*id).cloned().unwrap_or_else(|| id.to_string()))
            .collect();
        let who = match (named.is_empty(), ids.len() - named.len()) {
            (true, _) => "niemand".to_string(),
            (false, 0) => named.join(", "),
            (false, rest) => format!("{} +{rest}", named.join(", ")),
        };
        let start = room.get("date_start").and_then(Value::as_i64).unwrap_or(0);
        let end = room.get("date_end").and_then(Value::as_i64).unwrap_or(0);
        return Some(format!("[Anruf: {who} — {}m]", (end - start).max(0) / 60));
    }

    if let Some(files) = msg.get("files").and_then(Value::as_array).filter(|f| !f.is_empty()) {
        let name = files[0]
            .get("name")
            .or_else(|| files[0].get("title"))
            .and_then(Value::as_str)
            .unwrap_or("Datei");
        let more = match files.len() {
            1 => String::new(),
            n => format!(" +{}", n - 1),
        };
        return Some(format!("[Upload: {name}{more}]"));
    }

    if text_is_empty {
        if let Some(atts) = msg.get("attachments").and_then(Value::as_array).filter(|a| !a.is_empty()) {
            let label = atts[0]
                .get("title")
                .or_else(|| atts[0].get("fallback"))
                .and_then(Value::as_str)
                .unwrap_or("Anhang");
            return Some(format!("[Link: {label}]"));
        }
        return Some("[kein Text]".to_string());
    }

    None
}

// Slack's own thread identity is (channel, thread_ts) — far too long to scan
// by eye. This folds it into 4 base36 characters via FNV-1a, which is stable
// across runs and across the two code paths (live RTM and search backfill),
// so `t:xxxx` in fzf pulls up every message of the same thread. 4 chars is
// ~1.7M buckets: collisions are conceivable but harmless here (worst case a
// filter shows one unrelated thread alongside the intended one).
fn thread_tag(channel_id: &str, thread_ts: &str) -> String {
    const BASE36: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut hash: u32 = 0x811c_9dc5;
    for b in channel_id.bytes().chain(thread_ts.bytes()) {
        hash ^= b as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    let mut out = [0u8; 4];
    for slot in out.iter_mut().rev() {
        *slot = BASE36[(hash % 36) as usize];
        hash /= 36;
    }
    String::from_utf8_lossy(&out).into_owned()
}

// search.messages doesn't return thread_ts as a field, but the permalink of a
// reply carries it as a query parameter:
//   https://team.slack.com/archives/C123/p169...?thread_ts=1690000000.000100&cid=C123
// Messages that aren't in a thread have no such parameter.
fn thread_ts_from_permalink(permalink: &str) -> Option<String> {
    let query = permalink.split('?').nth(1)?;
    query
        .split('&')
        .find_map(|p| p.strip_prefix("thread_ts="))
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

// Resolves Slack's inline reference syntax: <@U123> -> @Name, <#C123|foo> ->
// #foo (or the resolved channel name), <!here>/<!channel>/<!everyone> ->
// @here/@channel/@everyone. Links (<https://...|label>) are left untouched.
fn resolve_mentions(text: &str, channels: &HashMap<String, String>, users: &HashMap<String, String>) -> String {
    let yellow = c(YELLOW);
    let reset = c(RESET);
    let mut result = String::with_capacity(text.len());
    let mut i = 0;
    while i < text.len() {
        if text.as_bytes()[i] == b'<' {
            if let Some(rel_end) = text[i..].find('>') {
                let end = i + rel_end;
                let inner = &text[i + 1..end]; // between '<' and '>'
                let replacement = match inner.as_bytes().first() {
                    Some(b'@') => {
                        let id = inner[1..].split('|').next().unwrap_or(&inner[1..]);
                        let name = users.get(id).cloned().unwrap_or_else(|| id.to_string());
                        Some(format!("{yellow}@{name}{reset}"))
                    }
                    Some(b'#') => {
                        let id = inner[1..].split('|').next().unwrap_or(&inner[1..]);
                        let name = channels.get(id).cloned().unwrap_or_else(|| format!("#{id}"));
                        Some(format!("{yellow}{name}{reset}"))
                    }
                    Some(b'!') => Some(format!("{yellow}@{}{reset}", &inner[1..])), // here/channel/everyone
                    _ => None,
                };
                if let Some(rep) = replacement {
                    result.push_str(&rep);
                    i = end + 1;
                    continue;
                }
            }
        }
        let ch = text[i..].chars().next().unwrap();
        result.push(ch);
        i += ch.len_utf8();
    }
    result
}

// The mentions table needs the ids themselves, not the rendered names, so this
// walks the same <@U123> / <@U123|label> syntax as resolve_mentions but keeps
// the raw id. Deduplicated, because (message_id, user_id) is a primary key and
// people do get mentioned twice in one message.
fn extract_mentions(text: &str) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("<@") {
        let after = &rest[start + 2..];
        let Some(end) = after.find('>') else { break };
        let id = after[..end].split('|').next().unwrap_or_default();
        if !id.is_empty() && !ids.iter().any(|existing| existing == id) {
            ids.push(id.to_string());
        }
        rest = &after[end + 1..];
    }
    ids
}

// Slack escapes these three characters in message text (mainly so literal
// "<"/">" in a human's message can't be confused with the <@...>/<#...>
// syntax) — must run *after* resolve_mentions, which relies on that syntax
// still being intact literal angle brackets at the point it runs.
fn decode_html_entities(text: &str) -> String {
    text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
}

// Replaces :shortcode: with the matching emoji when it's a name we know.
// Unrecognized shortcodes (workspace-custom emoji, typos, skin-tone
// modifiers, etc.) are left exactly as-is rather than guessed at.
fn resolve_emoji(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut i = 0;
    while i < text.len() {
        if text.as_bytes()[i] == b':' {
            if let Some(rel_end) = text[i + 1..].find(':') {
                let end = i + 1 + rel_end;
                let name = &text[i + 1..end];
                let looks_like_shortcode = !name.is_empty()
                    && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '+' || c == '-');
                if looks_like_shortcode {
                    if let Some(emoji) = emoji_for(name) {
                        result.push_str(emoji);
                        i = end + 1;
                        continue;
                    }
                }
            }
        }
        let ch = text[i..].chars().next().unwrap();
        result.push(ch);
        i += ch.len_utf8();
    }
    result
}

// A curated set of the most common Slack emoji shortcodes — not exhaustive
// (no workspace-custom emoji, no skin-tone variants, no every-single-emoji
// coverage), just enough that everyday chat renders instead of showing
// ":shortcode:" text.
fn emoji_for(name: &str) -> Option<&'static str> {
    Some(match name {
        "smile" | "smiley" => "😄",
        "grin" => "😁",
        "grinning" => "😀",
        "laughing" | "satisfied" => "😆",
        "joy" => "😂",
        "rofl" | "rolling_on_the_floor_laughing" => "🤣",
        "blush" => "😊",
        "wink" => "😉",
        "slightly_smiling_face" => "🙂",
        "upside_down_face" => "🙃",
        "relieved" => "😌",
        "heart_eyes" => "😍",
        "kissing_heart" => "😘",
        "yum" => "😋",
        "sunglasses" => "😎",
        "smirk" => "😏",
        "unamused" => "😒",
        "thinking_face" | "thinking" => "🤔",
        "neutral_face" => "😐",
        "expressionless" => "😑",
        "no_mouth" => "😶",
        "roll_eyes" => "🙄",
        "confused" => "😕",
        "worried" => "😟",
        "slightly_frowning_face" => "🙁",
        "frowning_face" => "☹️",
        "open_mouth" => "😮",
        "hushed" => "😯",
        "astonished" => "😲",
        "flushed" => "😳",
        "pleading_face" => "🥺",
        "cry" => "😢",
        "sob" => "😭",
        "disappointed" => "😞",
        "persevere" => "😣",
        "confounded" => "😖",
        "tired_face" => "😫",
        "weary" => "😩",
        "triumph" => "😤",
        "angry" => "😠",
        "rage" => "😡",
        "dizzy_face" => "😵",
        "scream" => "😱",
        "fearful" => "😨",
        "cold_sweat" => "😰",
        "sweat_smile" => "😅",
        "sweat" => "😓",
        "sleepy" => "😪",
        "sleeping" => "😴",
        "zzz" => "💤",
        "mask" => "😷",
        "nauseated_face" => "🤢",
        "sneezing_face" => "🤧",
        "innocent" => "😇",
        "smiling_imp" => "😈",
        "imp" => "👿",
        "japanese_ogre" => "👹",
        "skull" => "💀",
        "ghost" => "👻",
        "alien" => "👽",
        "robot_face" => "🤖",
        "poop" | "hankey" | "shit" => "💩",
        "clown_face" => "🤡",
        "smiley_cat" | "cat" => "🐱",
        "wave" => "👋",
        "raised_hand" | "hand" => "✋",
        "ok_hand" => "👌",
        "thumbsup" | "+1" => "👍",
        "thumbsdown" | "-1" => "👎",
        "fist" | "fist_raised" => "✊",
        "punch" | "fist_oncoming" => "👊",
        "clap" => "👏",
        "raised_hands" => "🙌",
        "pray" => "🙏",
        "muscle" => "💪",
        "point_up" => "☝️",
        "point_down" => "👇",
        "point_left" => "👈",
        "point_right" => "👉",
        "v" => "✌️",
        "crossed_fingers" => "🤞",
        "handshake" => "🤝",
        "heart" => "❤️",
        "orange_heart" => "🧡",
        "yellow_heart" => "💛",
        "green_heart" => "💚",
        "blue_heart" => "💙",
        "purple_heart" => "💜",
        "black_heart" => "🖤",
        "broken_heart" => "💔",
        "two_hearts" => "💕",
        "sparkling_heart" => "💖",
        "heartbeat" => "💓",
        "100" => "💯",
        "fire" => "🔥",
        "star" => "⭐",
        "star2" => "🌟",
        "sparkles" => "✨",
        "tada" => "🎉",
        "confetti_ball" => "🎊",
        "rocket" => "🚀",
        "eyes" => "👀",
        "eye" => "👁️",
        "warning" => "⚠️",
        "exclamation" => "❗",
        "question" => "❓",
        "white_check_mark" | "heavy_check_mark" | "check" => "✅",
        "x" => "❌",
        "heavy_plus_sign" => "➕",
        "heavy_minus_sign" => "➖",
        "zap" => "⚡",
        "boom" | "collision" => "💥",
        "bulb" => "💡",
        "coffee" => "☕",
        "beer" | "beers" => "🍺",
        "pizza" => "🍕",
        "cake" => "🍰",
        "birthday" => "🎂",
        "gift" => "🎁",
        "trophy" => "🏆",
        "medal" | "sports_medal" => "🏅",
        "checkered_flag" => "🏁",
        "bell" => "🔔",
        "no_bell" => "🔕",
        "lock" => "🔒",
        "unlock" => "🔓",
        "key" => "🔑",
        "hourglass" => "⌛",
        "hourglass_flowing_sand" => "⏳",
        "clock1" | "clock" => "🕐",
        "calendar" => "📅",
        "email" | "envelope" => "✉️",
        "phone" => "📞",
        "computer" => "💻",
        "moneybag" => "💰",
        "gem" => "💎",
        "raised_eyebrow" => "🤨",
        "shrug" => "🤷",
        "facepalm" => "🤦",
        "wave_hand" => "👋",
        "loudspeaker" => "📢",
        "mega" => "📣",
        "speech_balloon" => "💬",
        "thought_balloon" => "💭",
        "arrow_right" => "➡️",
        "arrow_left" => "⬅️",
        "arrow_up" => "⬆️",
        "arrow_down" => "⬇️",
        "recycle" => "♻️",
        "new" => "🆕",
        "ok" => "🆗",
        "sos" => "🆘",
        "up" => "🆙",
        "us" | "flag-us" => "🇺🇸",
        "de" | "flag-de" => "🇩🇪",
        _ => return None,
    })
}

fn print_message(
    // The database row id, when there is one. Printed first so a line selected
    // in a pager can be turned back into a message — nothing else in the output
    // identifies it: the channel appears by name, not id, and the timestamp is
    // shown to the second while Slack addresses messages to the microsecond.
    id: Option<i64>,
    ts: &str,
    channel_id: &str,
    user_id: &str,
    text: &str,
    note: Option<&str>,
    thread_ts: Option<&str>,
    channels: &HashMap<String, String>,
    users: &HashMap<String, String>,
) {
    let channel_name = channels.get(channel_id).cloned().unwrap_or_else(|| channel_id.to_string());
    let user_name = users.get(user_id).cloned().unwrap_or_else(|| user_id.to_string());
    let time = format_slack_ts(ts);
    let text = resolve_mentions(text, channels, users);
    let text = decode_html_entities(&text);
    let text = resolve_emoji(&text);
    let (green, blue, cyan, magenta, reset) = (c(GREEN), c(BLUE), c(CYAN), c(MAGENTA), c(RESET));
    let thread = match thread_ts {
        Some(t) => format!(" {magenta}[t:{}]{reset}", thread_tag(channel_id, t)),
        None => String::new(),
    };
    // The label leads the body: for an upload or a call it *is* the body, and
    // where there's text too it says what the text is about. Left uncoloured —
    // the brackets already set it apart, and a dimmed grey reads as too faint
    // against a dark terminal.
    let body = match note {
        Some(n) if text.is_empty() => n.to_string(),
        Some(n) => format!("{n} {text}"),
        None => text,
    };
    let line = format!("{green}[{time}]{reset} {blue}[{channel_name}]{reset}{thread} {cyan}{user_name}{reset}: {body}");
    match id {
        // A message can span several lines, and a pager treats each of them as
        // its own entry — so every line carries the id, not just the first.
        // Otherwise selecting a continuation line resolves against whatever its
        // first word happens to be.
        Some(id) => line.split('\n').for_each(|l| println!("{id} {l}")),
        None => println!("{line}"),
    }
}

fn handle_event(
    event: &Value,
    verbose: bool,
    channels: &HashMap<String, String>,
    users: &HashMap<String, String>,
    store: Option<&Store>,
) {
    let event_type = event.get("type").and_then(Value::as_str).unwrap_or("");

    match event_type {
        "message" => {
            let channel_id = event.get("channel").and_then(Value::as_str).unwrap_or("?");
            let user_id = message_author(event);
            let text = message_text(event);
            let note = describe_payload(event, users, text.is_empty());
            let ts = event.get("ts").and_then(Value::as_str).unwrap_or("");
            // Present on every reply, and on the parent itself once it has
            // replies — so a live thread shows the same tag on both.
            let thread_ts = event.get("thread_ts").and_then(Value::as_str);

            // Stored before printing, because the row id is the first column and
            // only the insert can tell us what it is.
            let mut id = None;
            if let Some(store) = store {
                let tag = thread_ts.map(|t| thread_tag(channel_id, t));
                let row = StoredMessage {
                    channel_id,
                    ts,
                    // "?" is our stand-in for an event with no `user` field, not
                    // a Slack id — better stored as unknown than as a fake user.
                    user_id: (user_id != "?").then_some(user_id),
                    text: &text,
                    note: note.as_deref(),
                    thread_ts,
                    thread_tag: tag.as_deref(),
                    mentions: extract_mentions(&text),
                };
                // A failed write must not take the stream down with it — the
                // message still prints, just without an id to link back to.
                match store.save_message(&row) {
                    Ok(row_id) => id = Some(row_id),
                    Err(e) => eprintln!("sqlite write failed for {channel_id}/{ts}: {e}"),
                }
            }
            print_message(id, ts, channel_id, user_id, &text, note.as_deref(), thread_ts, channels, users);
        }
        // A reaction is its own event type, not a message, which is why these
        // never showed up at all. It carries no text of its own: the emoji is in
        // `reaction` and the message it was left on one level down in `item`.
        //
        // It gets no row of its own — it isn't a message — so the line borrows
        // the id of the message reacted to. That keeps the first column a usable
        // id, and following it leads to the message the reaction is about. With
        // no --sql, or a target that isn't stored, there's no id to borrow.
        "reaction_added" => {
            let item = event.get("item");
            let channel_id = item
                .and_then(|i| i.get("channel"))
                .and_then(Value::as_str)
                .unwrap_or("?");
            let target_ts = item.and_then(|i| i.get("ts")).and_then(Value::as_str).unwrap_or("");
            let user_id = event.get("user").and_then(Value::as_str).unwrap_or("?");
            let ts = event.get("event_ts").and_then(Value::as_str).unwrap_or("");
            let reaction = event.get("reaction").and_then(Value::as_str).unwrap_or("");

            let id = store.and_then(|s| s.message_id_by_ts(channel_id, target_ts).ok().flatten());
            // Wrapped in colons so resolve_emoji renders it like any other.
            let text = format!(":{reaction}:");
            print_message(id, ts, channel_id, user_id, &text, Some("[Reaktion]"), None, channels, users);
        }
        "hello" if verbose => println!("(connected — hello received)"),
        "" => {} // no type field (e.g. reply acknowledgements) — ignore
        other if verbose => println!("({other} event)"),
        _ => {} // non-verbose: only "message" events get printed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // The JSON below is real message shapes from this workspace, taken via
    // conversations.history — which returns the same objects the RTM stream
    // delivers as "message" events, so these cover the live path too.
    fn users() -> HashMap<String, String> {
        [("UD25T8BEW", "Michael"), ("UD0QC5Z8R", "Ole"), ("UD1CGTWUQ", "Johannes")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn a_bot_post_is_attributed_to_the_bot_instead_of_questionmark() {
        let m = json!({"subtype": "bot_message", "text": "hi", "username": "Karma", "bot_id": "BQCJMA6DD"});
        assert_eq!(message_author(&m), "Karma");
    }

    #[test]
    fn a_huddle_is_attributed_to_whoever_started_it_not_to_slackbot() {
        let m = json!({
            "subtype": "huddle_thread", "user": "USLACKBOT", "text": "",
            "room": {"created_by": "UD25T8BEW", "date_start": 1781248493, "date_end": 1781249546,
                     "participant_history": ["UD25T8BEW", "UD0QC5Z8R", "UD1CGTWUQ"]}
        });
        assert_eq!(message_author(&m), "UD25T8BEW");
        assert_eq!(describe_payload(&m, &users(), true).unwrap(), "[Anruf: Michael, Ole, Johannes — 17m]");
    }

    #[test]
    fn a_huddle_with_more_than_three_participants_counts_the_rest() {
        let m = json!({
            "subtype": "huddle_thread", "user": "USLACKBOT", "text": "",
            "room": {"created_by": "UD25T8BEW", "date_start": 0, "date_end": 60,
                     "participant_history": ["UD25T8BEW", "UD0QC5Z8R", "UD1CGTWUQ", "UX1", "UX2"]}
        });
        assert_eq!(describe_payload(&m, &users(), true).unwrap(), "[Anruf: Michael, Ole, Johannes +2 — 1m]");
    }

    #[test]
    fn an_upload_reports_its_filename() {
        let m = json!({"text": "", "upload": true, "user": "U03BN4TDFEC",
                       "files": [{"name": "Bild von iOS.jpg", "mimetype": "image/jpeg"}]});
        assert_eq!(describe_payload(&m, &users(), true).unwrap(), "[Upload: Bild von iOS.jpg]");
    }

    #[test]
    fn several_uploads_count_the_remainder() {
        let m = json!({"text": "", "files": [{"name": "a.mp4"}, {"name": "b.pdf"}]});
        assert_eq!(describe_payload(&m, &users(), true).unwrap(), "[Upload: a.mp4 +1]");
    }

    #[test]
    fn a_caption_does_not_hide_what_was_uploaded() {
        let m = json!({"text": "schau mal", "files": [{"name": "image.png", "mimetype": "image/png"}]});
        assert_eq!(describe_payload(&m, &users(), false).unwrap(), "[Upload: image.png]");
    }

    #[test]
    fn an_unfurl_is_described_only_when_the_message_has_no_text_of_its_own() {
        let empty = json!({"text": "", "attachments": [{"title": "Internetz-TV", "fallback": "YouTube Video"}]});
        assert_eq!(describe_payload(&empty, &users(), true).unwrap(), "[Link: Internetz-TV]");

        let with_text = json!({"text": "<https://youtu.be/x>", "attachments": [{"title": "Internetz-TV"}]});
        assert_eq!(describe_payload(&with_text, &users(), false), None);
    }

    #[test]
    fn ordinary_text_gets_no_label() {
        let m = json!({"text": "hello", "user": "UD25T8BEW"});
        assert_eq!(describe_payload(&m, &users(), false), None);
        assert_eq!(message_author(&m), "UD25T8BEW");
    }

    #[test]
    fn an_empty_message_with_nothing_to_describe_still_says_so() {
        assert_eq!(describe_payload(&json!({"text": ""}), &users(), true).unwrap(), "[kein Text]");
    }

    // Real Jira-integration payload: `text` is empty, the body is in a block.
    #[test]
    fn block_kit_body_is_recovered_when_text_is_empty() {
        let m = json!({"text": "", "username": "jira cloud", "blocks": [
            {"type": "section", "block_id": "no:ih:1", "text": {"type": "mrkdwn",
             "text": "*Marius Exner created a Bug*\n*BCRT-206 Patienten landen*"}}]});
        assert_eq!(message_text(&m), "*Marius Exner created a Bug*\n*BCRT-206 Patienten landen*");
        // With text recovered there is nothing left to label.
        assert_eq!(describe_payload(&m, &users(), false), None);
    }

    #[test]
    fn inline_runs_stay_on_one_line_while_blocks_become_separate_lines() {
        let m = json!({"text": "", "blocks": [
            {"type": "rich_text", "elements": [{"type": "rich_text_section", "elements": [
                {"type": "text", "text": "hello "}, {"type": "text", "text": "world"}]}]},
            {"type": "divider"},
            {"type": "section", "text": {"type": "plain_text", "text": "second"}}]});
        assert_eq!(message_text(&m), "hello world\nsecond");
    }

    #[test]
    fn a_present_text_field_wins_over_blocks() {
        let m = json!({"text": "real", "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": "block"}}]});
        assert_eq!(message_text(&m), "real");
    }

    fn rate_limited() -> String {
        "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nConnection: close\r\nContent-Length: 0\r\n\r\n".to_string()
    }

    fn ok_body() -> String {
        let json = r#"{"ok":true,"value":42}"#;
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{json}",
            json.len()
        )
    }

    // Answers each connection with the next canned response, so a test can lay
    // out the exact sequence a caller should survive.
    async fn serve(responses: Vec<String>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for body in responses {
                let Ok((mut sock, _)) = listener.accept().await else { return };
                let mut buf = [0u8; 2048];
                let _ = sock.read(&mut buf).await; // consume the request first
                let _ = sock.write_all(body.as_bytes()).await;
                let _ = sock.flush().await;
            }
        });
        format!("http://{addr}/api/test")
    }

    #[tokio::test]
    async fn a_rate_limit_is_waited_out_rather_than_returned_as_an_error() {
        let url = serve(vec![rate_limited(), ok_body()]).await;
        let started = std::time::Instant::now();

        let resp = slack_get(&reqwest::Client::new(), "token", "cookie", &url)
            .await
            .expect("should succeed on the retry");

        assert_eq!(resp.get("value").and_then(Value::as_i64), Some(42));
        assert!(
            started.elapsed() >= std::time::Duration::from_secs(1),
            "Retry-After: 1 should have been honoured, took {:?}",
            started.elapsed()
        );
    }

    // The body-level spelling: HTTP 200 with {"ok":false,"error":"ratelimited"}.
    #[tokio::test]
    async fn a_rate_limit_reported_in_the_body_is_treated_the_same() {
        let json = r#"{"ok":false,"error":"ratelimited"}"#;
        let limited = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nRetry-After: 1\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{json}",
            json.len()
        );
        let url = serve(vec![limited, ok_body()]).await;

        let resp = slack_get(&reqwest::Client::new(), "token", "cookie", &url).await.unwrap();
        assert_eq!(resp.get("value").and_then(Value::as_i64), Some(42));
    }

    #[tokio::test]
    async fn the_retry_budget_is_bounded_so_a_persistent_limit_cannot_hang_the_run() {
        // One more 429 than the budget allows.
        let responses = vec![rate_limited(); MAX_RATE_LIMIT_WAITS as usize + 1];
        let url = serve(responses).await;

        let err = slack_get(&reqwest::Client::new(), "token", "cookie", &url)
            .await
            .expect_err("should give up instead of looping forever");
        assert!(err.to_string().contains("ratelimited"), "got: {err}");
    }

    // A genuine API error must not be retried — only rate limits are transient.
    #[tokio::test]
    async fn other_api_errors_fail_immediately() {
        let json = r#"{"ok":false,"error":"invalid_auth"}"#;
        let body = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{json}",
            json.len()
        );
        let url = serve(vec![body]).await;

        let err = slack_get(&reqwest::Client::new(), "token", "cookie", &url)
            .await
            .expect_err("invalid_auth is not retryable");
        assert!(err.to_string().contains("invalid_auth"), "got: {err}");
    }
}
