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
    thread_ts: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut verbose = false;
    let mut no_color = false;
    let mut backfill_arg: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "-v" | "--verbose" => verbose = true,
            "--no-color" => no_color = true,
            "-t" | "--time" => {
                backfill_arg = Some(args.next().ok_or("missing value for -t (e.g. -t 1h)")?);
            }
            _ => {}
        }
    }
    COLOR_ENABLED.set(!no_color).ok();

    dotenvy::dotenv().ok(); // fine if there's no .env — vars may already be set in the environment

    let token = std::env::var("SLACK_TOKEN").map_err(|_| "SLACK_TOKEN not set (see .env.example)")?;
    let cookie = std::env::var("SLACK_COOKIE").map_err(|_| "SLACK_COOKIE not set (see .env.example)")?;

    let client = reqwest::Client::new();

    if verbose {
        println!("Resolving users/channels...");
    }
    let users = fetch_users(&client, &token, &cookie).await?;
    let channels = fetch_channels(&client, &token, &cookie, &users).await?;
    if verbose {
        println!("{} user(s), {} channel(s)/DM(s) resolved", users.len(), channels.len());
    }

    if let Some(t_str) = &backfill_arg {
        let duration = parse_duration(t_str).ok_or_else(|| format!("invalid -t value: {t_str:?} (try e.g. 1h, 30m, 2d)"))?;
        backfill(&client, &token, &cookie, &channels, &users, duration, t_str).await?;
    }

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

    while let Some(msg) = read.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                eprintln!("WebSocket error: {e}");
                break;
            }
        };

        let Ok(text) = msg.to_text() else { continue };
        let Ok(event) = serde_json::from_str::<Value>(text) else { continue };

        handle_event(&event, verbose, &channels, &users);
    }

    Ok(())
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let oldest = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .checked_sub(duration)
        .ok_or("duration too large")?
        .as_secs_f64();

    let mut all_messages = search_messages_since(client, token, cookie, oldest, label).await?;
    eprintln!(); // finish the progress line

    all_messages.sort_by(|a, b| a.ts.partial_cmp(&b.ts).unwrap());
    tag_thread_parents(&mut all_messages);

    println!("--- {} historical message(s) since {label} ---", all_messages.len());
    for m in &all_messages {
        print_message(
            &m.ts_raw,
            &m.channel_id,
            &m.user_id,
            &m.text,
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
) -> Result<Vec<HistMessage>, Box<dyn std::error::Error + Send + Sync>> {
    let oldest_dt = Local.timestamp_opt(oldest as i64, 0).single().ok_or("bad oldest timestamp")?;
    let after_date = (oldest_dt - ChronoDuration::days(1)).format("%Y-%m-%d").to_string();
    let query = format!("after:{after_date}");

    let mut messages = Vec::new();
    let mut page = 1u32;
    loop {
        eprint!("\rSearching history since {label}: page {page}, {} message(s) so far...", messages.len());
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
            let user_id = item.get("user").and_then(Value::as_str).unwrap_or_default();
            if user_id.is_empty() {
                continue;
            }
            let channel_id = item.get("channel").and_then(|c| c.get("id")).and_then(Value::as_str).unwrap_or_default();
            let text = item.get("text").and_then(Value::as_str).unwrap_or_default().to_string();
            let thread_ts = item
                .get("thread_ts")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| item.get("permalink").and_then(Value::as_str).and_then(thread_ts_from_permalink));
            messages.push(HistMessage {
                ts,
                ts_raw: ts_str.to_string(),
                channel_id: channel_id.to_string(),
                user_id: user_id.to_string(),
                text,
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

    Ok(messages)
}

async fn slack_get(
    client: &reqwest::Client,
    token: &str,
    cookie: &str,
    url: &str,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let resp: Value = client
        .get(url)
        .bearer_auth(token)
        .header(reqwest::header::COOKIE, format!("d={cookie}"))
        .send()
        .await?
        .json()
        .await?;

    if resp.get("ok").and_then(Value::as_bool) != Some(true) {
        let err = resp.get("error").and_then(Value::as_str).unwrap_or("unknown_error");
        return Err(format!("Slack API error: {err}").into());
    }
    Ok(resp)
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

async fn fetch_channels(
    client: &reqwest::Client,
    token: &str,
    cookie: &str,
    users: &HashMap<String, String>,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error + Send + Sync>> {
    let mut channels = HashMap::new();
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
            let display = if let Some(n) = name {
                format!("#{n}")
            } else if let Some(dm_user) = item.get("user").and_then(Value::as_str) {
                let uname = users.get(dm_user).cloned().unwrap_or_else(|| dm_user.to_string());
                format!("DM: {uname}")
            } else {
                id.to_string()
            };
            channels.insert(id.to_string(), display);
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
    ts: &str,
    channel_id: &str,
    user_id: &str,
    text: &str,
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
    println!("{green}[{time}]{reset} {blue}[{channel_name}]{reset}{thread} {cyan}{user_name}{reset}: {text}");
}

fn handle_event(event: &Value, verbose: bool, channels: &HashMap<String, String>, users: &HashMap<String, String>) {
    let event_type = event.get("type").and_then(Value::as_str).unwrap_or("");

    match event_type {
        "message" => {
            let channel_id = event.get("channel").and_then(Value::as_str).unwrap_or("?");
            let user_id = event.get("user").and_then(Value::as_str).unwrap_or("?");
            let text = event.get("text").and_then(Value::as_str).unwrap_or("");
            let ts = event.get("ts").and_then(Value::as_str).unwrap_or("");
            // Present on every reply, and on the parent itself once it has
            // replies — so a live thread shows the same tag on both.
            let thread_ts = event.get("thread_ts").and_then(Value::as_str);
            print_message(ts, channel_id, user_id, text, thread_ts, channels, users);
        }
        "hello" if verbose => println!("(connected — hello received)"),
        "" => {} // no type field (e.g. reply acknowledgements) — ignore
        other if verbose => println!("({other} event)"),
        _ => {} // non-verbose: only "message" events get printed
    }
}
