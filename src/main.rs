use std::collections::HashMap;

use chrono::{Local, TimeZone};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::COOKIE;

#[derive(Debug, Deserialize)]
struct RtmConnectResponse {
    ok: bool,
    error: Option<String>,
    url: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let verbose = std::env::args().any(|a| a == "--verbose" || a == "-v");

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

async fn slack_get(
    client: &reqwest::Client,
    token: &str,
    cookie: &str,
    url: &str,
) -> Result<Value, Box<dyn std::error::Error>> {
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
) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
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
) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
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
) -> Result<String, Box<dyn std::error::Error>> {
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
    datetime.unwrap_or_else(Local::now).format("%H:%M:%S").to_string()
}

fn handle_event(event: &Value, verbose: bool, channels: &HashMap<String, String>, users: &HashMap<String, String>) {
    let event_type = event.get("type").and_then(Value::as_str).unwrap_or("");

    match event_type {
        "message" => {
            let channel_id = event.get("channel").and_then(Value::as_str).unwrap_or("?");
            let user_id = event.get("user").and_then(Value::as_str).unwrap_or("?");
            let text = event.get("text").and_then(Value::as_str).unwrap_or("");
            let ts = event.get("ts").and_then(Value::as_str).unwrap_or("");

            let channel_name = channels.get(channel_id).cloned().unwrap_or_else(|| channel_id.to_string());
            let user_name = users.get(user_id).cloned().unwrap_or_else(|| user_id.to_string());
            let time = format_slack_ts(ts);

            println!("[{time}] [{channel_name}] {user_name}: {text}");
        }
        "hello" if verbose => println!("(connected — hello received)"),
        "" => {} // no type field (e.g. reply acknowledgements) — ignore
        other if verbose => println!("({other} event)"),
        _ => {} // non-verbose: only "message" events get printed
    }
}
