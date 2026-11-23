// What is unread for the logged-in user, the way the Slack client itself sees it.
//
// None of this is in the public API. `client.counts` and
// `subscriptions.thread.getView` are what the web client calls to draw its bold
// channel names and the Threads view; a browser session token is allowed to call
// them, a bot or app token is not.

use std::collections::HashMap;

use serde_json::Value;

use chrono::TimeZone as _;

use crate::{
    c, describe_payload, format_message, format_slack_ts, message_author, message_text, slack_get, thread_tag,
    DEFAULT_TIME_FORMAT, RESET, YELLOW,
};

#[derive(Debug, PartialEq)]
pub struct UnreadConversation {
    pub id: String,
    // Everything after this ts is unread; conversations.history's `oldest` is
    // exclusive, so it can be passed straight through.
    pub last_read: String,
    pub mention_count: u64,
}

// The order is the one a person would read them in: whatever mentions me first,
// then direct messages before group DMs before channels, because a DM is
// addressed to me even when it doesn't say my name.
pub fn unread_conversations(counts: &Value) -> Vec<UnreadConversation> {
    let mut out = Vec::new();
    for key in ["ims", "mpims", "channels"] {
        let Some(items) = counts.get(key).and_then(Value::as_array) else { continue };
        for item in items {
            if item.get("has_unreads").and_then(Value::as_bool) != Some(true) {
                continue;
            }
            let (Some(id), Some(last_read)) =
                (item.get("id").and_then(Value::as_str), item.get("last_read").and_then(Value::as_str))
            else {
                continue;
            };
            out.push(UnreadConversation {
                id: id.to_string(),
                last_read: last_read.to_string(),
                mention_count: item.get("mention_count").and_then(Value::as_u64).unwrap_or(0),
            });
        }
    }
    // Stable, so the DM/group/channel order survives among equal mention counts.
    out.sort_by_key(|c| std::cmp::Reverse(c.mention_count));
    out
}

// Every conversation's read position, unread or not: --mark-read must not move
// one backwards when --until lies before it.
fn read_positions(counts: &Value) -> HashMap<String, String> {
    ["ims", "mpims", "channels"]
        .iter()
        .filter_map(|key| counts.get(key).and_then(Value::as_array))
        .flatten()
        .filter_map(|item| {
            Some((item.get("id")?.as_str()?.to_string(), item.get("last_read")?.as_str()?.to_string()))
        })
        .collect()
}

// --unread prints the raw id for a conversation the channel list lacks, an
// archived channel for one, so an id has to work without being listed.
fn looks_like_id(s: &str) -> bool {
    s.len() >= 9
        && matches!(s.as_bytes()[0], b'C' | b'D' | b'G')
        && s.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

// A raw id, or every id whose display name is `wanted`. Names need not be
// unique: two people can share a display name, and two group DMs with the
// same members get the same "Group: …" name.
fn conversation_ids(channels: &HashMap<String, String>, wanted: &str) -> Vec<String> {
    if channels.contains_key(wanted) || looks_like_id(wanted) {
        return vec![wanted.to_string()];
    }
    let mut ids: Vec<String> =
        channels.iter().filter(|(_, name)| name.as_str() == wanted).map(|(id, _)| id.clone()).collect();
    ids.sort();
    ids
}

type ThreadRef = (String, String, Vec<String>);

// Unread threads by tag: channel, root ts and the ts of each unread reply. A
// tag is only 4 characters, so two threads can share one; both are kept, and
// --mark-read refuses such a tag instead of picking one of them. The view's
// last page holds read threads too; they are left out, so that they can
// neither make a tag ambiguous nor be found by it.
fn index_threads(view: Vec<Value>) -> HashMap<String, Vec<ThreadRef>> {
    let mut out: HashMap<String, Vec<ThreadRef>> = HashMap::new();
    for t in view {
        let Some(root) = t.get("root_msg") else { continue };
        let (Some(ch), Some(ts)) = (root.get("channel").and_then(Value::as_str), root.get("ts").and_then(Value::as_str))
        else {
            continue;
        };
        let replies: Vec<String> = t
            .get("unread_replies")
            .and_then(Value::as_array)
            .map(|r| r.iter().filter_map(|m| m.get("ts").and_then(Value::as_str).map(str::to_string)).collect())
            .unwrap_or_default();
        if replies.is_empty() {
            continue;
        }
        out.entry(thread_tag(ch, ts)).or_default().push((ch.to_string(), ts.to_string(), replies));
    }
    out
}

// The thread view returns at most ten threads per page, whatever limit says.
const THREAD_VIEW_LIMIT: u32 = 10;
const HISTORY_LIMIT: u32 = 200;
// Printed as the first line of --unread, in the shape --until takes.
// The offset keeps it unambiguous in the hour repeated when the clocks go back.
pub const AS_OF_FORMAT: &str = "%Y-%m-%d %H:%M:%S %:z";

pub async fn print_unread(
    client: &reqwest::Client,
    token: &str,
    cookie: &str,
    channels: &HashMap<String, String>,
    users: &HashMap<String, String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (yellow, reset) = (c(YELLOW), c(RESET));
    let local_before = chrono::Local::now();
    let counts = slack_get(client, token, cookie, "https://slack.com/api/client.counts").await?;
    println!("(as of {} - pass it as --until)", as_of(&counts, local_before).format(AS_OF_FORMAT));
    let conversations = unread_conversations(&counts);

    println!("--- {} conversation(s) with unread messages ---", conversations.len());
    // One conversation failing (left the channel, rate limit after all
    // retries) is no reason to hide the rest, but the list is then incomplete,
    // and the exit code says so.
    let mut failed: Vec<String> = Vec::new();
    for conv in &conversations {
        let name = channels.get(&conv.id).cloned().unwrap_or_else(|| conv.id.clone());
        let mut messages = match fetch_unread_history(client, token, cookie, &conv.id, &conv.last_read).await {
            Ok(h) => h,
            Err(e) => {
                eprintln!("could not load {name}: {e}");
                failed.push(name);
                continue;
            }
        };
        // Reading order is oldest first. Each page comes newest first, but with
        // `oldest` set the pages themselves run forward in time, so the pages
        // can't simply be reversed.
        messages.sort_by(|a, b| {
            let ts = |m: &Value| ts_value(m.get("ts").and_then(Value::as_str).unwrap_or(""));
            ts(a).total_cmp(&ts(b))
        });

        let mentions = match conv.mention_count {
            0 => String::new(),
            n => format!(", {n} mention(s)"),
        };
        println!("{yellow}== {name}: {} unread{mentions} =={reset}", messages.len());
        for m in &messages {
            print_history_message(m, &conv.id, channels, users, false);
        }
    }

    let threads = fetch_thread_view(client, token, cookie).await?;
    let mut shown = 0u64;

    println!("--- threads with unread replies ---");
    for thread in &threads {
        let replies = thread.get("unread_replies").and_then(Value::as_array).cloned().unwrap_or_default();
        if replies.is_empty() {
            continue;
        }
        let Some(root) = thread.get("root_msg") else { continue };
        let channel_id = root.get("channel").and_then(Value::as_str).unwrap_or("?");
        let name = channels.get(channel_id).cloned().unwrap_or_else(|| channel_id.to_string());
        let root_ts = root.get("ts").and_then(Value::as_str).unwrap_or("");
        // The tag in the header is what --mark-read takes, so a thread can be
        // picked from its header line as well as from any of its messages.
        let tag = thread_tag(channel_id, root_ts);
        println!("{yellow}== thread in {name} [t:{tag}]: {} unread =={reset}", replies.len());
        print_history_message(root, channel_id, channels, users, true);
        for r in &replies {
            print_history_message(r, channel_id, channels, users, true);
        }
        shown += replies.len() as u64;
    }
    println!("--- {shown} unread thread repl(ies) ---");
    if !failed.is_empty() {
        return Err(format!("incomplete, could not load: {}", failed.join(", ")).into());
    }
    Ok(())
}

// When the list was taken, for --until: whatever arrives while it is read must
// stay out of a --mark-read run. Slack's own clock, since message timestamps
// are Slack's too and the local clock may be ahead. counts_last_fetched is in
// whole seconds and may be rounded up, so one second comes off. The local time
// before the request is the fallback.
fn as_of(counts: &Value, local_before: chrono::DateTime<chrono::Local>) -> chrono::DateTime<chrono::Local> {
    counts
        .get("counts_last_fetched")
        .and_then(Value::as_i64)
        .and_then(|t| chrono::DateTime::from_timestamp(t - 1, 0))
        .map(|t| t.with_timezone(&chrono::Local))
        .unwrap_or(local_before)
}

// Every message after `last_read`. All of them, with no page cap: with
// `oldest` set, Slack returns the oldest `limit` messages first and the cursor
// moves forward in time (measured 23.11.2026), so whatever a cap left out
// would be the newest messages, and --mark-read would mark them read unseen.
async fn fetch_unread_history(
    client: &reqwest::Client,
    token: &str,
    cookie: &str,
    channel: &str,
    last_read: &str,
) -> Result<Vec<Value>, Box<dyn std::error::Error + Send + Sync>> {
    let mut out = Vec::new();
    let mut cursor = String::new();
    loop {
        let mut url = format!(
            "https://slack.com/api/conversations.history?channel={channel}&oldest={last_read}&limit={HISTORY_LIMIT}"
        );
        if !cursor.is_empty() {
            url += &format!("&cursor={}", urlencoding::encode(&cursor));
        }
        let page = slack_get(client, token, cookie, &url).await?;
        out.extend(page.get("messages").and_then(Value::as_array).cloned().unwrap_or_default());
        cursor = page
            .get("response_metadata")
            .and_then(|m| m.get("next_cursor"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if page.get("has_more").and_then(Value::as_bool) != Some(true) || cursor.is_empty() {
            return Ok(out);
        }
    }
}

// The view lists unread threads first, newest reply first, then the read ones
// (measured 23.11.2026: 20 unread threads on pages 1-2, none after). Reading
// stops at the first page without anything unread; a full walk would take
// minutes, since the view holds every thread ever taken part in (400 pages
// reached back to 2023 and still had more). It pages via `current_ts` = the
// last thread's latest_reply; `max_ts` and `cursor` are accepted but ignored.
// Its `total_unread_replies` only counts the current page.
const THREAD_VIEW_MAX_PAGES: u32 = 60;

async fn fetch_thread_view(
    client: &reqwest::Client,
    token: &str,
    cookie: &str,
) -> Result<Vec<Value>, Box<dyn std::error::Error + Send + Sync>> {
    let mut out = Vec::new();
    let mut current: Option<String> = None;
    for page in 1..=THREAD_VIEW_MAX_PAGES {
        let mut url = format!("https://slack.com/api/subscriptions.thread.getView?limit={THREAD_VIEW_LIMIT}");
        if let Some(ts) = &current {
            url += &format!("&current_ts={ts}");
        }
        let view = slack_get(client, token, cookie, &url).await?;
        let threads = view.get("threads").and_then(Value::as_array).cloned().unwrap_or_default();
        let mut unread = 0;
        current = threads
            .last()
            .and_then(|t| t.get("root_msg"))
            .and_then(|r| r.get("latest_reply"))
            .and_then(Value::as_str)
            .map(str::to_string);
        // Pages overlap at the boundary: the thread whose latest_reply is the
        // cursor comes back as the first one of the next page.
        for t in threads {
            let key = t.get("root_msg").map(|r| (r.get("channel").cloned(), r.get("ts").cloned()));
            if !out.iter().any(|o: &Value| o.get("root_msg").map(|r| (r.get("channel").cloned(), r.get("ts").cloned())) == key) {
                // Counted only here: a page that brings back nothing but the
                // boundary thread from the last one has nothing new unread.
                if t.get("unread_replies").and_then(Value::as_array).is_some_and(|r| !r.is_empty()) {
                    unread += 1;
                }
                out.push(t);
            }
        }
        if unread == 0 || view.get("has_more").and_then(Value::as_bool) != Some(true) || current.is_none() {
            break;
        }
        if page == THREAD_VIEW_MAX_PAGES {
            eprintln!("(stopped after {page} pages of unread threads; older unread threads may be missing)");
        }
    }
    Ok(out)
}

// Messages of the thread section start with this mark. A message in a
// conversation section carries [t:…] too when it has replies, and the mark is
// what tells --mark-read, given just that line, to mark the thread, not the
// conversation. A visible character rather than plain indentation, since
// copying from a terminal or a trimming tool drops leading spaces, and the
// line would then mark the whole conversation.
const THREAD_MARK: &str = "↳";
// Starts the later lines of a multi-line message. They name nothing, and a
// line of text that happens to read "#general" or "t:9x4t" must not become a
// target.
const CONTINUATION_MARK: &str = "┆";

fn print_history_message(
    m: &Value,
    channel_id: &str,
    channels: &HashMap<String, String>,
    users: &HashMap<String, String>,
    in_thread: bool,
) {
    let text = message_text(m);
    let note = describe_payload(m, users, text.is_empty());
    let ts = m.get("ts").and_then(Value::as_str).unwrap_or("");
    let thread_ts = m.get("thread_ts").and_then(Value::as_str);
    let line = format_message(ts, channel_id, message_author(m), &text, note.as_deref(), thread_ts, channels, users);
    for (i, l) in line.split('\n').enumerate() {
        match (in_thread, i) {
            (false, 0) => println!("{l}"),
            (false, _) => println!("  {CONTINUATION_MARK} {l}"),
            (true, 0) => println!("  {THREAD_MARK} {l}"),
            (true, _) => println!("    {CONTINUATION_MARK} {l}"),
        }
    }
}

// ---- marking as read ----

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Target {
    // the short tag from `[t:9x4t]`
    Thread(String),
    // a channel name as --unread prints it (`#general`, `DM: Ada Lovelace`) or a raw id
    Conversation(String),
}

// `t:9x4t`, as --unread prints it inside brackets or as a whole argument.
fn as_thread_tag(s: &str) -> Option<String> {
    let tag = s.strip_prefix("t:")?;
    (tag.len() == 4 && tag.bytes().all(|b| b.is_ascii_digit() || b.is_ascii_lowercase())).then(|| tag.to_string())
}

// Colour stays on in a pipe so fzf --ansi can show it, which means a line
// picked from --unread arrives here with its escape sequences.
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            if chars.next() == Some('[') {
                for p in chars.by_ref() {
                    if ('@'..='~').contains(&p) {
                        break;
                    }
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

// A line of --unread output that names nothing: a blank, a summary, the as-of
// line or the later line of a multi-line message.
pub fn is_filler(line: &str) -> bool {
    let plain = strip_ansi(line);
    let s = plain.trim();
    let s = s.strip_prefix(THREAD_MARK).map_or(s, str::trim_start);
    s.is_empty() || s.starts_with("---") || s.starts_with('(') || s.starts_with(CONTINUATION_MARK)
}

// One argument, or one line of --unread output piped back in: a header
// (`== #general: 3 unread ==`, `== thread in #general [t:9x4t]: 2 unread ==`),
// a message line (`[…] [#general] [t:9x4t] Ada: …`) or a bare name or tag.
// `known` are the conversation names, used to find the channel in a message
// line, whose first bracket may be a timestamp in any --time-format.
pub fn parse_target(input: &str, known: &[&str]) -> Option<Target> {
    let plain = strip_ansi(input);
    let s = plain.trim();
    let (in_thread_section, s) = match s.strip_prefix(THREAD_MARK) {
        Some(rest) => (true, rest.trim_start()),
        None => (false, s),
    };
    if is_filler(s) {
        return None;
    }
    if let Some(tag) = as_thread_tag(s) {
        return Some(Target::Thread(tag));
    }
    if let Some(inner) = s.strip_prefix("== ").and_then(|r| r.strip_suffix(" ==")) {
        let name = inner.rsplit_once(": ").map_or(inner, |(name, _)| name);
        if let Some(thread) = name.strip_prefix("thread in ") {
            let tag = thread.rsplit_once(" [").and_then(|(_, t)| t.strip_suffix(']')).and_then(as_thread_tag);
            return tag.map(Target::Thread);
        }
        return Some(Target::Conversation(name.to_string()));
    }
    // A message line: only the bracket groups before the sender's name count,
    // so a tag or channel quoted in the text cannot pick the target. In the
    // thread section the tag decides, elsewhere the channel.
    if s.starts_with('[') {
        let (mut channel, mut tag, mut rest) = (None, None, s);
        while let Some(after) = rest.strip_prefix('[') {
            let Some(group) = leading_group(after, known) else { break };
            let close = group.len();
            if tag.is_none() {
                tag = as_thread_tag(group);
            }
            if channel.is_none() && (known.contains(&group) || looks_like_id(group)) {
                channel = Some(group);
            }
            rest = after[close + 1..].trim_start();
        }
        return if in_thread_section {
            tag.map(Target::Thread)
        } else {
            channel.map(|c| Target::Conversation(c.to_string()))
        };
    }
    Some(Target::Conversation(s.to_string()))
}

// The content of the bracket group `after` starts, up to its closing bracket.
// Display names and --time-format can hold brackets of their own, so a known
// name is tried first, which also covers an unbalanced one, then the bracket
// that balances the opening one.
fn leading_group<'a>(after: &'a str, known: &[&str]) -> Option<&'a str> {
    if let Some(name) = known
        .iter()
        .filter(|n| after.strip_prefix(**n).is_some_and(|r| r.starts_with(']')))
        .max_by_key(|n| n.len())
    {
        return Some(&after[..name.len()]);
    }
    let mut depth = 0usize;
    for (i, c) in after.char_indices() {
        match c {
            '[' => depth += 1,
            ']' if depth == 0 => return Some(&after[..i]),
            ']' => depth -= 1,
            _ => {}
        }
    }
    None
}

// An empty --time-format drops the stamp from message lines, but in what
// --mark-read reports the stamp is the point: it says what was marked.
fn marked_up_to(ts: &str) -> String {
    let stamp = format_slack_ts(ts);
    if !stamp.is_empty() {
        return stamp;
    }
    chrono::Local
        .timestamp_opt(ts_value(ts) as i64, 0)
        .single()
        .map_or_else(|| ts.to_string(), |t| t.format(DEFAULT_TIME_FORMAT).to_string())
}

fn ts_value(ts: &str) -> f64 {
    ts.parse().unwrap_or(0.0)
}

// Marks each target read up to the newest message at or before `until` (unix
// seconds), so whatever arrived later stays unread. Slack's own client uses the
// same two endpoints: conversations.mark moves a conversation's read position,
// subscriptions.thread.mark that of a thread.
pub async fn mark_read(
    client: &reqwest::Client,
    token: &str,
    cookie: &str,
    channels: &HashMap<String, String>,
    targets: Vec<Target>,
    // Arguments that named no target, already reported; they count as failed.
    not_targets: usize,
    until: f64,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut seen = std::collections::HashSet::new();
    let targets: Vec<Target> = targets.into_iter().filter(|t| seen.insert(t.clone())).collect();
    let verb = if dry_run { "would mark" } else { "marked" };
    let mut failed = not_targets;

    let read_up_to = if targets.iter().any(|t| matches!(t, Target::Conversation(_))) {
        read_positions(&slack_get(client, token, cookie, "https://slack.com/api/client.counts").await?)
    } else {
        HashMap::new()
    };

    // Only unread threads can be found by tag: the tag is a hash, and the
    // thread view is the one place that lists them.
    let threads = if targets.iter().any(|t| matches!(t, Target::Thread(_))) {
        index_threads(fetch_thread_view(client, token, cookie).await?)
    } else {
        HashMap::new()
    };

    for target in &targets {
        match target {
            Target::Thread(tag) => {
                let (ch, root_ts, replies) = match threads.get(tag).map(Vec::as_slice) {
                    None | Some([]) => {
                        eprintln!("t:{tag}: no unread thread with this tag");
                        failed += 1;
                        continue;
                    }
                    Some([one]) => one,
                    Some(many) => {
                        eprintln!("t:{tag}: ambiguous, matches {} unread threads", many.len());
                        failed += 1;
                        continue;
                    }
                };
                let name = channels.get(ch).cloned().unwrap_or_else(|| ch.clone());
                let Some(ts) = replies.iter().filter(|r| ts_value(r) <= until).max_by(|a, b| ts_value(a).total_cmp(&ts_value(b)))
                else {
                    println!("t:{tag} ({name}): nothing unread up to the cut-off");
                    continue;
                };
                if !dry_run {
                    let url = format!(
                        "https://slack.com/api/subscriptions.thread.mark?channel={ch}&thread_ts={root_ts}&ts={ts}&read=1"
                    );
                    if let Err(e) = slack_get(client, token, cookie, &url).await {
                        eprintln!("t:{tag} ({name}): {e}");
                        failed += 1;
                        continue;
                    }
                }
                println!("{verb} read: thread t:{tag} in {name} up to {}", marked_up_to(ts));
            }
            Target::Conversation(wanted) => {
                let id = match conversation_ids(channels, wanted).as_slice() {
                    [] => {
                        eprintln!("{wanted}: no such conversation");
                        failed += 1;
                        continue;
                    }
                    [one] => one.clone(),
                    many => {
                        eprintln!("{wanted}: ambiguous, matches {}", many.join(", "));
                        failed += 1;
                        continue;
                    }
                };
                let name = channels.get(&id).cloned().unwrap_or_else(|| id.clone());
                let url = format!(
                    "https://slack.com/api/conversations.history?channel={id}&latest={until:.6}&inclusive=true&limit=1"
                );
                let ts = match slack_get(client, token, cookie, &url).await {
                    Ok(h) => h
                        .get("messages")
                        .and_then(Value::as_array)
                        .and_then(|m| m.first())
                        .and_then(|m| m.get("ts"))
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    Err(e) => {
                        eprintln!("{name}: {e}");
                        failed += 1;
                        continue;
                    }
                };
                let Some(ts) = ts else {
                    println!("{name}: no message up to the cut-off");
                    continue;
                };
                // client.counts leaves out most old, read conversations; for
                // those the position comes from conversations.info. Without
                // one, nothing is marked: the mark could move it backwards.
                let read = match read_up_to.get(&id) {
                    Some(read) => Some(read.clone()),
                    None => {
                        let url = format!("https://slack.com/api/conversations.info?channel={id}");
                        slack_get(client, token, cookie, &url)
                            .await
                            .ok()
                            .and_then(|i| i.get("channel")?.get("last_read")?.as_str().map(str::to_string))
                    }
                };
                let Some(read) = read else {
                    eprintln!("{name}: read position unknown, not marked");
                    failed += 1;
                    continue;
                };
                if ts_value(&ts) <= ts_value(&read) {
                    println!("{name}: nothing unread up to the cut-off");
                    continue;
                }
                if !dry_run {
                    let url = format!("https://slack.com/api/conversations.mark?channel={id}&ts={ts}");
                    if let Err(e) = slack_get(client, token, cookie, &url).await {
                        eprintln!("{name}: {e}");
                        failed += 1;
                        continue;
                    }
                }
                println!("{verb} read: {name} up to {}", marked_up_to(&ts));
            }
        }
    }
    if failed > 0 {
        return Err(format!("{failed} of {} target(s) could not be marked", targets.len() + not_targets).into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const KNOWN: &[&str] = &["#general", "DM: Ada Lovelace", "Group: ada, grace"];

    #[test]
    fn a_thread_is_found_by_its_tag_where_unread_prints_it() {
        for input in [
            "t:9x4t",
            "== thread in #general [t:9x4t]: 2 unread ==",
            "  ↳ [18.08.26 15:02:43] [#general] [t:9x4t] Ada Lovelace: hi",
            "↳ [18.08.26 15:02:43] [#general] [t:9x4t] Ada Lovelace: hi",
        ] {
            assert_eq!(parse_target(input, KNOWN), Some(Target::Thread("9x4t".into())), "{input}");
        }
    }

    #[test]
    fn a_message_with_replies_in_a_conversation_section_targets_the_conversation() {
        assert_eq!(
            parse_target("[18.08.26 15:02:43] [#general] [t:9x4t] Ada Lovelace: hi", KNOWN),
            Some(Target::Conversation("#general".into()))
        );
    }

    #[test]
    fn a_conversation_is_found_by_header_message_line_or_name() {
        assert_eq!(
            parse_target("== DM: Ada Lovelace: 2 unread, 2 mention(s) ==", KNOWN),
            Some(Target::Conversation("DM: Ada Lovelace".into()))
        );
        assert_eq!(
            parse_target("[18.08.26 15:02:43] [#general] Ada Lovelace: see [this]", KNOWN),
            Some(Target::Conversation("#general".into()))
        );
        assert_eq!(parse_target("#general", KNOWN), Some(Target::Conversation("#general".into())));
    }

    #[test]
    fn a_tag_or_channel_quoted_in_the_text_is_not_a_target() {
        assert_eq!(
            parse_target("[18.08.26 15:02:43] [#general] Ada: see t:abcd and [t:wxyz] now", KNOWN),
            Some(Target::Conversation("#general".into()))
        );
        assert_eq!(parse_target("[18.08.26 15:02:43] [C123] Ada: ask in [#general]", KNOWN), None);
        assert_eq!(parse_target("see t:abcd", KNOWN), Some(Target::Conversation("see t:abcd".into())));
    }

    #[test]
    fn brackets_inside_a_name_or_the_timestamp_do_not_hide_the_target() {
        let known = &["#general", "DM: Ada [Acme]", "DM: Bob ]x"];
        let conv = |name: &str| Some(Target::Conversation(name.into()));
        let thread = Some(Target::Thread("9x4t".into()));
        for (input, want) in [
            ("[18.08.26 15:02:43] [DM: Ada [Acme]] Ada [Acme]: hi", conv("DM: Ada [Acme]")),
            ("  ↳ [18.08.26 15:02:43] [DM: Ada [Acme]] [t:9x4t] Ada [Acme]: hi", thread.clone()),
            ("[18.08.26 15:02:43] [DM: Bob ]x] Bob ]x: hi", conv("DM: Bob ]x")),
            ("  ↳ [18.08.26 15:02:43] [DM: Bob ]x] [t:9x4t] Bob ]x: hi", thread.clone()),
            ("== DM: Ada [Acme]: 2 unread ==", conv("DM: Ada [Acme]")),
            ("== thread in DM: Ada [Acme] [t:9x4t]: 2 unread ==", thread.clone()),
            ("[[15:02]] [#general] Ada: hi", conv("#general")),
            ("  ↳ [[15:02]] [#general] [t:9x4t] Ada: hi", thread.clone()),
            ("[#general] Ada: hi", conv("#general")),
            ("  ↳ [#general] [t:9x4t] Ada: hi", thread.clone()),
            ("[18.08.26 15:02:43] [C123] Ada: ask in [DM: Ada [Acme]]", None),
        ] {
            assert_eq!(parse_target(input, known), want, "{input}");
        }
    }

    #[test]
    fn only_lines_that_name_nothing_are_filler() {
        for line in ["", "   ", "--- threads with unread replies ---", "(as of 2026-11-23 11:45:09 +01:00 - pass it as --until)",
                     "  ┆ #general", "    ┆ t:9x4t", "\x1b[33m--- 2 conversation(s) with unread messages ---\x1b[0m"] {
            assert!(is_filler(line), "{line:?}");
        }
        for line in ["#georgdunkre", "t:ZZZZ", "== #general: 2 unread ==", "[18.08.26 15:02:43] [#gone] Ada: hi",
                     "  ↳ [18.08.26 15:02:43] [#general] Ada: hi"] {
            assert!(!is_filler(line), "{line:?}");
        }
    }

    #[test]
    fn continuation_lines_are_not_targets_whatever_they_say() {
        for input in ["  ┆ #general", "    ┆ t:9x4t", "┆ [#general] Ada: hi", "┆ C0123ABCD"] {
            assert_eq!(parse_target(input, KNOWN), None, "{input}");
        }
    }

    #[test]
    fn coloured_lines_from_a_pipe_are_recognised() {
        assert_eq!(
            parse_target("\x1b[33m== DM: Ada Lovelace: 2 unread, 2 mention(s) ==\x1b[0m", KNOWN),
            Some(Target::Conversation("DM: Ada Lovelace".into()))
        );
        assert_eq!(
            parse_target("\x1b[32m[18.08.26 15:02:43]\x1b[0m \x1b[36m[#general]\x1b[0m \x1b[34mAda\x1b[0m: hi", KNOWN),
            Some(Target::Conversation("#general".into()))
        );
    }

    #[test]
    fn summary_lines_and_text_that_merely_contains_t_colon_are_not_targets() {
        assert_eq!(parse_target("--- 3 unread thread repl(ies) ---", KNOWN), None);
        assert_eq!(parse_target("   (more than 200 unread, older ones not shown)", KNOWN), None);
        assert_eq!(parse_target("(as of 2026-11-23 11:45:09 - pass it as --until)", KNOWN), None);
        assert_eq!(
            parse_target("[18.08.26 15:02:43] [#general] Ada: look at:that", KNOWN),
            Some(Target::Conversation("#general".into()))
        );
    }

    #[test]
    fn only_conversations_with_unreads_are_listed_mentions_first_then_dms_groups_channels() {
        let counts = json!({
            "channels": [
                {"id": "C1", "last_read": "1.0", "mention_count": 0, "has_unreads": true},
                {"id": "C2", "last_read": "2.0", "mention_count": 3, "has_unreads": true},
                {"id": "C3", "last_read": "3.0", "mention_count": 0, "has_unreads": false}
            ],
            "mpims": [{"id": "G1", "last_read": "4.0", "mention_count": 0, "has_unreads": true}],
            "ims": [{"id": "D1", "last_read": "5.0", "mention_count": 0, "has_unreads": true}]
        });
        let ids: Vec<String> = unread_conversations(&counts).into_iter().map(|c| c.id).collect();
        assert_eq!(ids, ["C2", "D1", "G1", "C1"]);
    }

    #[test]
    fn as_of_takes_slacks_clock_one_second_back_and_falls_back_to_the_local_one() {
        let local = chrono::Local::now();
        let at = as_of(&json!({"counts_last_fetched": 1795455113}), local);
        assert_eq!(at.timestamp(), 1795455112);
        assert_eq!(as_of(&json!({}), local), local);
    }

    #[test]
    fn read_positions_cover_read_and_unread_conversations() {
        let counts = json!({
            "channels": [{"id": "C1", "last_read": "1794315645.455429", "has_unreads": false}],
            "mpims": [{"id": "G1", "last_read": "2.0", "has_unreads": true}],
            "ims": [{"id": "D1"}]
        });
        let pos = read_positions(&counts);
        assert_eq!(pos.get("C1").map(String::as_str), Some("1794315645.455429"));
        assert_eq!(pos.get("G1").map(String::as_str), Some("2.0"));
        assert!(!pos.contains_key("D1"));
        assert!(ts_value("1794315645.455428") < ts_value("1794315645.455429"));
    }

    #[test]
    fn an_id_works_even_when_the_channel_list_lacks_it() {
        let channels: HashMap<String, String> = HashMap::new();
        assert_eq!(conversation_ids(&channels, "C0123ABCD"), ["C0123ABCD"]);
        assert!(conversation_ids(&channels, "C123").is_empty());
        assert!(conversation_ids(&channels, "#general").is_empty());
        assert_eq!(
            parse_target("[18.08.26 15:02:43] [C0123ABCD] Ada: hi", KNOWN),
            Some(Target::Conversation("C0123ABCD".into()))
        );
        assert_eq!(parse_target("== C0123ABCD: 2 unread ==", KNOWN), Some(Target::Conversation("C0123ABCD".into())));
    }

    #[test]
    fn a_name_shared_by_several_conversations_yields_all_of_them() {
        let channels: HashMap<String, String> = [("D2", "DM: X"), ("D1", "DM: X"), ("C1", "#general")]
            .into_iter()
            .map(|(id, name)| (id.to_string(), name.to_string()))
            .collect();
        assert_eq!(conversation_ids(&channels, "DM: X"), ["D1", "D2"]);
        assert_eq!(conversation_ids(&channels, "#general"), ["C1"]);
        assert_eq!(conversation_ids(&channels, "D2"), ["D2"]);
        assert!(conversation_ids(&channels, "#nope").is_empty());
    }

    #[test]
    fn unread_threads_sharing_a_tag_are_both_kept_read_ones_left_out() {
        let thread = |ts: &str| json!({"root_msg": {"channel": "C1", "ts": ts}, "unread_replies": [{"ts": "1700000001.000000"}]});
        let read = json!({"root_msg": {"channel": "C1", "ts": "1700000000.019961"}, "unread_replies": []});
        let index = index_threads(vec![
            thread("1700000000.019961"),
            thread("1700000000.024200"),
            thread("1700000000.000001"),
            read,
            json!({"root_msg": {"channel": "C1", "ts": "1700000000.000002"}}),
        ]);
        assert_eq!(index["eir9"].len(), 2);
        assert_eq!(index.values().map(Vec::len).sum::<usize>(), 3);
    }

    #[test]
    fn last_read_is_carried_over_verbatim() {
        let counts = json!({"ims": [{"id": "D1", "last_read": "1794315645.455429", "mention_count": 1, "has_unreads": true}]});
        assert_eq!(
            unread_conversations(&counts),
            [UnreadConversation { id: "D1".into(), last_read: "1794315645.455429".into(), mention_count: 1 }]
        );
    }
}
