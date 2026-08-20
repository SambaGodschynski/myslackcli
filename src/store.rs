use std::collections::HashMap;

use rusqlite::{params, Connection, OptionalExtension};

// Persistence is opt-in via --sql=<file>; nothing in here runs otherwise.
//
// Idempotency comes from normalising around Slack's own identifiers rather than
// from any bookkeeping of our own: a message is uniquely (channel_id, ts), a
// thread is uniquely (channel_id, thread_ts), and users and channels are keyed
// by their Slack id. Every write is an upsert on one of those keys, so running
// overlapping backfills — or restarting the live stream — rewrites the same
// rows instead of appending duplicates.
pub struct Store {
    conn: Connection,
}

// One channel as we want it recorded. DMs get a row here too, with kind 'dm',
// so a message can always reference exactly one channel and queries don't need
// a separate special case for direct messages.
pub struct ChannelRow {
    pub id: String,
    pub name: String,
    pub kind: &'static str,
}

// A message about to be written. `text` is stored exactly as Slack sent it —
// <@U123> references, :emoji: shortcodes and HTML entities all intact. Rendering
// is a lossy view of that: resolving references discards where they were, so a
// resolved column could not reproduce the terminal output, while the raw one
// can. Resolved names remain available by joining users through mentions.
pub struct StoredMessage<'a> {
    pub channel_id: &'a str,
    pub ts: &'a str,
    pub user_id: Option<&'a str>,
    pub text: &'a str,
    pub thread_ts: Option<&'a str>,
    pub thread_tag: Option<&'a str>,
    pub mentions: Vec<String>,
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS users (
    id   TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS channels (
    id   TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL          -- 'channel' | 'dm' | 'unknown'
);

CREATE TABLE IF NOT EXISTS threads (
    id         INTEGER PRIMARY KEY,
    channel_id TEXT NOT NULL REFERENCES channels(id),
    thread_ts  TEXT NOT NULL,   -- Slack's own thread key: the parent's ts
    tag        TEXT NOT NULL,   -- the short id shown in the terminal
    UNIQUE (channel_id, thread_ts)
);

CREATE TABLE IF NOT EXISTS messages (
    id         INTEGER PRIMARY KEY,
    channel_id TEXT    NOT NULL REFERENCES channels(id),
    ts         TEXT    NOT NULL,
    ts_epoch   REAL    NOT NULL, -- ts as a number, for range queries
    user_id    TEXT             REFERENCES users(id),
    thread_id  INTEGER          REFERENCES threads(id),
    text       TEXT    NOT NULL, -- exactly as Slack sent it
    UNIQUE (channel_id, ts)
);

-- Workspace-level facts needed to build links, fetched once per database:
-- the team id for slack:// deep links and the domain for web permalinks.
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mentions (
    message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    user_id    TEXT    NOT NULL REFERENCES users(id),
    PRIMARY KEY (message_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_messages_ts     ON messages (ts_epoch);
CREATE INDEX IF NOT EXISTS idx_messages_thread ON messages (thread_id);
CREATE INDEX IF NOT EXISTS idx_messages_user   ON messages (user_id);
CREATE INDEX IF NOT EXISTS idx_mentions_user   ON mentions (user_id);
";

impl Store {
    pub fn open(path: &str) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        // journal_mode is set explicitly rather than left alone: the mode is
        // stored in the database file, so a file previously opened in WAL would
        // otherwise stay WAL. DELETE keeps the database a single file, with no
        // -wal/-shm sidecars to copy around, and leaves synchronous at its
        // default FULL — in rollback-journal mode anything less risks a corrupt
        // file on power loss, unlike under WAL.
        conn.execute_batch("PRAGMA journal_mode = DELETE; PRAGMA foreign_keys = ON;")?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    // Called once at startup with everything conversations.list/users.list
    // resolved, so later messages can reference real names instead of ids.
    pub fn sync_users(&self, users: &HashMap<String, String>) -> rusqlite::Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO users (id, name) VALUES (?1, ?2)
                 ON CONFLICT(id) DO UPDATE SET name = excluded.name",
            )?;
            for (id, name) in users {
                stmt.execute(params![id, name])?;
            }
        }
        tx.commit()
    }

    pub fn sync_channels(&self, rows: &[ChannelRow]) -> rusqlite::Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO channels (id, name, kind) VALUES (?1, ?2, ?3)
                 ON CONFLICT(id) DO UPDATE SET name = excluded.name, kind = excluded.kind",
            )?;
            for r in rows {
                stmt.execute(params![r.id, r.name, r.kind])?;
            }
        }
        tx.commit()
    }

    // Consumes the store so it can't be used afterwards. rusqlite hands the
    // connection back on failure; only the error is of interest here.
    pub fn close(self) -> rusqlite::Result<()> {
        self.conn.close().map_err(|(_, e)| e)
    }

    pub fn get_meta(&self, key: &str) -> rusqlite::Result<Option<String>> {
        self.conn
            .query_row("SELECT value FROM meta WHERE key = ?1", params![key], |r| r.get(0))
            .optional()
    }

    pub fn set_meta(&self, key: &str, value: &str) -> rusqlite::Result<()> {
        self.conn.execute(
            "INSERT INTO meta (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    // Returns the row id, which the caller prints as the first column so a
    // selected line can be turned back into a link.
    pub fn save_message(&self, m: &StoredMessage) -> rusqlite::Result<i64> {
        let tx = self.conn.unchecked_transaction()?;
        let id = insert_message(&tx, m)?;
        tx.commit()?;
        Ok(id)
    }

    // Backfill inserts the whole window at once; a single commit for the batch
    // rather than one per message is the difference between seconds and minutes.
    // Ids come back in the same order as `msgs`.
    pub fn save_batch(&self, msgs: &[StoredMessage]) -> rusqlite::Result<Vec<i64>> {
        let tx = self.conn.unchecked_transaction()?;
        let mut ids = Vec::with_capacity(msgs.len());
        for m in msgs {
            ids.push(insert_message(&tx, m)?);
        }
        tx.commit()?;
        Ok(ids)
    }

    // The three loaders below feed the offline mode, which renders straight from
    // the database and never talks to Slack. They deliberately return the same
    // shapes the live path builds from the API — id-to-name maps and raw text —
    // so the renderer can be driven identically from either source.
    pub fn load_users(&self) -> rusqlite::Result<HashMap<String, String>> {
        let mut stmt = self.conn.prepare("SELECT id, name FROM users")?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
        rows.collect()
    }

    pub fn load_channels(&self) -> rusqlite::Result<HashMap<String, String>> {
        let mut stmt = self.conn.prepare("SELECT id, name FROM channels")?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
        rows.collect()
    }

    // Ordered by ts_epoch, which is chronological across every channel — the
    // order the messages actually happened in, not the order they were written.
    // A backfill inserts an older window after newer live messages already
    // exist, so insertion order would be wrong. ts breaks ties so the output is
    // stable between runs.
    pub fn load_messages(&self) -> rusqlite::Result<Vec<LocalMessage>> {
        let mut stmt = self.conn.prepare(
            "SELECT m.id, m.ts, m.channel_id, m.user_id, m.text, t.thread_ts
             FROM messages m
             LEFT JOIN threads t ON t.id = m.thread_id
             ORDER BY m.ts_epoch, m.ts",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(LocalMessage {
                id: r.get(0)?,
                ts: r.get(1)?,
                channel_id: r.get(2)?,
                user_id: r.get(3)?,
                text: r.get(4)?,
                thread_ts: r.get(5)?,
            })
        })?;
        rows.collect()
    }

    // Looks up the one message a link is being built for. Returns None when the
    // id doesn't exist, which the caller reports rather than treating as empty.
    pub fn message_by_id(&self, id: i64) -> rusqlite::Result<Option<MessageRef>> {
        self.conn
            .query_row(
                "SELECT m.channel_id, m.ts, t.thread_ts
                 FROM messages m
                 LEFT JOIN threads t ON t.id = m.thread_id
                 WHERE m.id = ?1",
                params![id],
                |r| {
                    Ok(MessageRef {
                        channel_id: r.get(0)?,
                        ts: r.get(1)?,
                        thread_ts: r.get(2)?,
                    })
                },
            )
            .optional()
    }
}

// Just enough of a message to address it: which conversation, which timestamp,
// and the thread it belongs to if any.
pub struct MessageRef {
    pub channel_id: String,
    pub ts: String,
    pub thread_ts: Option<String>,
}

// One stored message, read back for rendering.
pub struct LocalMessage {
    pub id: i64,
    pub ts: String,
    pub channel_id: String,
    pub user_id: Option<String>,
    pub text: String,
    pub thread_ts: Option<String>,
}

fn insert_message(conn: &Connection, m: &StoredMessage) -> rusqlite::Result<i64> {
    // A message can name a channel or user that wasn't in the startup listing —
    // a channel created since, or an id we couldn't resolve. A placeholder row
    // keeps the foreign keys satisfied instead of dropping the message.
    conn.execute(
        "INSERT INTO channels (id, name, kind) VALUES (?1, ?1, 'unknown') ON CONFLICT(id) DO NOTHING",
        params![m.channel_id],
    )?;
    if let Some(u) = m.user_id {
        ensure_user(conn, u)?;
    }

    let thread_id: Option<i64> = match (m.thread_ts, m.thread_tag) {
        (Some(ts), Some(tag)) => Some(conn.query_row(
            "INSERT INTO threads (channel_id, thread_ts, tag) VALUES (?1, ?2, ?3)
             ON CONFLICT(channel_id, thread_ts) DO UPDATE SET tag = excluded.tag
             RETURNING id",
            params![m.channel_id, ts, tag],
            |row| row.get(0),
        )?),
        _ => None,
    };

    // Slack messages can be edited, and a backfill can re-see a message it
    // already stored, so conflicts update the mutable columns rather than being
    // ignored — and RETURNING hands back the row id either way.
    let message_id: i64 = conn.query_row(
        "INSERT INTO messages (channel_id, ts, ts_epoch, user_id, thread_id, text)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(channel_id, ts) DO UPDATE SET
             ts_epoch  = excluded.ts_epoch,
             user_id   = excluded.user_id,
             thread_id = excluded.thread_id,
             text      = excluded.text
         RETURNING id",
        params![
            m.channel_id,
            m.ts,
            m.ts.parse::<f64>().unwrap_or(0.0),
            m.user_id,
            thread_id,
            m.text
        ],
        |row| row.get(0),
    )?;

    // An edit can add or remove mentions, so the set is replaced wholesale
    // rather than accumulated.
    conn.execute("DELETE FROM mentions WHERE message_id = ?1", params![message_id])?;
    for user_id in &m.mentions {
        ensure_user(conn, user_id)?;
        conn.execute(
            "INSERT INTO mentions (message_id, user_id) VALUES (?1, ?2) ON CONFLICT DO NOTHING",
            params![message_id, user_id],
        )?;
    }
    Ok(message_id)
}

// Names come from sync_users; this only guarantees the row exists, so it must
// not clobber a real name with an id.
fn ensure_user(conn: &Connection, id: &str) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO users (id, name) VALUES (?1, ?1) ON CONFLICT(id) DO NOTHING",
        params![id],
    )?;
    Ok(())
}
