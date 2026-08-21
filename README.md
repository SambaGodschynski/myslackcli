# myslackcli

Just a fun little Slack client. Does not use the official API (registered
app, OAuth, the works) — it piggybacks on your own logged-in browser
session instead, the same way Slack's own web client talks to its servers.
This is unofficial, reverse-engineered territory: it can break at any moment
if Slack changes something internally. Use at your own risk, don't rely on
it for anything important.

## Setup

Credentials live outside the checkout, so they can't be committed by accident:

```bash
mkdir -p ~/.config/myslackcli
cp .env.example ~/.config/myslackcli/.env
chmod 600 ~/.config/myslackcli/.env
# fill in SLACK_TOKEN and SLACK_COOKIE below
cargo run
```

`--env <file>` points at a different file if you'd rather keep it elsewhere.

## Getting a token & cookie

1. Log into `app.slack.com` in your browser, open the workspace you want.
2. Open DevTools (F12) → **Console** tab, run:
   ```js
   JSON.parse(localStorage.localConfig_v2).teams[Object.keys(JSON.parse(localStorage.localConfig_v2).teams)[0]].token
   ```
   That's your `SLACK_TOKEN` (starts with `xoxc-`).
3. DevTools → **Application** tab → **Cookies** → `https://app.slack.com` →
   find the cookie named **`d`** → copy its value. That's your
   `SLACK_COOKIE` (starts with `xoxd-`).

Both are full session credentials for your own account — don't share them, and
keep the file out of the checkout (see Setup).

## Usage

```bash
./myslackcli                  # live stream of every channel/DM you're in
./myslackcli -t 1h            # --time: backfill the last hour first, then go live
./myslackcli -v               # --verbose: connection/lifecycle chatter too
./myslackcli --no-color       # plain text, no ANSI colors
./myslackcli --help           # all flags
```

Mentions come out as names rather than ids: `@Ada Lovelace` for a person,
`@team-qa` for a user group, `#general` for a channel reference.

Messages that belong to a thread carry a short id like `[t:9x4t]` — the same one
on every message of that thread, so filtering on `t:9x4t` pulls the whole
conversation out of the stream.

Not every message is text. Slack leaves `text` empty for uploads, huddles and
link unfurls and puts the substance in a field of its own, so those lines used
to show nothing but a name and a colon. They now say what they are:

```
[18.08.26 15:02:43] [#general] Ada Lovelace: [Upload: diagram.png +2] have a look
[18.08.26 15:10:11] [#general] Ada Lovelace: [Call: Ada, Grace, Alan — 17m]
[18.08.26 15:11:02] [#general] Grace Hopper: [Link: Improbable Research #129]
```

A bot post names itself instead of carrying a user id, so it's attributed to the
bot rather than to `?`, and integrations that post Block Kit (Jira, Confluence)
get their text reassembled from the blocks. Reactions appear live as
`[Reaction] 👍`; they aren't messages, so they aren't recorded.

Pipes nicely into a fuzzy-finder:
```bash
./myslackcli | fzf --no-sort --tac --ansi
```

## Recording to sqlite

`--sql=<file>` stores every message, live and backfilled, in a sqlite file.
`--local-no-sync` then reads that file instead of the server.

```bash
./myslackcli -t 3d --sql=slack.db            # stream and record
./myslackcli --sql=slack.db --local-no-sync   # read the file instead of the server
```

## Loading older history

`-t` counts back from now, so reaching further means re-fetching everything you
already have. `--before` moves the newer edge of the window instead, which is
how you pull in older chunks:

```bash
./myslackcli --before=2026-05-13 -t 100d --sql=slack.db   # days 100–200
./myslackcli --before=2026-02-02 -t 100d --sql=slack.db   # days 200–300
```

`--oldest` says how far back the file already reaches, which is where the next
chunk should end:

```bash
./myslackcli --sql=slack.db --oldest
1 [20.05.26 11:51:40] [#support] [t:sw8t] Ada Lovelace: …
```

Expect to be rate limited on windows this size. Slack answers with how long to
wait and the run waits it out — `Rate limited by Slack — waiting 30s (1/5)` is
progress, not a hang. If the limit outlasts that budget, the load stops early,
keeps and stores what it already fetched, and says how much that was.

The date is that newer edge, and it's exclusive — local midnight of the day you
name. So using one chunk's oldest day as the next `--before` neither overlaps
nor leaves a gap. A `--before` run also exits once the window is loaded instead
of dropping into the live stream, so chunks can be chained in a script.

## Jumping back into Slack

With `--sql`, every line starts with the message's database id:

```
4711 [18.08.26 10:22:32] [#general] [t:9x4t] Ada Lovelace: see the thread
```

Hand that id back and you get a link to the message — a reply links into its
thread, not just the channel:

```bash
./myslackcli --sql=slack.db --to-app-url=4711   # slack://…  opens the desktop app
./myslackcli --sql=slack.db --to-web-url=4711   # https://…  permalink for sharing
```

Both only read the database. The workspace details they need are fetched on the
first run with `--sql` and kept in the file, so the links work offline after
that.

## fzf integration

Putting those pieces together: a fuzzy-searchable Slack where Enter opens the
selected message in the desktop app and Ctrl-Y copies its permalink.

```bash
X_SLACK_BIN=/path/to/myslackcli/target/release/myslackcli
X_SLACK_DB=~/slack/db.sqlite

x_slack_fzf()
{
  fzf --no-sort --tac --ansi --wrap=word --style minimal \
      --with-nth=2.. \
      --bind "enter:execute-silent(xdg-open \"\$($X_SLACK_BIN --sql $X_SLACK_DB --to-app-url={1})\")" \
      --bind "ctrl-y:execute-silent($X_SLACK_BIN --sql $X_SLACK_DB --to-web-url={1} | xclip -selection clipboard)"
}

# last 2h of history, then keep streaming live
x_slack()         { $X_SLACK_BIN -t "${1:-2h}" --sql $X_SLACK_DB | x_slack_fzf; }

# everything ever recorded, straight from the database
x_slack_history() { $X_SLACK_BIN --sql $X_SLACK_DB --local-no-sync | x_slack_fzf; }
```

Two details in there are easy to get wrong:

- **`--with-nth=2..`** hides the id column from the list *and* from the search,
  so the ids don't pollute your queries. `{1}` still resolves against the
  original line, so the bindings can use the id you can't see.
- **`execute-silent`, not `become`.** `become` *replaces* the fzf process, so
  the session would end the moment you open the first message. The tradeoff is
  that Enter no longer accepts, so fzf prints nothing on stdout — bind a third
  key to `accept` if you want that.
