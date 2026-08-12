# myslackcli

Just a fun little Slack client. Does not use the official API (registered
app, OAuth, the works) — it piggybacks on your own logged-in browser
session instead, the same way Slack's own web client talks to its servers.
This is unofficial, reverse-engineered territory: it can break at any moment
if Slack changes something internally. Use at your own risk, don't rely on
it for anything important.

## Setup

```bash
cp .env.example .env
# fill in SLACK_TOKEN and SLACK_COOKIE below
cargo run
```

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

Both are full session credentials for your own account — don't share them,
don't commit `.env` (it's gitignored already).

## Usage

```bash
./myslackcli                  # live stream of every channel/DM you're in
./myslackcli -t 1h            # also backfill the last hour first, then go live
./myslackcli -v               # show connection/lifecycle chatter too
./myslackcli --no-color       # plain text, no ANSI colors
```

Pipes nicely into a pager or fuzzy-finder:
```bash
./myslackcli | less -RF       # -R for colors, -F to follow like tail -f
./myslackcli | fzf --no-sort --tac --ansi # fuzzy-search the live stream
```
