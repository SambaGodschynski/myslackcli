# myslackcli

Standalone Slack CLI client — connects to Slack's real-time message stream
(the same internal WebSocket mechanism the official web client uses) instead
of polling the REST API, so messages show up in the terminal as they happen.

## Setup

```bash
cp .env.example .env
# fill in SLACK_TOKEN (xoxc-...) and SLACK_COOKIE (xoxd-..., the "d" cookie)
cargo run
```

See `../parade/README.md` ("Messenger POC") for how to grab a browser
session token/cookie from your own logged-in `app.slack.com` session via
DevTools — same credentials work here.

## How it connects

Calls the officially documented `rtm.connect` Web API method (same
Bearer-token + cookie auth as any other Slack Web API call) to get a
short-lived `wss://` URL, then opens a WebSocket to it and prints every
`message` event as `[channel] user: text`.

`rtm.connect`/RTM is marked legacy for new OAuth app registrations (Slack
pushes those toward the Events API instead), but it's what the official
clients still use internally for session-token connections — this is
unofficial/reverse-engineered territory, not a documented integration path,
so it can break without notice. If `rtm.connect` ever stops accepting
session tokens, the fallback is the approach used by
[wee-slack](https://github.com/wee-slack/wee-slack): resolve team info, then
connect directly to `wss://wss-primary.slack.com/?token=...`.

## Status

First vertical slice: connect, stream raw `message` events for every
channel/DM the token can see, print channel/user IDs as-is (no name
resolution yet).
