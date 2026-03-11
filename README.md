# dydx-project-xray — Mediamark → DYDX Integration

Automatically syncs cards from the **Mediamark Workflow Support** board (Pipefy) to the **DYDX Development Tasks Management** board (Pipefy) in real time.

---

## How it works

When a card is created or moves on the Mediamark board, this service:

1. Receives a webhook from Pipefy (or detects the change via background polling)
2. Reads the card fields — request type, description, assignee, priority, due date, etc.
3. Creates or updates the corresponding card on the DYDX board
4. Maps the Mediamark phase to the correct DYDX phase
5. Logs all activity to the console and SolarWinds (Papertrail)

All processing happens in the background — Pipefy receives an immediate `200 OK` so it never retries.

---

## Project structure

```
├── integrations/
│   └── mediamark/
│       ├── app_mediamark.py          # Flask webhook server (entry point)
│       ├── sync_to_dydx.py           # Core sync engine
│       ├── card_listener.py          # Background polling fallback
│       ├── field_mappings.py         # Field ID constants & mapping tables
│       ├── move_mediamark_card.py    # Utility: move a card to a phase manually
│       └── test_connection_mediamark.py  # Verify API connectivity
├── utils/
│   └── pipefy_client.py              # Pipefy GraphQL client (shared)
├── gunicorn_config.py                # Production server config
├── requirements.txt
├── test_logging.py                   # Verify SolarWinds/Papertrail connection
└── test_phase_movements.py           # Test phase mapping end-to-end
```

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/KatekaniN/dydx-project-xray.git
cd dydx-project-xray
```

### 2. Create a virtual environment and install dependencies

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Mac/Linux:
source .venv/bin/activate

pip install -r requirements.txt
```

### 3. Create the environment file

Create `integrations/mediamark/.env.mediamark` — this file is gitignored and must never be committed.

```env
# Pipefy API keys
MEDIAMARK_API_KEY=your_mediamark_pipefy_token
DYDX_API_KEY=your_dydx_pipefy_token

# Board (pipe) IDs
MEDIAMARK_SUPPORT_BOARD_PIPE_ID=301684738
DYDX_PIPE_ID=306286881

# Card listener polling interval (seconds)
MM_LISTENER_POLL_INTERVAL=10

# Set to false to disable background polling (webhooks only)
ENABLE_CARD_LISTENER=true

# SolarWinds / Papertrail logging
SOLARWINDS_LOG_URL=https://logs.collector.solarwinds.com/v1/log
SOLARWINDS_TOKEN=your_solarwinds_token
```

---

## Running the server

### Development

```bash
python integrations/mediamark/app_mediamark.py
```

Server starts on `http://localhost:5001`.

### Production (EC2 / Linux)

```bash
gunicorn -c gunicorn_config.py integrations.mediamark.app_mediamark:app
```

---

## Configuring the Pipefy webhook

In the Mediamark Pipefy board settings, add a webhook pointing to:

```
http://<your-server-ip>:8472/mediamark/events
```

Events to enable: `card.create`, `card.move`, `card.field_update`, `card.done`

---

## API endpoints

| Method | Endpoint                | Description                                    |
| ------ | ----------------------- | ---------------------------------------------- |
| GET    | `/health`               | Health check — returns `{"status": "healthy"}` |
| GET    | `/`                     | Service info                                   |
| POST   | `/mediamark/events`     | Main Pipefy webhook receiver                   |
| POST   | `/webhook/test`         | Manually trigger a sync (for testing)          |
| GET    | `/status`               | List the 20 most recent background jobs        |
| GET    | `/status/<job_id>`      | Check the status of a specific job             |
| GET    | `/webhook/debug`        | Accepts any POST and logs it                   |
| GET    | `/debug/card/<card_id>` | Inspect fields extracted from a Mediamark card |

### Manually trigger a sync (`/webhook/test`)

```json
POST http://localhost:5001/webhook/test
Content-Type: application/json

{
  "board_type": "support",
  "card_id": "1309710645",
  "action": "card.create",
  "current_phase": "NEW"
}
```

`board_type` accepts `support` or `change_request`.

### Check job status (`/status/<job_id>`)

Every webhook or test trigger returns a `job_id`. Poll it to see progress:

```
GET http://localhost:5001/status/a3f2c1d0-...
```

Response:

```json
{
  "job_id": "a3f2c1d0-...",
  "status": "completed",
  "card_id": "1309710645",
  "action": "card.create",
  "started_at": "2026-03-11T10:00:00Z",
  "finished_at": "2026-03-11T10:00:03Z",
  "result": { ... },
  "error": null
}
```

`status` is one of: `running` | `completed` | `failed`

---

## Card title naming convention

The DYDX card title is built as `[{Request Type}] {Description}` where the prefix is the exact request type name from the Mediamark card:

| Mediamark request type          | Example DYDX title                                     |
| ------------------------------- | ------------------------------------------------------ |
| Issue/Question                  | `[Issue/Question] Login page is not loading`           |
| Feature/Change Request          | `[Feature/Change Request] Add export to CSV`           |
| Access To System (For Yourself) | `[Access To System (For Yourself)] Jane Doe - Finance` |
| New User Request                | `[New User Request] Card title`                        |

---

## Phase mapping

| Mediamark phase        | DYDX phase      |
| ---------------------- | --------------- |
| New                    | Backlog         |
| Review                 | In Progress     |
| Escalated              | In Progress     |
| SOW and Scoping        | In Progress     |
| Client Approval        | In Progress     |
| Backlog                | Backlog         |
| In Progress            | In Progress     |
| Comms to Client        | Testing / Comms |
| Resolved               | Done            |
| Not Approved           | Cancelled       |
| Change Request On Hold | Backlog         |

---

## Background card listener

In addition to webhooks, a polling listener runs every 10 seconds (configurable via `MM_LISTENER_POLL_INTERVAL`) as a safety net. It:

- Fetches all active cards from the Mediamark board
- Computes a hash of each card's key fields
- Triggers a sync if anything changed since the last poll
- Triggers a final sync if a card disappears (moved to Done)

Disable it by setting `ENABLE_CARD_LISTENER=false` in the env file.

---

## Logging

All sync activity is logged to:

- **Console** — always on, useful for local development
- **SolarWinds / Papertrail** — when `SOLARWINDS_LOG_URL` and `SOLARWINDS_TOKEN` are set

Each log line includes a timestamp and the module name, e.g.:

```
2026-03-11T10:00:01 [integrations.mediamark.sync_to_dydx] INFO Created DYDX card for card 1309710645
```

Test the Papertrail connection:

```bash
python test_logging.py
```
