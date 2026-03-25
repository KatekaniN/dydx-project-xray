# Mediamark → DYDX Integration — Manual Test Sheet

> **Before starting:** Deploy the latest code to the EC2 server and confirm the service is running (`GET /health`).  
> Wait **at least 10 seconds** between steps to allow the card listener poll to fire.  
> Mark each test ✅ Pass, ❌ Fail, or ⚠️ Partial — note the actual outcome in the "Result" column.

---

## Setup

| Item | Value |
|------|-------|
| Mediamark board | [Workflow Support pipe](https://app.pipefy.com) |
| DYDX board | [Dev Tasks Management](https://app.pipefy.com) |
| Test assignees | At least 2 people who are DYDX board members (e.g. Abigail, Katekani, Jason) |
| Papertrail logs | Use to verify what the server actually did |

---

## Section 1 — Card Creation

| # | Action | Expected Result | Result | Notes |
|---|--------|-----------------|--------|-------|
| 1.1 | Create a new MM card with **1 assignee** set in the `responsible_support` field | 1 DYDX card created in **Backlog** for that assignee | | |
| 1.2 | Create a new MM card with **2 different assignees** across `responsible_support` and `responsible_1` | 2 DYDX cards created in **Backlog**, one per assignee | | |
| 1.3 | Create a new MM card with **the same person** in both `responsible_support` and `responsible_1` | Only **1** DYDX card created (no duplicate) | | |
| 1.4 | Create a new MM card with **no assignees** set | Either 1 DYDX card for the fallback assignee (`co-creation.support@dydx.digital`), or no card created with a warning in logs | | |
| 1.5 | Open a newly created DYDX card and verify its title format | Title should be `[Request Type] Description - Assignee Name` | | |
| 1.6 | Verify the DYDX card's fields | `main_task_id` = MM card ID, `client_name` = "Mediamark", `assignee` = correct DYDX user, `due_date` populated, `task_description` populated | | |

---

## Section 2 — Phase Transitions (card.move)

Use a single MM card with 1 assignee. Move it through each phase and confirm the DYDX card follows.

| # | Move MM card TO | Expected DYDX Phase | Result | Notes |
|---|-----------------|---------------------|--------|-------|
| 2.1 | **BACKLOG** | DYDX card stays in / moves to **Backlog** | | |
| 2.2 | **NEW** | DYDX card moves to **Backlog** | | |
| 2.3 | **REVIEW** | DYDX card moves to **In Progress** | | |
| 2.4 | **IN PROGRESS** | DYDX card stays in **In Progress** | | |
| 2.5 | **ESCALATED** | DYDX card stays in **In Progress** (card created if missing, but not force-moved) | | |
| 2.6 | **SOW AND SCOPING** | DYDX card moves to **In Progress** | | |
| 2.7 | **COMMS TO CLIENT** | DYDX card moves to **Testing / Comms** | | |
| 2.8 | **CHANGE REQUEST ON HOLD** | DYDX card moves to **Backlog** AND gets an **"On Hold" label** | | |
| 2.9 | **RESOLVED** | DYDX card moves to **Done** | | |
| 2.10 | **NOT APPROVED** | DYDX card moves to **Cancelled** (NOT Done) | | |

---

## Section 3 — Assignee Management (Add / Remove)

| # | Action | Expected Result | Result | Notes |
|---|--------|-----------------|--------|-------|
| 3.1 | On an existing MM card, **add a new person** to the `responsible_support` field | A new DYDX card created for that person within ~10s (listener detects it) | | |
| 3.2 | On an existing MM card, **add a new person** to the card-level assignees (top-left avatars) | A new DYDX card created for that person within ~10s | | |
| 3.3 | On an existing MM card, **add a new person** to `responsible_comms_to_client` | A new DYDX card created for that person | | |
| 3.4 | The same person is in `responsible_support` + `responsible_1` — **remove them from `responsible_support` only** | Cascade: person also removed from `responsible_1`, their DYDX card closed | | |
| 3.5 | A person is only in `responsible_support` — **remove them from it** | Their DYDX card closed | | |
| 3.6 | A person is in card-level assignees AND `responsible_support` — **remove from card-level only** | Cascade: person removed from `responsible_support`, DYDX card closed | | |
| 3.7 | **Remove Person A** from one field and **add Person B** to a different field in the same change | Person A's card closed, Person B's card created. No cascade interference | | |
| 3.8 | **Remove a person** and **re-add them** to a different field simultaneously (reassignment) | No cascade: person stays on card, DYDX card remains open | | |
| 3.9 | A card has 2 assignees. Remove **both** | Both DYDX cards closed | | |
| 3.10 | A card with no assignees — **add an assignee** | 1 DYDX card created | | |

---

## Section 4 — Labels & Priority

| # | Action | Expected Result | Result | Notes |
|---|--------|-----------------|--------|-------|
| 4.1 | Create MM card **with no priority label** → check DYDX card | DYDX card has **Low** label | | |
| 4.2 | Create MM card with **"Low" label** | DYDX card has **Low** label | | |
| 4.3 | Create MM card with **"Important" label** | DYDX card has **High** label | | |
| 4.4 | Create MM card with **"System critical" label** | DYDX card has **Very High** label | | |
| 4.5 | Move MM card to any active phase — **change label from Low → Important** first | On next move, DYDX card label updates to **High** | | |
| 4.6 | Move MM card to **CHANGE REQUEST ON HOLD** | DYDX card has **both** the priority label AND the **"On Hold" label** | | |

---

## Section 5 — Duplicate Prevention

| # | Action | Expected Result | Result | Notes |
|---|--------|-----------------|--------|-------|
| 5.1 | Trigger the same webhook twice rapidly (within 5s) — e.g. move card to same phase twice | Only **1** sync processed; second is ignored as duplicate | | Check logs |
| 5.2 | Add the same person who is already assigned | No duplicate DYDX card created | | |
| 5.3 | Move a card and immediately move it again within 10s | Sync dedup window allows at most 1 sync per 10s; second sync skipped | | Check logs |

---

## Section 6 — Terminal Phases

| # | Action | Expected Result | Result | Notes |
|---|--------|-----------------|--------|-------|
| 6.1 | Move MM card to **RESOLVED** | All DYDX cards for that MM card move to **Done** | | |
| 6.2 | Move MM card to **NOT APPROVED** | All DYDX cards for that MM card move to **Cancelled** | | |
| 6.3 | MM card with **2 assignees** → move to RESOLVED | **Both** DYDX cards move to Done | | |
| 6.4 | MM card with **2 assignees** → move to NOT APPROVED | **Both** DYDX cards move to Cancelled | | |
| 6.5 | Move MM card to RESOLVED, then back to IN PROGRESS | New DYDX card(s) created in In Progress (existing Done cards remain Done) | | This may vary — log and note actual behaviour |

---

## Section 7 — Multi-Assignee Scenarios

| # | Scenario | Expected Result | Result | Notes |
|---|----------|-----------------|--------|-------|
| 7.1 | MM card has **3 assignees** — move through BACKLOG → IN PROGRESS → COMMS TO CLIENT | All 3 DYDX cards follow each phase transition | | |
| 7.2 | MM card has **3 assignees** — remove 1 midway through (while In Progress) | Removed person's DYDX card closes; other 2 remain open in same phase | | |
| 7.3 | MM card has **2 assignees** — move to NOT APPROVED | Both DYDX cards move to Cancelled | | |
| 7.4 | Same person appears in **3 different assignee fields** — verify in DYDX | Only **1** DYDX card for that person (union dedup) | | |

---

## Section 8 — Card Listener (Polling)

These tests validate changes the listener detects that don't produce webhooks.

| # | Action | Expected Result | Result | Notes |
|---|--------|-----------------|--------|-------|
| 8.1 | Add someone to **card-level assignees** (top-left avatars) — wait up to 15s | DYDX card created for them (listener-detected, no webhook) | | |
| 8.2 | Remove someone from **card-level assignees** — wait up to 15s | Cascade triggers, DYDX card closed | | |
| 8.3 | Call `POST /listener/force_check` immediately after a card-level change | DYDX card created/closed immediately without waiting for poll | | |
| 8.4 | Call `GET /listener/status` | Returns `running: true`, last poll time, number of monitored cards | | |

---

## Section 9 — On Hold

| # | Action | Expected Result | Result | Notes |
|---|--------|-----------------|--------|-------|
| 9.1 | Move MM card to **CHANGE REQUEST ON HOLD** | DYDX card(s) move to **Backlog** and gain **"On Hold" label** | | |
| 9.2 | Move MM card from On Hold back to **IN PROGRESS** | DYDX card(s) move to **In Progress**; On Hold label may remain (note behaviour) | | |
| 9.3 | Create a card **already in** CHANGE REQUEST ON HOLD phase | DYDX card created in Backlog with On Hold label | | |

---

## Section 10 — Health & Operations

| # | Check | Expected Result | Result | Notes |
|---|-------|-----------------|--------|-------|
| 10.1 | `GET /health` | `{"status": "healthy"}` or similar | | |
| 10.2 | `GET /` | Service status including listener state | | |
| 10.3 | `GET /listener/status` | `running: true`, poll interval, card count | | |
| 10.4 | `GET /status` | List of recent webhook jobs with statuses | | |
| 10.5 | `POST /listener/stop` then `POST /listener/start` | Listener stops and restarts cleanly | | |
| 10.6 | `GET /cleanup/orphaned` | Returns list (may be empty if board is clean) | | |

---

## Known Timing Constraints

| Constraint | Value | Impact on Testing |
|------------|-------|-------------------|
| Listener poll interval | ~10s | Card-level changes may take up to 10s to appear |
| Sync dedup window | 10s | Moving a card twice rapidly → only 1 sync |
| Webhook dedup | 5s | Same card+action within 5s → ignored |
| DYDX card creation cooldown | 30s | Cannot create same card twice within 30s |
| DYDX board cache TTL | 120s | Board state cached 2min; in-place updates applied immediately |

---

## Quick Smoke Test (5-minute check)

If you only have 5 minutes, run these 5 tests:

1. **Create** a new MM card with 1 assignee → confirm 1 DYDX card appears in Backlog
2. **Move** that card to IN PROGRESS → confirm DYDX card moves to In Progress
3. **Add** a second assignee → confirm a second DYDX card is created
4. **Move** to NOT APPROVED → confirm **both** DYDX cards move to **Cancelled**
5. Verify the DYDX card title format and `main_task_id` field contain the correct MM card ID

---

## Defect Log

| Test # | Description | Steps to Reproduce | Actual Behaviour | Severity |
|--------|-------------|-------------------|------------------|----------|
| | | | | |
| | | | | |
