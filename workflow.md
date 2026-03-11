# Mediamark → DYDX Development Tasks Sync Engine

===  (Big Picture) ===
This is the CORE SYNC ENGINE for the Mediamark integration. It mirrors cards from
Mediamark's "Workflow Support" board to the DYDX Development Tasks Management board.

Key details:
1. SOURCE ORG: Mediamark
2. SOURCE BOARD: Single "Workflow Support" board (pipe ID: 301684738)
3. DESTINATION: DYDX Development Tasks Management board (pipe ID: 306286881)
4. CLIENT NAME: Hardcoded as "Mediamark"
5. TITLE FORMAT: "[{Support request type}] Card Title" (e.g., "[Feature/change request] Fix Login Bug")
6. PROJECT NAME: "Mediamark Integration | {System Field Value/s}"
7. PARTNER: Jesslynn Shepherd (hardcoded)
8. SYSTEM TYPE: Always "Pipefy"

=== SINGLE BOARD ARCHITECTURE ===
There is NO separate Change Request board. The "Workflow Support" board has a
"Support request type" start form field with options:
  - Issue/Question
  - Feature/change request  (← this is how CRs are filed)
  - Access to system (for yourself)
  - New user request

Mediamark Pipefy Org (SOURCE)         DYDX Pipefy Org (DESTINATION)
┌──────────────────────┐              ┌─────────────────────────────────┐
│  Workflow Support      │────────────►  │  Development Tasks Management   │
│  (all request types)   │              │  (Backlog → In Progress → ...)  │
└──────────────────────┘              └─────────────────────────────────┘

=== PER-ASSIGNEE MODEL ===
Each source card creates ONE DYDX card PER ASSIGNEE.
If a Mediamark card has 3 assignees, 3 separate DYDX cards are created.

# Sync cards from Mediamark → DYDX Development Tasks Management board.
    
    Key behaviors:
    - Title format: [{Support request type}] Card Title
    - Client name: "Mediamark" (hardcoded)
    - Project name: "Mediamark Integration | {System Field Value/s}"
    - Per-assignee model: one DYDX card per assignee per source card
    - Description field: picked based on Support request type

    