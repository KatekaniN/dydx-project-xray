#!/usr/bin/env python3
"""
Field Mappings — Mediamark → DYDX Development Tasks Management

All field ID constants and mapping tables for the Mediamark integration.
Field IDs were discovered from the live Pipefy API via test_connection_mediamark.py.

Source board: Mediamark "Workflow Support" (pipe ID: 301684738)
Destination board: DYDX "Development Tasks Management" (pipe ID: 306286881)
"""

# HARDCODED VALUES
# These are always the same for every card synced from Mediamark.

HARDCODED_CLIENT_NAME = "Mediamark"
HARDCODED_PARTNER = "Jesslynn Shepherd"
HARDCODED_PARTNER_ID = "877678"          # Jess Shepherd (jess@dydx.digital)
HARDCODED_SYSTEM_TYPE = "Pipefy"
PROJECT_NAME_PREFIX = "Mediamark Integration"

# Title prefixes — used as fallback if "Support request type" field is empty
TITLE_PREFIX_CR = "CR"
TITLE_PREFIX_SUPPORT = "Support"

MEDIAMARK_ACTIVE_PHASES = [
    'new',
    'review',
    'escalated',
    'sow and scoping',
    'client approval',
    'backlog',
    'in progress',
    'comms to client',
    'change request on hold',
]

MEDIAMARK_TERMINAL_PHASES = [
    'resolved',
    'not approved',
]


# ==========================================
# MEDIAMARK SOURCE FIELD IDS
# ==========================================
# These are the real field IDs on the Mediamark "Workflow Support" board.
# Source: test_connection_mediamark.py Step 4 (start form fields)

# Start form fields
FIELD_SUPPORT_REQUEST_TYPE = "support_request_type_1"   # select, REQUIRED
FIELD_ISSUE_QUESTION = "issues_question"                # long_text
FIELD_FEATURE_REQUEST = "feature_request"               # long_text
FIELD_SYSTEM = "system"                                 # select
FIELD_LINK_TO_ISSUE = "link_to_issue"                   # short_text
FIELD_PRIORITY = "priority"                             # select
FIELD_EXPECTED_DATE = "expected_date"                   # date
FIELD_ADDITIONAL_INFO = "additional_information"        # long_text

# Support request type options (from start form)
SUPPORT_TYPE_ISSUE_QUESTION = "Issue/Question"
SUPPORT_TYPE_FEATURE_CHANGE = "Feature/change request"
SUPPORT_TYPE_ACCESS_SYSTEM = "Access to system (for yourself)"
SUPPORT_TYPE_NEW_USER = "New user request"


# ==========================================
# DYDX DESTINATION FIELD IDS
# ==========================================
# These are the real field IDs on the DYDX "Development Tasks Management" board.

DYDX_TASK_NAME = "task_name"
DYDX_CLIENT_NAME = "client_name"
DYDX_PRIORITY = "priority"
DYDX_PARTNER = "partner"
DYDX_SYSTEM_TYPE = "system_type"
DYDX_PROJECT_NAME = "project_name"
DYDX_DESCRIPTION = "task_description"
DYDX_DUE_DATE = "due_date"
DYDX_SOURCE_CARD_ID = "source_card_id"
DYDX_SOURCE_BOARD = "source_board"
DYDX_SOURCE_PHASE = "main_task_status_name"
DYDX_SOURCE_PHASE_ID = "main_task_status_id"
DYDX_REQUEST_TYPE = "request_type"
DYDX_SYSTEM = "system_name"
DYDX_ADDITIONAL_INFO = "additional_info"
DYDX_LINK = "link_to_issue"
DYDX_ASSIGNEE = "assignee"


# ==========================================
# SUPPORT REQUEST TYPE PATTERNS
# ==========================================
# Used to find the "Support request type" field on source cards.
# Matches against field ID, label, and name (case-insensitive substring).

SUPPORT_REQUEST_TYPE_FIELD_PATTERNS = [
    'support_request_type_1',   # actual field ID
    'support request type',
    'request type',
]

# Values that indicate a Change Request (everything else is support)
CR_REQUEST_TYPES = ['Feature/change request']


# ==========================================
# DESCRIPTION FIELD PATTERNS
# ==========================================
# Used to find the description field on source cards.
# Different patterns for CR vs support tickets.

CR_DESCRIPTION_FIELD_PATTERNS = [
    'feature_request',          # actual field ID for CR descriptions
    'feature request',
    'description',
    'change request description',
]

SUPPORT_DESCRIPTION_FIELD_PATTERNS = [
    'issues_question',          # actual field ID for support descriptions
    'issues question',
    'issue/question',
    'description',
    'initial notes',
    'link_to_issue',
]


# ==========================================
# TYPE OF REQUEST FIELD PATTERNS
# ==========================================
# Used to find the request type field for routing.

TYPE_OF_REQUEST_FIELD_PATTERNS = [
    'support_request_type_1',
    'support request type',
    'request type',
    'type of request',
]


# ==========================================
# SYSTEM FIELD PATTERNS
# ==========================================
# Used to find the "System" field for building project names.

SYSTEM_FIELD_PATTERNS = [
    'system',
    'systems',
    'platform',
]


# ==========================================
# FIELD MAPPING TABLES
# ==========================================
# Maps Mediamark source field IDs → DYDX destination field IDs.
# Used by sync_to_dydx.py to copy field values across boards.

CHANGE_REQUEST_TO_DYDX_MAPPING = {
    'feature_request':              'task_description',
    'priority':                     'priority',
    'expected_date':                'due_date',
    'additional_information':       'additional_info',
    'link_to_issue':                'link_to_issue',
}

SUPPORT_TO_DYDX_MAPPING = {
    'issues_question':              'task_description',
    'priority':                     'priority',
    'expected_date':                'due_date',
    'additional_information':       'additional_info',
    'link_to_issue':                'link_to_issue',
}
