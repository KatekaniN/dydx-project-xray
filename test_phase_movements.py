#!/usr/bin/env python3
"""
Test phase movement mappings: Mediamark → DYDX.

Usage:
  # Dry-run: verify routing logic only (no API calls)
  python test_phase_movements.py

  # Live: simulate card.move webhooks against a real Mediamark card
  python test_phase_movements.py --live --card-id 1309710645

  # Live: also physically move the Mediamark card through each phase
  python test_phase_movements.py --live --card-id 1309710645 --move-card
"""

import argparse
import os
import sys
import time

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _PROJECT_ROOT)

from dotenv import load_dotenv

_ENV_FILE = os.path.join(_PROJECT_ROOT, 'integrations', 'mediamark', '.env.mediamark')
if os.path.exists(_ENV_FILE):
    load_dotenv(_ENV_FILE)
else:
    load_dotenv()


# ──────────────────────────────────────────────
# Phase mapping reference (mirrors sync_to_dydx.py)
# ──────────────────────────────────────────────

# Each entry: (mediamark_phase, expected_dydx_phase, enable_move, board_type, expected_action)
#   enable_move  = whether a card.move webhook should physically move the DYDX card
#   expected_action = what the sync engine should do with the DYDX card

SUPPORT_PHASE_CASES = [
    # Mediamark phase           Expected DYDX phase     Move?   Action
    ("NEW",                     "backlog",               True,   "create/move to backlog"),
    ("REVIEW",                  "in progress",           True,   "create/move to in progress"),
    ("ESCALATED",               "in progress",           False,  "sync fields only (no move)"),
    ("SOW and Scoping",         "in progress",           True,   "create/move to in progress"),
    ("CLIENT APPROVAL",         "in progress",           True,   "create/move to in progress"),
    ("BACKLOG",                 "backlog",               True,   "create/move to backlog"),
    ("IN PROGRESS",             "in progress",           True,   "create/move to in progress"),
    ("COMMS TO CLIENT",         "testing / comms",       True,   "move to testing / comms"),
    ("RESOLVED",                "done",                  True,   "close all DYDX cards"),
]

CR_PHASE_CASES = [
    # Mediamark phase           Expected DYDX phase     Note
    ("NEW",                     "backlog",               "create in backlog"),
    ("REVIEW",                  "in progress",           "create/move to in progress"),
    ("ESCALATED",               "in progress",           "create/move to in progress"),
    ("SOW and Scoping",         "in progress",           "create/move to in progress"),
    ("CLIENT APPROVAL",         "EXCLUDED",              "no action (excluded phase)"),
    ("BACKLOG",                 "backlog",               "create in backlog"),
    ("IN PROGRESS",             "in progress",           "create/move to in progress"),
    ("COMMS TO CLIENT",         "testing / comms",       "move to testing / comms"),
    ("RESOLVED",                "done",                  "close all DYDX cards"),
    ("NOT APPROVED",            "cancelled",             "move all to cancelled"),
    ("CHANGE REQUEST ON HOLD",  "backlog",               "create/move to backlog"),
]


def print_phase_table():
    """Print the full phase mapping table to the console."""
    print("\n" + "=" * 75)
    print("MEDIAMARK → DYDX PHASE MAPPING")
    print("=" * 75)

    print("\n── SUPPORT TICKETS ─────────────────────────────────────────────────────")
    print(f"  {'Mediamark Phase':<28} {'DYDX Phase':<22} {'Move?':<8} {'Action'}")
    print(f"  {'-'*27:<28} {'-'*21:<22} {'-'*5:<8} {'-'*30}")
    for mm_phase, dydx_phase, move, action in SUPPORT_PHASE_CASES:
        move_str = "Yes" if move else "No "
        print(f"  {mm_phase:<28} {dydx_phase:<22} {move_str:<8} {action}")

    print("\n── CHANGE REQUESTS (CR) ─────────────────────────────────────────────────")
    print(f"  {'Mediamark Phase':<28} {'DYDX Phase':<22} {'Note'}")
    print(f"  {'-'*27:<28} {'-'*21:<22} {'-'*30}")
    for mm_phase, dydx_phase, note in CR_PHASE_CASES:
        print(f"  {mm_phase:<28} {dydx_phase:<22} {note}")

    print("\n── HOW IT WORKS ─────────────────────────────────────────────────────────")
    print("""
  Webhook events from Mediamark are routed by action type:

  card.create
    → Determines board_type from "Support request type" field
    → Calls sync_assignees_to_dydx() to create DYDX card

  card.move  (most important)
    → Reads current_phase from webhook payload
    → Routes to the correct handler:
         resolved         → close_dydx_card() for all linked DYDX cards
         comms to client  → move to 'testing / comms' (enable_move=True)
         escalated        → sync fields only (enable_move=False, no move)
         in progress/new/review → move to 'in progress' (enable_move=True)
         backlog          → move to 'backlog' (enable_move=True)
         not approved     (CR only) → move all to 'cancelled'
         client approval  (CR only) → no action (excluded phase)

  card.field_update / card.update
    → Re-reads current phase from API
    → Same routing logic as card.move but called from _handle_field_update()

  enable_move flag
    → True  → sync_assignees_to_dydx() will physically move the DYDX card
               to the target phase using move_card_to_phase()
    → False → fields are synced but the DYDX card stays in its current phase
               (used for escalated to avoid bouncing cards already in progress)
""")


def test_routing_logic():
    """Unit-test the _get_correct_dydx_phase() logic without any API calls."""
    from integrations.mediamark.sync_to_dydx import MediamarkSync

    # We only need the mapping function — patch __init__ to skip API setup
    import unittest.mock as mock
    with mock.patch.object(MediamarkSync, '__init__', lambda self: None):
        engine = MediamarkSync.__new__(MediamarkSync)
        # Copy only the phase constants needed
        engine.SUPPORT_ESCALATED_PHASES = MediamarkSync.SUPPORT_ESCALATED_PHASES
        engine.SUPPORT_TESTING_PHASES   = MediamarkSync.SUPPORT_TESTING_PHASES
        engine.SUPPORT_COMMS_PHASES     = MediamarkSync.SUPPORT_COMMS_PHASES
        engine.SUPPORT_COMPLETED_PHASES = MediamarkSync.SUPPORT_COMPLETED_PHASES
        engine.SUPPORT_IN_PROGRESS_PHASES = MediamarkSync.SUPPORT_IN_PROGRESS_PHASES
        engine.SUPPORT_BACKLOG_PHASES   = MediamarkSync.SUPPORT_BACKLOG_PHASES
        engine.CR_EXCLUDED_PHASES       = MediamarkSync.CR_EXCLUDED_PHASES
        engine.CR_COMPLETED_PHASES      = MediamarkSync.CR_COMPLETED_PHASES
        engine.CR_CANCELLED_PHASES      = MediamarkSync.CR_CANCELLED_PHASES

    print("\n── ROUTING LOGIC UNIT TESTS ─────────────────────────────────────────────")
    passed = 0
    failed = 0

    all_cases = [(mm, expected) for mm, expected, *_ in SUPPORT_PHASE_CASES + [
        (mm, exp, None) for mm, exp, _ in CR_PHASE_CASES
    ]]

    # Deduplicate and test _get_correct_dydx_phase
    seen = set()
    test_cases = []
    for mm, expected, *_ in SUPPORT_PHASE_CASES:
        if mm.lower() not in seen:
            seen.add(mm.lower())
            test_cases.append((mm, expected))
    for mm, expected, _ in CR_PHASE_CASES:
        if mm.lower() not in seen and expected != "EXCLUDED":
            seen.add(mm.lower())
            test_cases.append((mm, expected))

    for mm_phase, expected_dydx in test_cases:
        actual = engine._get_correct_dydx_phase(mm_phase)
        ok = actual.lower() == expected_dydx.lower()
        status = "PASS" if ok else "FAIL"
        if ok:
            passed += 1
        else:
            failed += 1
        print(f"  [{status}] '{mm_phase}' → '{actual}' (expected '{expected_dydx}')")

    print(f"\n  Result: {passed} passed, {failed} failed")
    return failed == 0


def test_live_routing(card_id: str, move_card: bool = False):
    """
    Simulate card.move webhooks against a real Mediamark card.

    For each Mediamark phase, calls process_support_webhook() with
    action='card.move' and the given phase. Reports what the sync
    engine did without (unless --move-card) physically moving the card.
    """
    from integrations.mediamark.sync_to_dydx import MediamarkSync
    from integrations.mediamark.move_mediamark_card import (
        load_mediamark_env, get_pipe_phases, get_card_phase, resolve_phase_id_by_name
    )
    from utils.pipefy_client import PipefyClient

    print(f"\n── LIVE ROUTING TEST  card={card_id} ─────────────────────────────────────")

    engine = MediamarkSync()
    print(f"  MediamarkSync initialised.")

    if move_card:
        mm_client = engine.clients.mediamark_client
        pipe_id = os.getenv('MEDIAMARK_SUPPORT_BOARD_PIPE_ID')
        if not pipe_id:
            print("  WARNING: MEDIAMARK_SUPPORT_BOARD_PIPE_ID not set — cannot move card")
            move_card = False
        else:
            phases = get_pipe_phases(mm_client, pipe_id)
            print(f"  Loaded {len(phases)} Mediamark phases for card moves\n")

    # When physically moving the card, exclude terminal phases (RESOLVED closes the DYDX card)
    # so the card remains usable after the test run.
    terminal_phases = {'resolved', 'not approved'}
    test_phases = [
        mm for mm, *_ in SUPPORT_PHASE_CASES
        if not (move_card and mm.lower() in terminal_phases)
    ]

    for mm_phase in test_phases:
        print(f"\n  → Simulating card.move to '{mm_phase}'")

        if move_card:
            phase_id = resolve_phase_id_by_name(phases, mm_phase)
            if phase_id:
                try:
                    mm_client.move_card_to_phase(card_id, phase_id)
                    print(f"    Moved Mediamark card to '{mm_phase}'")
                    time.sleep(2)  # allow webhook to settle
                except Exception as e:
                    print(f"    WARNING: Could not move card: {e}")
            else:
                print(f"    WARNING: Phase '{mm_phase}' not found on Mediamark board")

        try:
            result = engine.process_support_webhook(
                source_card_id=card_id,
                action='card.move',
                current_phase=mm_phase,
            )
            print(f"    Result: {result}")
        except Exception as e:
            print(f"    ERROR: {e}")

        time.sleep(1)  # avoid hammering API


def main():
    parser = argparse.ArgumentParser(description='Test Mediamark → DYDX phase movement mapping')
    parser.add_argument('--live',      action='store_true', help='Run live routing test against real Mediamark card')
    parser.add_argument('--card-id',   default=None,       help='Mediamark card ID to test against (required for --live)')
    parser.add_argument('--move-card', action='store_true', help='Also physically move the Mediamark card through each phase')
    args = parser.parse_args()

    # Always print the mapping table
    print_phase_table()

    if not args.live:
        # Routing logic unit tests (no API calls)
        ok = test_routing_logic()
        print()
        sys.exit(0 if ok else 1)

    if not args.card_id:
        print("\nERROR: --card-id is required for --live mode")
        sys.exit(1)

    test_live_routing(args.card_id, move_card=args.move_card)
    print()


if __name__ == '__main__':
    main()
