#!/usr/bin/env python3
"""
Connection Test Script — Verify Mediamark & DYDX API Connections
"""

import os
import sys

# ---- Add project root to Python path ----
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_THIS_DIR, '..', '..'))
sys.path.insert(0, _PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(_THIS_DIR, '.env.mediamark'))

from utils.pipefy_client import PipefyClient


def test_mediamark_connection():
   
    print("=" * 60)
    print(" MEDIAMARK ↔ DYDX CONNECTION TEST")
    print("=" * 60)
    print()

    # ---- Step 0: Check environment variables ----
    print("[STEP 0] Checking environment variables...")

    env_vars = {
        'MEDIAMARK_API_KEY': os.getenv('MEDIAMARK_API_KEY'),
        'DYDX_API_KEY': os.getenv('DYDX_API_KEY'),
        'DYDX_DEV_TASKS_PIPE_ID': os.getenv('DYDX_DEV_TASKS_PIPE_ID'),
        'MEDIAMARK_SUPPORT_BOARD_PIPE_ID': os.getenv('MEDIAMARK_SUPPORT_BOARD_PIPE_ID'),
    }

    all_set = True
    for var_name, var_value in env_vars.items():
        if var_value and len(var_value) > 5:
            display = var_value[:8] + "..." if len(var_value) > 8 else var_value
            print(f"   {var_name} = {display}")
        else:
            print(f"   {var_name} — NOT SET!")
            all_set = False

    if not all_set:
        print()
        print(" Fix the missing required environment variables above.")
        return False

    print()

    # ---- Step 1: Initialize API Clients ----
    print("[STEP 1] Creating Pipefy API clients...")
    try:
        mediamark_client = PipefyClient(os.getenv('MEDIAMARK_API_KEY'))
        dydx_client = PipefyClient(os.getenv('DYDX_API_KEY'))
        print("   Both API clients created!")
    except Exception as e:
        print(f"   Failed to create clients: {e}")
        return False

    print()

    # ---- Step 2: Test Mediamark connection ----
    print("[STEP 2] Testing Mediamark Pipefy connection...")
    try:
        result = mediamark_client.execute_query("query { me { name email } }")
        me = result.get('data', {}).get('me', {})
        print(f"   Connected as: {me.get('name', 'Unknown')} ({me.get('email', 'Unknown')})")
    except Exception as e:
        print(f"   Failed to connect to Mediamark: {e}")
        print("     → Check that MEDIAMARK_API_KEY is correct and not expired.")
        return False

    print()

    # ---- Step 3: Test DYDX connection ----
    print("[STEP 3] Testing DYDX Pipefy connection...")
    try:
        result = dydx_client.execute_query("query { me { name email } }")
        me = result.get('data', {}).get('me', {})
        print(f"  Connected as: {me.get('name', 'Unknown')} ({me.get('email', 'Unknown')})")
    except Exception as e:
        print(f"  Failed to connect to DYDX: {e}")
        print("     → Check that DYDX_API_KEY is correct and not expired.")
        return False

    print()

    # ---- Step 4: Test DYDX Dev Tasks board ----
    dev_tasks_pipe_id = os.getenv('DYDX_DEV_TASKS_PIPE_ID')
    print(f"[STEP 4] Testing DYDX Development Tasks board (pipe {dev_tasks_pipe_id})...")
    try:
        query = """query GetPipe($pipeId: ID!) {
            pipe(id: $pipeId) {
                id name
                phases { id name cards_count }
            }
        }"""
        result = dydx_client.execute_query(query, {"pipeId": dev_tasks_pipe_id})
        pipe = result.get('data', {}).get('pipe', {})
        phases = pipe.get('phases', [])
        total_cards = sum(p.get('cards_count', 0) for p in phases)
        print(f"   Board: {pipe.get('name', 'Unknown')} (ID: {pipe.get('id')})")
        print(f"   Phases ({len(phases)}), Total cards: {total_cards}")
        for phase in phases:
            print(f"     - {phase['name']} ({phase.get('cards_count', 0)} cards, ID: {phase['id']})")
    except Exception as e:
        print(f"   Failed to access DYDX Dev Tasks board: {e}")
        return False

    print()

    # ---- Step 5: Inspect the Mediamark "Workflow Support" board ----
    support_pipe_id = os.getenv('MEDIAMARK_SUPPORT_BOARD_PIPE_ID')
    print(f"[STEP 5] Inspecting Mediamark Workflow Support board (pipe {support_pipe_id})...")
    print("         This is the ONLY Mediamark source board — there is no separate CR board.")
    print("         The 'Support request type' start-form field determines card routing.")
    print()
    try:
        # Query the board with start_form_fields including options (for dropdown values)
        query = """query GetPipeDetail($pipeId: ID!) {
            pipe(id: $pipeId) {
                id name
                phases { id name cards_count }
                start_form_fields {
                    id label type required
                    options
                }
            }
        }"""
        result = mediamark_client.execute_query(query, {"pipeId": support_pipe_id})
        pipe = result.get('data', {}).get('pipe', {})
        phases = pipe.get('phases', [])
        total_cards = sum(p.get('cards_count', 0) for p in phases)

        print(f"   Board: {pipe.get('name', 'Unknown')} (ID: {pipe.get('id')})")
        print(f"   Phases ({len(phases)}), Total cards: {total_cards}")
        for phase in phases:
            print(f"     - {phase['name']} ({phase.get('cards_count', 0)} cards, ID: {phase['id']})")
        print()

        # Show start form fields — especially the "Support request type" dropdown
        start_fields = pipe.get('start_form_fields', [])
        if start_fields:
            print(f"   Start Form Fields ({len(start_fields)}):")
            for f in start_fields:
                req = " [REQUIRED]" if f.get('required') else ""
                options = f.get('options', [])
                print(f"     - {f['label']} (ID: {f['id']}, type: {f['type']}{req})")
                if options:
                    print(f"       Options: {options}")
            print()

            # Highlight the Support request type field
            for f in start_fields:
                if 'request type' in f.get('label', '').lower() or 'support' in f.get('label', '').lower():
                    print(f"   KEY FIELD — '{f['label']}' (ID: {f['id']})")
                    print(f"     This dropdown determines CR vs Support routing.")
                    if f.get('options'):
                        for opt in f['options']:
                            marker = "  ← CR" if 'feature' in str(opt).lower() or 'change' in str(opt).lower() else ""
                            print(f"       • {opt}{marker}")
                    print()

    except Exception as e:
        print(f"   Failed to inspect Workflow Support board: {e}")
        print("     → Check MEDIAMARK_SUPPORT_BOARD_PIPE_ID is correct.")

    print()

    # ---- Step 5b: Inspect phase-level fields on Workflow Support board ----
    print("[STEP 5b] Fetching phase-level fields for Workflow Support board...")
    try:
        query = """query GetPipePhaseFields($pipeId: ID!) {
            pipe(id: $pipeId) {
                phases {
                    name
                    fields { id label type required }
                }
            }
        }"""
        result = mediamark_client.execute_query(query, {"pipeId": support_pipe_id})
        pipe = result.get('data', {}).get('pipe', {})
        for phase in pipe.get('phases', []):
            phase_fields = phase.get('fields', [])
            if phase_fields:
                print(f"  Phase '{phase['name']}' Fields ({len(phase_fields)}):")
                for f in phase_fields:
                    req = " [REQUIRED]" if f.get('required') else ""
                    print(f"     - {f['label']} (ID: {f['id']}, type: {f['type']}{req})")
    except Exception as e:
        print(f"  Failed to fetch phase-level fields: {e}")

    print()

    # ---- Step 6: Discover DYDX Dev Tasks field IDs ----
    print("[STEP 6] Fetching DYDX Dev Tasks board field IDs...")
    try:
        query = """query GetPipeFields($pipeId: ID!) {
            pipe(id: $pipeId) {
                start_form_fields { id label type required }
                phases {
                    name
                    fields { id label type required }
                }
            }
        }"""
        result = dydx_client.execute_query(query, {"pipeId": dev_tasks_pipe_id})
        pipe = result.get('data', {}).get('pipe', {})

        start_fields = pipe.get('start_form_fields', [])
        if start_fields:
            print(f"  Start Form Fields ({len(start_fields)}):")
            for f in start_fields:
                req = " [REQUIRED]" if f.get('required') else ""
                print(f"     - {f['label']} (ID: {f['id']}, type: {f['type']}{req})")

        for phase in pipe.get('phases', []):
            phase_fields = phase.get('fields', [])
            if phase_fields:
                print(f"  Phase '{phase['name']}' Fields ({len(phase_fields)}):")
                for f in phase_fields:
                    req = " [REQUIRED]" if f.get('required') else ""
                    print(f"     - {f['label']} (ID: {f['id']}, type: {f['type']}{req})")

    except Exception as e:
        print(f"  Failed to fetch field IDs: {e}")

    print()

   
    print(" CONNECTION TESTS COMPLETE!")
    return True


if __name__ == "__main__":
    success = test_mediamark_connection()
    sys.exit(0 if success else 1)
