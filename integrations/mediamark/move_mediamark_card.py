"""
Move a Mediamark card to a target phase using Mediamark API credentials.

Usage examples:
  python integrations/mediamark/move_mediamark_card.py --card-id 1309394388 --phase-name "BACKLOG"
  python integrations/mediamark/move_mediamark_card.py --card-id 1309394388 --phase-id 123456789
"""

import argparse
import os
import sys
from typing import Optional, Dict, List

from dotenv import load_dotenv

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_THIS_DIR, '..', '..'))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from utils.pipefy_client import PipefyClient


def load_mediamark_env() -> None:
    env_path = os.path.join(_THIS_DIR, '.env.mediamark')
    if os.path.exists(env_path):
        load_dotenv(env_path)
    else:
        load_dotenv()


def get_pipe_phases(client: PipefyClient, pipe_id: str) -> List[Dict[str, str]]:
    query = """
    query GetPipePhases($pipeId: ID!) {
      pipe(id: $pipeId) {
        id
        name
        phases {
          id
          name
        }
      }
    }
    """
    result = client.execute_query(query, {'pipeId': pipe_id})
    return result.get('data', {}).get('pipe', {}).get('phases', [])


def get_card_phase(client: PipefyClient, card_id: str) -> Optional[Dict[str, str]]:
    result = client.get_card(card_id)
    return result.get('data', {}).get('card', {}).get('current_phase', {})


def resolve_phase_id_by_name(phases: List[Dict[str, str]], phase_name: str) -> Optional[str]:
    target = phase_name.strip().lower()

    # Exact match first
    for phase in phases:
        if (phase.get('name') or '').strip().lower() == target:
            return phase.get('id')

    # Then substring match (helpful for minor naming differences)
    for phase in phases:
        name = (phase.get('name') or '').strip().lower()
        if target in name or name in target:
            return phase.get('id')

    return None


def main() -> int:
    parser = argparse.ArgumentParser(description='Move a Mediamark card to a target phase.')
    parser.add_argument('--card-id', required=True, help='Source Mediamark card ID')
    parser.add_argument('--pipe-id', default=None, help='Mediamark pipe ID (defaults to MEDIAMARK_SUPPORT_BOARD_PIPE_ID env var)')
    parser.add_argument('--phase-id', default=None, help='Destination phase ID')
    parser.add_argument('--phase-name', default=None, help='Destination phase name (e.g. BACKLOG)')

    args = parser.parse_args()

    if not args.phase_id and not args.phase_name:
        print('Error: provide either --phase-id or --phase-name')
        return 2

    load_mediamark_env()

    api_key = os.getenv('MEDIAMARK_API_KEY')
    if not api_key:
        print('Error: MEDIAMARK_API_KEY not found in environment')
        return 2

    pipe_id = args.pipe_id or os.getenv('MEDIAMARK_SUPPORT_BOARD_PIPE_ID')
    if not pipe_id:
        print('Error: pipe ID missing. Provide --pipe-id or set MEDIAMARK_SUPPORT_BOARD_PIPE_ID')
        return 2

    client = PipefyClient(api_key, org_name='mediamark')

    try:
        before_phase = get_card_phase(client, args.card_id)
        print(f"Card {args.card_id} current phase: {before_phase.get('name')} ({before_phase.get('id')})")

        destination_phase_id = args.phase_id
        if not destination_phase_id:
            phases = get_pipe_phases(client, pipe_id)
            destination_phase_id = resolve_phase_id_by_name(phases, args.phase_name)
            if not destination_phase_id:
                print(f"Error: phase name '{args.phase_name}' not found in pipe {pipe_id}")
                print('Available phases:')
                for phase in phases:
                    print(f"  - {phase.get('name')} ({phase.get('id')})")
                return 2

        client.move_card_to_phase(args.card_id, destination_phase_id)

        after_phase = get_card_phase(client, args.card_id)
        print(f"Card {args.card_id} moved to: {after_phase.get('name')} ({after_phase.get('id')})")
        return 0

    except Exception as exc:
        print(f"Move failed: {exc}")
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
