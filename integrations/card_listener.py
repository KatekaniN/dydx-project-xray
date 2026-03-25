#!/usr/bin/env python3
"""
Real-Time Card Change Listener for Mediamark → DYDX Sync
Polls the Mediamark "Workflow Support" board for changes and triggers sync automatically.

=== WHAT THIS FILE DOES ===
This is the SAFETY NET for the Mediamark integration. It runs in the background,
polling Mediamark's single "Workflow Support" board for changes that webhooks might miss.

=== HOW IT WORKS ===
1. Every N seconds, fetch all active cards from the Workflow Support board
2. Compute a hash (fingerprint) of each card's key fields
3. Compare to previous hash → if different, trigger a sync
4. If a card disappears (moved to Done) → trigger final sync
"""

import os
import time
import threading
import logging
import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from dotenv import load_dotenv

from integrations.field_mappings import MEDIAMARK_ACTIVE_PHASES, MEDIAMARK_TERMINAL_PHASES

load_dotenv()

logger = logging.getLogger(__name__)


class MediamarkCardChangeListener:
    """
    Polls Mediamark Pipefy cards for changes and triggers DYDX sync.
    """
    
    def __init__(self, sync_service, mediamark_client):
        """
        Args:
            sync_service:    A MediamarkSync instance that does the actual syncing.
            mediamark_client: A PipefyClient for the Mediamark organization.
        """
        self.sync_service = sync_service
        self.mediamark_client = mediamark_client
        
        # ---- Configuration ----
        self.poll_interval = int(os.getenv('MM_LISTENER_POLL_INTERVAL', 10))
        
        # Mediamark "Workflow Support" board pipe ID (from .env)
        # This is the ONLY source board — all request types come from here.
        self.support_board_pipe_id = os.getenv('MEDIAMARK_SUPPORT_BOARD_PIPE_ID')
        
        # ---- State tracking ----
        self.card_states: Dict[str, str] = {}
        self.card_board_types: Dict[str, str] = {}
        self.card_assignees: Dict[str, Set[str]] = {}
        self.monitored_cards: Set[str] = set()
        self._last_processed: Dict[str, float] = {}
        self._dedup_window = 30  # seconds — longer than webhook processing to avoid double-processing
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        
        logger.info(f"MediamarkCardChangeListener initialized (poll interval: {self.poll_interval}s)")
    
    def mark_recently_processed(self, card_id: str):
        """Mark a card as recently processed (e.g. by a webhook) to prevent duplicate polling."""
        with self._lock:
            self._last_processed[card_id] = time.time()

    def _get_card_assignees(self, card: Dict) -> Set[str]:
        """Extract assignee IDs from a card as a Set."""
        assignees = set()
        for a in card.get('assignees', []):
            aid = str(a.get('id', ''))
            if aid and aid.isdigit():
                assignees.add(aid)
        return assignees
    
    def _get_card_hash(self, card: Dict) -> str:
        """Generate a hash of key card fields to detect changes."""
        hash_data = [
            (card.get('title') or '').strip(),
            card.get('due_date') or '',
            card.get('current_phase', {}).get('id') or '',
            tuple(sorted([str(a.get('id', '')) for a in card.get('assignees', [])])),
            tuple(sorted([str(l.get('id', '')) for l in card.get('labels', [])])),
        ]
        data_str = '|'.join([str(x) for x in hash_data])
        return hashlib.md5(data_str.encode()).hexdigest()
    
    def _is_active_phase(self, phase_name: str, board_type: str = 'support') -> bool:
        """Check if a phase is active (not a terminal/done phase)."""
        if not phase_name:
            return True

        phase_lower = phase_name.lower().strip()
        if phase_lower in MEDIAMARK_TERMINAL_PHASES:
            return False

        return any(
            phase_lower == active_phase or active_phase in phase_lower or phase_lower in active_phase
            for active_phase in MEDIAMARK_ACTIVE_PHASES
        )
    
    def _fetch_active_cards(self, pipe_id: str, board_type: str) -> List[Dict]:
        """Fetch all active cards from a Mediamark pipe using pagination."""
        query = """
        query GetPipeCards($pipeId: ID!, $after: String) {
            cards(pipe_id: $pipeId, first: 50, after: $after) {
                pageInfo { hasNextPage endCursor }
                edges {
                    node {
                        id title due_date
                        current_phase { id name }
                        labels { id name }
                        assignees { id name email }
                        fields { field { id label } name value }
                        updated_at
                    }
                }
            }
        }
        """
        all_cards = []
        has_next = True
        cursor = None
        
        try:
            while has_next:
                result = self.mediamark_client.execute_query(query, {
                    'pipeId': pipe_id,
                    'after': cursor
                })
                
                data = result.get('data', {}).get('cards', {})
                edges = data.get('edges', [])
                
                for edge in edges:
                    card = edge['node']
                    phase_name = card.get('current_phase', {}).get('name', '')
                    if self._is_active_phase(phase_name, board_type):
                        all_cards.append(card)
                
                page_info = data.get('pageInfo', {})
                has_next = page_info.get('hasNextPage', False)
                cursor = page_info.get('endCursor')
                
        except Exception as e:
            logger.error(f"Error fetching cards from Mediamark: {e}")
        
        return all_cards
    
    def _check_for_changes(self) -> List[Dict]:
        """Fetch all active cards from the Workflow Support board, compare to previous state."""
        changes = []
        all_cards = []
        
        if self.support_board_pipe_id:
            all_cards = self._fetch_active_cards(self.support_board_pipe_id, 'support')
        
        for card in all_cards:
            card_id = card['id']
            # All cards come from the single Workflow Support board.
            # The sync engine will check the "Support request type" field
            # to determine if it's a CR or support ticket.
            board_type = 'support'
            new_hash = self._get_card_hash(card)
            new_assignees = self._get_card_assignees(card)
            
            with self._lock:
                old_hash = self.card_states.get(card_id)
                old_assignees = self.card_assignees.get(card_id, set())
                
                if old_hash is None:
                    # First time seeing this card
                    self.card_states[card_id] = new_hash
                    self.card_board_types[card_id] = board_type
                    self.card_assignees[card_id] = new_assignees
                    self.monitored_cards.add(card_id)
                elif old_hash != new_hash:
                    # Change detected!
                    change_info = {
                        'card_id': card_id,
                        'board_type': board_type,
                        'title': card.get('title'),
                        'phase': card.get('current_phase', {}).get('name')
                    }
                    
                    added_assignees = new_assignees - old_assignees
                    removed_assignees = old_assignees - new_assignees
                    
                    if added_assignees or removed_assignees:
                        change_info['assignee_change'] = {
                            'added': list(added_assignees),
                            'removed': list(removed_assignees)
                        }
                    
                    changes.append(change_info)
                    self.card_states[card_id] = new_hash
                    self.card_assignees[card_id] = new_assignees
        
        # Detect removed cards (moved to done)
        current_card_ids = set(c['id'] for c in all_cards)
        
        with self._lock:
            removed = self.monitored_cards - current_card_ids
            
            for card_id in removed:
                board_type = self.card_board_types.get(card_id)
                if board_type:
                    changes.append({
                        'card_id': card_id,
                        'board_type': board_type,
                        'title': 'Completed Card',
                        'phase': 'done',
                        'is_completion': True
                    })
                self.card_states.pop(card_id, None)
                self.card_board_types.pop(card_id, None)
                self.card_assignees.pop(card_id, None)
                self.monitored_cards.discard(card_id)
        
        return changes
    
    def _process_change(self, change: Dict):
        """Handle a detected change by calling the appropriate sync method."""
        card_id = change['card_id']
        
        current_time = time.time()
        with self._lock:
            last_time = self._last_processed.get(card_id, 0)
            if current_time - last_time < self._dedup_window:
                return
            self._last_processed[card_id] = current_time
        
        if change.get('is_completion', False):
            # Card moved to Done — close all DYDX cards
            self.sync_service.handle_support_completed(card_id)
        elif change.get('assignee_change'):
            # Card-level assignee added/removed — Pipefy doesn't fire a webhook
            # for this, so the listener is the only way to detect it.
            ac = change['assignee_change']
            logger.info(
                f"Listener detected assignee change on MM card {card_id}: "
                f"added={ac.get('added', [])}, removed={ac.get('removed', [])}"
            )
            self.sync_service.handle_assignee_change(card_id, 'support_ticket')
        else:
            # Field-level changes on the MM card are intentionally
            # NOT synced to DYDX.  Only card.create (via webhook)
            # and card.move (via webhook) are actionable events.
            logger.debug(f"Listener detected change on MM card {card_id} — ignoring (field updates disabled)")
    
    def _poll_loop(self):
        """Main loop running in background thread."""
        logger.info(f" Mediamark card listener started - polling every {self.poll_interval} seconds")
        time.sleep(2)
        
        while self._running:
            try:
                changes = self._check_for_changes()
                for change in changes:
                    try:
                        self._process_change(change)
                    except ValueError as e:
                        # Webhook already processed this change — not an error.
                        logger.debug(f"Poll skipped (webhook already handled): {e}")
            except Exception as e:
                logger.error(f"Error in Mediamark poll loop: {e}", exc_info=True)
            time.sleep(self.poll_interval)
    
    def start(self):
        """Start the background polling thread."""
        if self._running: return
        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        logger.info(" Mediamark card change listener started")
    
    def stop(self):
        """Stop the background polling thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Mediamark card change listener stopped")
    
    def get_status(self) -> Dict:
        """Return current listener status."""
        with self._lock:
            return {
                'running': self._running,
                'poll_interval': self.poll_interval,
                'monitored_cards_count': len(self.monitored_cards)
            }
    
    def force_check(self, card_id: str = None) -> Dict:
        """Force an immediate check."""
        if card_id:
            board_type = self.card_board_types.get(card_id)
            if not board_type: return {'error': f'Card {card_id} not being monitored'}
            try:
                card_data = self.mediamark_client.get_card(card_id)
                card = card_data['data']['card']
                new_hash = self._get_card_hash(card)
                with self._lock:
                    old_hash = self.card_states.get(card_id)
                    changed = old_hash != new_hash
                    if changed:
                        self.card_states[card_id] = new_hash
                        self._process_change({
                            'card_id': card_id, 'board_type': board_type,
                            'title': card.get('title'), 'phase': card.get('current_phase', {}).get('name')
                        })
                return {'card_id': card_id, 'changed': changed, 'synced': changed}
            except Exception as e: return {'error': str(e)}
        else:
            changes = self._check_for_changes()
            for change in changes: self._process_change(change)
            return {'changes_detected': len(changes), 'changes': changes}


# ==========================================
# SINGLETON — One listener instance
# ==========================================

_listener_instance: Optional[MediamarkCardChangeListener] = None

def get_listener(sync_service=None, mediamark_client=None) -> Optional[MediamarkCardChangeListener]:
    """Get the existing listener instance (or create one if args provided)."""
    global _listener_instance
    if _listener_instance is None and sync_service and mediamark_client:
        _listener_instance = MediamarkCardChangeListener(sync_service, mediamark_client)
    return _listener_instance

def start_listener(sync_service, mediamark_client) -> MediamarkCardChangeListener:
    """Create (if needed) and start the listener."""
    global _listener_instance
    if _listener_instance is None:
        _listener_instance = MediamarkCardChangeListener(sync_service, mediamark_client)
    _listener_instance.start()
    return _listener_instance

def stop_listener():
    """Stop the listener if running."""
    global _listener_instance
    if _listener_instance:
        _listener_instance.stop()
