#!/usr/bin/env python3


import os
import re
import time
import json
import threading
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Set, Tuple
from utils.pipefy_client import PipefyClient

from integrations.field_mappings import (
    CHANGE_REQUEST_TO_DYDX_MAPPING, SUPPORT_TO_DYDX_MAPPING,
    HARDCODED_CLIENT_NAME, HARDCODED_PARTNER_ID, HARDCODED_SYSTEM_TYPE,
    PROJECT_NAME_PREFIX, TITLE_PREFIX_CR, TITLE_PREFIX_SUPPORT,
    CR_DESCRIPTION_FIELD_PATTERNS, SUPPORT_DESCRIPTION_FIELD_PATTERNS,
    TYPE_OF_REQUEST_FIELD_PATTERNS, SYSTEM_FIELD_PATTERNS,
    SUPPORT_REQUEST_TYPE_FIELD_PATTERNS, CR_REQUEST_TYPES
)

logger = logging.getLogger(__name__)

class MediamarkSync:
    """
Sync cards from Mediamark → DYDX Development Tasks Management board.
    
PHASE MAPPING CONSTANTS
These define which Mediamark phase names map to which DYDX board phases.

Mediamark phases: NEW, REVIEW, ESCALATED, SOW and Scoping, CLIENT APPROVAL, BACKLOG, IN PROGRESS, COMMS TO CLIENT, RESOLVED, NOT APPROVED, CHANGE REQUEST ON HOLD

    """
    SUPPORT_ESCALATED_PHASES = ['escalated']
    SUPPORT_TESTING_PHASES = ['comms to client']
    SUPPORT_COMMS_PHASES = ['comms to client']
    SUPPORT_COMPLETED_PHASES = ['resolved']
    SUPPORT_IN_PROGRESS_PHASES = ['in progress', 'review', 'new']
    SUPPORT_BACKLOG_PHASES = ['backlog']
    
    # --- CHANGE REQUEST (CR) phase groups ---
    CR_EXCLUDED_PHASES = ['client approval']
    CR_COMPLETED_PHASES = ['resolved']
    CR_CANCELLED_PHASES = ['not approved']
    
    # ---- Deduplication settings ----
    SYNC_DEDUP_WINDOW = 10 # seconds to prevent duplicate syncs from rapid consecutive webhooks 
    CREATION_COOLDOWN = 30 # seconds to prevent creating multiple DYDX cards for the same source card/assignee/phase combo
    FALLBACK_ASSIGNEE_EMAIL = 'co-creation.support@dydx.digital'
    
    def __init__(self):
        """
        Initialize the Mediamark → DYDX sync engine.
        
        1. Creates API clients for Mediamark + DYDX
        2. Fetches the DYDX board's phase IDs
        3. Fetches the DYDX board's label IDs
        4. Sets up thread-safety locks
        """
        mediamark_key = os.getenv('MEDIAMARK_API_KEY')
        dydx_key = os.getenv('DYDX_API_KEY')
        if not mediamark_key:
            raise ValueError("MEDIAMARK_API_KEY not found in environment variables")
        if not dydx_key:
            raise ValueError("DYDX_API_KEY not found in environment variables")

        self.mediamark_client = PipefyClient(mediamark_key, org_name="mediamark")
        self.dydx_client = PipefyClient(dydx_key, org_name="dydx")

        logger.info("Pipefy API clients initialized for Mediamark and DYDX")
        
    # The DYDX Development Tasks Management board pipe ID
        self.dydx_pipe_id = os.getenv('DYDX_DEV_TASKS_PIPE_ID')
        if not self.dydx_pipe_id:
            raise ValueError(
                " DYDX_DEV_TASKS_PIPE_ID not found!\n"
            )
        
        # Fetch board structure from DYDX
        self.dydx_phases = self._fetch_dydx_phases()
        self.priority_labels = self._fetch_dydx_labels()

        # We keep two user lookups:
        # 1) org users (all known users)
        # 2) pipe members (users actually assignable on this board)
        # Assignment uses pipe members to avoid creating cards with empty assignee fields.
        self.dydx_users_by_email = self._fetch_dydx_users_by_email()
        self.dydx_user_ids = {u['id'] for u in self.dydx_users_by_email.values() if u.get('id')}
        self.dydx_pipe_members_by_email = self._fetch_dydx_pipe_members_by_email()
        self.dydx_pipe_member_ids = {u['id'] for u in self.dydx_pipe_members_by_email.values() if u.get('id')}

        fallback_email = os.getenv('DYDX_ASSIGNEE_FALLBACK_EMAIL', self.FALLBACK_ASSIGNEE_EMAIL).strip().lower()
        fallback_info = self.dydx_pipe_members_by_email.get(fallback_email) or self.dydx_users_by_email.get(fallback_email)
        self.fallback_assignee_email = fallback_email
        self.fallback_assignee_id = (fallback_info or {}).get('id')
        self.fallback_assignee_name = (fallback_info or {}).get('name') or fallback_email
        if self.fallback_assignee_id:
            logger.info(f"Fallback assignee configured: {self.fallback_assignee_name} ({self.fallback_assignee_id})")
        else:
            logger.warning(f"Fallback assignee email '{fallback_email}' was not found in DYDX users/members")
        
        # Default partner = Jesslynn Shepherd
        self.default_partner = HARDCODED_PARTNER_ID
        
        # ---- Thread-safety: Locks and deduplication tracking ----
        self._sync_locks: Dict[str, threading.Lock] = {}
        self._sync_timestamps: Dict[str, float] = {}
        self._lock_manager = threading.Lock()
        self._recently_created: Dict[str, float] = {}
        self._last_known_phase: Dict[str, Dict] = {}
        self._processing_cards: set = set()
        
        logger.info(f"MediamarkSync initialized. Phases: {list(self.dydx_phases.keys())}")
    
    # ============================================
    # THREAD SAFETY — Locks and deduplication
    # ============================================
    # Prevents duplicate card creation from concurrent webhooks
    
    def _get_sync_lock(self, source_card_id: str) -> threading.Lock:
        """Get or create a thread lock for a specific source card."""
        with self._lock_manager:
            if source_card_id not in self._sync_locks:
                self._sync_locks[source_card_id] = threading.Lock()
            return self._sync_locks[source_card_id]
    
    def _should_skip_sync(self, source_card_id: str) -> bool:
        """Check if we just synced this card (within dedup window)."""
        current_time = time.time()
        with self._lock_manager:
            last_sync = self._sync_timestamps.get(source_card_id, 0)
            if current_time - last_sync < self.SYNC_DEDUP_WINDOW:
                return True
            return False
    
    def _record_sync(self, source_card_id: str):
        """Record that we just synced this card."""
        with self._lock_manager:
            self._sync_timestamps[source_card_id] = time.time()
    
    def _was_recently_created(self, source_card_id: str, assignee_id: str, dydx_phase: str) -> bool:
        """Check if a card was recently created for this source/assignee/phase combo."""
        key = f"{source_card_id}:{assignee_id}:{dydx_phase.lower()}"
        with self._lock_manager:
            last_created = self._recently_created.get(key, 0)
            if time.time() - last_created < self.CREATION_COOLDOWN:
                return True
            return False
    
    def _record_creation(self, source_card_id: str, assignee_id: str, dydx_phase: str):
        """Record that we just created a card for this combo."""
        key = f"{source_card_id}:{assignee_id}:{dydx_phase.lower()}"
        with self._lock_manager:
            self._recently_created[key] = time.time()
            cutoff = time.time() - 300
            self._recently_created = {k: v for k, v in self._recently_created.items() if v > cutoff}
    
    def _has_phase_changed(self, source_card_id: str, current_phase_name: str, current_phase_id: str) -> bool:
        """Check if the source card's phase has ACTUALLY changed since last check."""
        with self._lock_manager:
            last = self._last_known_phase.get(source_card_id)
            if not last:
                self._last_known_phase[source_card_id] = {
                    'phase_name': current_phase_name,
                    'phase_id': current_phase_id,
                    'timestamp': time.time()
                }
                return True
            
            if last['phase_id'] != current_phase_id or last['phase_name'].lower() != current_phase_name.lower():
                self._last_known_phase[source_card_id] = {
                    'phase_name': current_phase_name,
                    'phase_id': current_phase_id,
                    'timestamp': time.time()
                }
                return True
            
            return False
    
    def _start_processing(self, source_card_id: str) -> bool:
        """Mark a card as currently being processed (prevents re-entry)."""
        with self._lock_manager:
            if source_card_id in self._processing_cards:
                return False
            self._processing_cards.add(source_card_id)
            return True
    
    def _end_processing(self, source_card_id: str):
        """Mark card as done processing."""
        with self._lock_manager:
            self._processing_cards.discard(source_card_id)
    
    # ============================================
    # PHASE MAPPING — Mediamark phases → DYDX phases
    # ============================================
    
    def _phase_matches(self, phase_name: str, phase_list: List[str]) -> bool:
        """Case-insensitive substring matching for phase names."""
        if not phase_name: return False
        phase_lower = phase_name.lower().strip()
        return any(p in phase_lower or phase_lower in p for p in phase_list)
    
    def _is_excluded_cr_phase(self, phase_name: str) -> bool:
        return self._phase_matches(phase_name, self.CR_EXCLUDED_PHASES)
    
    def _get_dydx_card_status_phase(self, dydx_card: Dict) -> Optional[str]:
        """Get the main_task_status_name from a DYDX card."""
        for field in dydx_card.get('fields', []):
            if field.get('field', {}).get('id') == 'main_task_status_name':
                return field.get('value', '').lower().strip()
        return None
    
    def _dydx_card_matches_source_phase(self, dydx_card: Dict, source_phase_name: str) -> bool:
        """Check if DYDX card's status matches the source card's current phase."""
        dydx_status = self._get_dydx_card_status_phase(dydx_card)
        if not dydx_status:
            return False
        source_lower = source_phase_name.lower().strip()
        return dydx_status == source_lower or dydx_status in source_lower or source_lower in dydx_status
    
    def _get_correct_dydx_phase(self, source_phase: str) -> str:
        """Determine the correct DYDX phase for a given Mediamark source phase."""
        if not source_phase:
            return 'backlog'
        name = source_phase.lower().strip()
        
        # Exact-match against the real Mediamark phases first
        phase_to_dydx = {
            'new':                      'backlog',
            'review':                   'in progress',
            'escalated':                'in progress',
            'sow and scoping':          'in progress',
            'client approval':          'in progress',
            'backlog':                  'backlog',
            'in progress':              'in progress',
            'comms to client':          'testing / comms',
            'resolved':                 'done',
            'not approved':             'cancelled',
            'change request on hold':   'backlog',
        }
        
        if name in phase_to_dydx:
            return phase_to_dydx[name]
        
        logger.warning(f" Unknown Mediamark phase '{source_phase}' — defaulting to 'in progress'")
        return 'in progress'
    
    # ============================================
    # FETCH DYDX BOARD STRUCTURE
    # ============================================
    
    def _fetch_dydx_phases(self) -> Dict[str, str]:
        """Fetch all phases from the DYDX board. Returns dict: phase name (lower) → phase ID."""
        try:
            query = """query GetPipePhases($pipeId: ID!) { pipe(id: $pipeId) { phases { id name } } }"""
            result = self.dydx_client.execute_query(query, {'pipeId': self.dydx_pipe_id})
            phases = result.get('data', {}).get('pipe', {}).get('phases', [])
            return {p['name'].lower().strip(): p['id'] for p in phases}
        except Exception as e:
            logger.error(f"Failed to fetch DYDX phases: {e}")
            return {}
    
    def _fetch_dydx_labels(self) -> Dict[str, str]:
        """Fetch priority labels from the DYDX board. Falls back to low if specific priority labels not found."""
        try:
            query = """query GetPipeLabels($pipeId: ID!) { pipe(id: $pipeId) { labels { id name } } }"""
            result = self.dydx_client.execute_query(query, {'pipeId': self.dydx_pipe_id})
            labels = result.get('data', {}).get('pipe', {}).get('labels', [])
            label_map = {}
            for l in labels:
                name = l['name'].lower().strip()
                if 'very high' in name: label_map['very_high'] = l['id']
                elif 'high' in name: label_map['high'] = l['id']
                elif 'low' in name: label_map['low'] = l['id']
                elif 'hold' in name: label_map['on_hold'] = l['id']
            return label_map
        except Exception:
            return {}

    def _fetch_dydx_users_by_email(self) -> Dict[str, Dict[str, str]]:
        """Fetch DYDX org users and build lookup by email."""
        try:
            org_id = os.getenv('DYDX_ORG_ID', '293341')
            query = """
            query GetOrgUsers($orgId: ID!) {
                organization(id: $orgId) {
                    users { id name email }
                }
            }
            """
            result = self.dydx_client.execute_query(query, {'orgId': org_id})
            users = result.get('data', {}).get('organization', {}).get('users', [])
            by_email: Dict[str, Dict[str, str]] = {}
            for user in users:
                email = (user.get('email') or '').strip().lower()
                if email:
                    by_email[email] = {
                        'id': str(user.get('id') or ''),
                        'name': user.get('name') or email,
                    }
            return by_email
        except Exception as e:
            logger.warning(f"Failed to fetch DYDX users: {e}")
            return {}

    def _fetch_dydx_pipe_members_by_email(self) -> Dict[str, Dict[str, str]]:
        """Fetch DYDX board members (assignable users) and build lookup by email."""
        try:
            query = """
            query GetPipeMembers($pipeId: ID!) {
                pipe(id: $pipeId) {
                    members {
                        user { id name email }
                    }
                }
            }
            """
            result = self.dydx_client.execute_query(query, {'pipeId': self.dydx_pipe_id})
            members = result.get('data', {}).get('pipe', {}).get('members', [])
            by_email: Dict[str, Dict[str, str]] = {}
            for member in members:
                user = member.get('user', {})
                email = (user.get('email') or '').strip().lower()
                if email:
                    by_email[email] = {
                        'id': str(user.get('id') or ''),
                        'name': user.get('name') or email,
                    }
            return by_email
        except Exception as e:
            logger.warning(f"Failed to fetch DYDX pipe members: {e}")
            return {}
    
    # ============================================
    # MEDIAMARK-SPECIFIC: Request Type, Title, Description, Project Name
    # ============================================
    
    def _get_request_type(self, source_card: Dict) -> Optional[str]:
        """
        Read the "Support request type" field from the Mediamark card.
        
        Returns the exact dropdown value, e.g.:
          - "Issue/Question"
          - "Feature/change request"
          - "Access to system (for yourself)"
          - "New user request"
        Or None if not found.
        """
        for field in source_card.get('fields', []):
            field_id = field.get('field', {}).get('id', '').lower()
            field_label = field.get('field', {}).get('label', '').lower()
            field_name = field.get('name', '').lower()
            
            for pattern in SUPPORT_REQUEST_TYPE_FIELD_PATTERNS:
                if pattern in field_id or pattern in field_label or pattern in field_name:
                    val = field.get('value')
                    if val and str(val).strip():
                        return str(val).strip()
        return None
    
    def _is_change_request(self, source_card: Dict) -> bool:
        """
        Check if this card is a Change Request based on the "Support request type" field.
        Returns True if the request type is "Feature/change request".
        """
        request_type = self._get_request_type(source_card)
        if not request_type:
            return False
        return any(request_type.lower() == cr_type.lower() for cr_type in CR_REQUEST_TYPES)
    
    def _get_board_type_from_card(self, source_card: Dict) -> str:
        """
        Determine the effective board_type based on the "Support request type" field.
        Returns 'change_request' or 'support_ticket'.
        """
        if self._is_change_request(source_card):
            return 'change_request'
        return 'support_ticket'
    
    def _build_card_title(self, source_card: Dict, field_values: Dict, board_type: str) -> str:
        """
        Build the DYDX card title in format: [{Prefix}] Card Title.

        The prefix is the actual request type name from the Mediamark card (title-cased).
        The suffix varies by type to surface the most useful description.
        """
        def _first_field_value(patterns: List[str]) -> str:
            for field in source_card.get('fields', []):
                field_id = field.get('field', {}).get('id', '').lower()
                field_label = field.get('field', {}).get('label', '').lower()
                field_name = field.get('name', '').lower()
                value = field.get('value')
                if not value:
                    continue
                for pattern in patterns:
                    if pattern in field_id or pattern in field_label or pattern in field_name:
                        text = str(value).strip()
                        if text and text != '[]' and text.lower() != 'null':
                            return text
            return ''

        original_title = source_card.get('title', 'Untitled')
        
        request_type = self._get_request_type(source_card)

        if request_type:
            prefix = request_type.title()
            request_type_lower = request_type.lower().strip()
            if request_type_lower == 'feature/change request':
                suffix = (
                    str(field_values.get('feature_request') or '').strip()
                    or str(field_values.get('issues_question') or '').strip()
                    or original_title
                )
            elif request_type_lower == 'issue/question':
                suffix = (
                    str(field_values.get('issues_question') or '').strip()
                    or str(field_values.get('feature_request') or '').strip()
                    or original_title
                )
            elif request_type_lower == 'access to system (for yourself)':
                person_name = (
                    str(field_values.get('name') or '').strip()
                    or _first_field_value(['name'])
                )
                department = (
                    str(field_values.get('team_department') or '').strip()
                    or str(field_values.get('department') or '').strip()
                    or str(field_values.get('team') or '').strip()
                    or _first_field_value(['team/department', 'team department', 'department', 'team'])
                )
                if person_name and department:
                    suffix = f"{person_name} - {department}"
                else:
                    suffix = person_name or department or original_title
            else:
                suffix = original_title
        else:
            # If the request type field is missing, fall back to board_type-based prefix.
            prefix = TITLE_PREFIX_CR if board_type == 'change_request' else TITLE_PREFIX_SUPPORT
            suffix = original_title
        
        return f"[{prefix}] {suffix}"
    
    def _build_project_name(self, source_card: Dict, field_values: Dict) -> str:
        """
        Build the Project Name: "Mediamark Integration | {System Field Value/s}"
        
        The system field value comes from the source card's "system" field.
        If not found, just uses the prefix alone.
        """
        system_value = None
        
        for field in source_card.get('fields', []):
            field_id = field.get('field', {}).get('id', '').lower()
            field_label = field.get('field', {}).get('label', '').lower()
            field_name = field.get('name', '').lower()
            
            for pattern in SYSTEM_FIELD_PATTERNS:
                if pattern in field_id or pattern in field_label or pattern in field_name:
                    val = field.get('value')
                    if val:
                        # Handle array values (multi-select fields)
                        if isinstance(val, list):
                            system_value = ', '.join(str(v) for v in val if v)
                        else:
                            raw_system = str(val).strip()
                            if raw_system.startswith('[') and raw_system.endswith(']'):
                                try:
                                    parsed_system = json.loads(raw_system)
                                    if isinstance(parsed_system, list):
                                        system_value = ', '.join(str(v) for v in parsed_system if v)
                                    else:
                                        system_value = raw_system
                                except Exception:
                                    system_value = raw_system
                            else:
                                system_value = raw_system
                        break
            if system_value:
                break
        
        if system_value:
            return f"{PROJECT_NAME_PREFIX} | {system_value}"
        else:
            return PROJECT_NAME_PREFIX
    
    def _get_description_from_card(self, source_card: Dict, field_values: Dict, board_type: str = 'support_ticket') -> str:
        """
        Extract description from Mediamark source card.
        
        Uses the "Support request type" to pick the right description field:
          - "Feature/change request" → looks for CR description patterns
          - Everything else → looks for support/issue description patterns
        """
        # Determine patterns from the request type, not the board_type param
        is_cr = self._is_change_request(source_card)
        patterns = CR_DESCRIPTION_FIELD_PATTERNS if is_cr else SUPPORT_DESCRIPTION_FIELD_PATTERNS
        
        # First check direct keys in field_values
        for key in ['description', 'initial_notes', 'task_description']:
            if field_values.get(key):
                return str(field_values[key])
        
        # Search through all fields by label/name patterns
        for field in source_card.get('fields', []):
            field_id = field.get('field', {}).get('id', '').lower()
            field_label = field.get('field', {}).get('label', '').lower()
            field_name = field.get('name', '').lower()
            value = field.get('value')
            
            if not value:
                continue
            
            for pattern in patterns:
                if pattern in field_id or pattern in field_label or pattern in field_name:
                    if isinstance(value, dict):
                        continue
                    desc_value = str(value).strip()
                    if desc_value and desc_value != '[]' and desc_value != 'null':
                        return desc_value
        
    def get_combined_labels(self, card_data: Dict, field_values: Dict) -> List[str]:
        """Map source card labels to DYDX priority label IDs."""
        labels_to_apply = []
        source_labels = card_data.get('labels', [])
        has_priority = False
        
        for label in source_labels:
            l_name = label['name'].lower()
            if 'hold' in l_name and 'on_hold' in self.priority_labels:
                labels_to_apply.append(self.priority_labels['on_hold'])
            if 'high' in l_name or 'urgent' in l_name:
                if 'very_high' in self.priority_labels:
                    labels_to_apply.append(self.priority_labels['very_high'])
                    has_priority = True
            elif 'med' in l_name:
                if 'high' in self.priority_labels:
                    labels_to_apply.append(self.priority_labels['high'])
                    has_priority = True
            elif 'low' in l_name:
                if 'low' in self.priority_labels:
                    labels_to_apply.append(self.priority_labels['low'])
                    has_priority = True

        if not has_priority:
            size_value = field_values.get('estimated_update_size', '')
            if isinstance(size_value, list): size_value = size_value[0] if size_value else ''
            size_str = str(size_value).lower()
            if any(x in size_str for x in ['large', 'xl', 'big']):
                if 'very_high' in self.priority_labels: labels_to_apply.append(self.priority_labels['very_high'])
            elif any(x in size_str for x in ['medium', 'med']):
                if 'high' in self.priority_labels: labels_to_apply.append(self.priority_labels['high'])
            elif any(x in size_str for x in ['small', 'xs']):
                if 'low' in self.priority_labels: labels_to_apply.append(self.priority_labels['low'])

        if not labels_to_apply or all(l == self.priority_labels.get('on_hold') for l in labels_to_apply):
            if 'low' in self.priority_labels:
                has_actual = any(l in [self.priority_labels.get('low'), self.priority_labels.get('high'), self.priority_labels.get('very_high')] for l in labels_to_apply)
                if not has_actual: labels_to_apply.append(self.priority_labels['low'])

        return list(set(labels_to_apply))
    
    def extract_field_values(self, card: Dict) -> Dict[str, Any]:
        """Extract all field values from a source card into a flat dict."""
        values = {}
        for field in card.get('fields', []):
            if field.get('value') is not None:
                values[field['field']['id']] = field['value']
        if card.get('due_date'):
            values['due_date'] = card.get('due_date')
        return values
    
    def _validate_iso_date(self, date_str: str) -> Optional[str]:
        """Validate and return an ISO date string, or None if invalid."""
        if not date_str:
            return None
        try:
            from datetime import datetime as dt
            clean = date_str.replace('Z', '')
            if 'T' in clean:
                dt.strptime(clean[:19], '%Y-%m-%dT%H:%M:%S')
            else:
                dt.strptime(clean[:10], '%Y-%m-%d')
            return date_str
        except ValueError:
            logger.warning(f" Invalid date format rejected: {date_str}")
            return None

    def _add_hours_to_date(self, date_str: str, hours: int) -> Optional[str]:
        """Add hours to a date string and return ISO format."""
        if not date_str:
            return None
        try:
            clean = date_str.replace('Z', '+00:00')
            if 'T' in clean:
                dt_obj = datetime.fromisoformat(clean) if '+' in clean or '-' in clean[10:] else datetime.fromisoformat(clean)
            else:
                dt_obj = datetime.fromisoformat(clean[:10] + 'T00:00:00')
            dt_obj = dt_obj + timedelta(hours=hours)
            result = dt_obj.strftime('%Y-%m-%dT%H:%M:%S') + 'Z'
            return result
        except Exception as e:
            logger.warning(f" Failed to add hours to date {date_str}: {e}")
            return self._validate_iso_date(date_str)

    def get_source_due_date(self, source_card: Dict, field_values: Dict) -> Optional[str]:
        """Extract the due date from a Mediamark source card."""
        raw_val = field_values.get('due_date') or field_values.get('date_of_implementation')
        if not raw_val:
            for f in source_card.get('fields', []):
                name = f.get('name', '').lower().strip()
                if 'due date' in name:
                    raw_val = f.get('value')
                    if raw_val: break
        if not raw_val: raw_val = source_card.get('due_date')
        
        if not raw_val:
            created_at = source_card.get('createdAt')
            if created_at:
                return self._add_hours_to_date(created_at, 6)
            return None

        if isinstance(raw_val, list): 
            raw_val = raw_val[0]
            if isinstance(raw_val, list): raw_val = raw_val[0]

        date_str = str(raw_val).strip().strip('"').strip("'")
        try:
            if '-' in date_str and len(date_str) >= 10:
                if len(date_str) == 10:
                    result = f"{date_str}T09:00:00Z"
                else:
                    result = date_str
                return self._validate_iso_date(result)
            
            if '/' in date_str:
                clean_date = date_str.split(' ')[0]
                parts = clean_date.split('/')
                if len(parts) == 3:
                    month = parts[0].zfill(2)
                    day = parts[1].zfill(2)
                    year = parts[2]
                    if len(year) == 2:
                        year = '20' + year
                    result = f"{year}-{month}-{day}T09:00:00Z"
                    return self._validate_iso_date(result)
        except Exception as e:
            logger.warning(f" Date parsing error: {e}")
        
        created_at = source_card.get('createdAt')
        if created_at:
            return self._add_hours_to_date(created_at, 6)
        return None
    
    # ============================================
    # ASSIGNEE EXTRACTION
    # ============================================
    
    def _resolve_user_name_to_id(self, name_or_id: str) -> Optional[str]:
        """Resolve a name or ID string to a Pipefy user ID.
        
        Pipefy assignee fields normally return numeric IDs.
        If we get a non-numeric string (a display name), we log it
        and skip — the card-level assignees (which always have IDs)
        will be used instead.
        """
        if not name_or_id:
            return None
        name_or_id = str(name_or_id).strip()
        if name_or_id.isdigit():
            return name_or_id
        # Non-numeric value — this is a display name, not an ID.
        # Pipefy assignee_select fields should return IDs, so this
        # is an edge case. Log it and move on.
        logger.debug(f"Skipping non-numeric assignee value: '{name_or_id}' (expected a numeric ID)")
        return None

    def _extract_user_ids_from_field(self, field: Dict) -> List[str]:
        """Extract user IDs from an assignee-type field."""
        user_ids = []
        array_val = field.get('array_value')
        if array_val and isinstance(array_val, list):
            for item in array_val:
                if isinstance(item, str):
                    resolved = self._resolve_user_name_to_id(item)
                    if resolved: user_ids.append(resolved)
                elif isinstance(item, dict):
                    uid = item.get('id') or item.get('value')
                    if uid:
                        resolved = self._resolve_user_name_to_id(str(uid))
                        if resolved: user_ids.append(resolved)
        
        value = field.get('value')
        if value and not user_ids:
            if isinstance(value, str):
                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, list):
                        for item in parsed:
                            if isinstance(item, str):
                                resolved = self._resolve_user_name_to_id(item)
                                if resolved: user_ids.append(resolved)
                            elif isinstance(item, dict):
                                uid = item.get('id') or item.get('value')
                                if uid:
                                    resolved = self._resolve_user_name_to_id(str(uid))
                                    if resolved: user_ids.append(resolved)
                except:
                    resolved = self._resolve_user_name_to_id(value)
                    if resolved: user_ids.append(resolved)
        
        return user_ids

    def _get_assignee_ids(self, source_card: Dict, field_values: Dict, is_backlog: bool = False) -> List[str]:
        """
        Extract assignee IDs from the Mediamark source card.
        Checks card assignees, then field-level assignees.
        """
        assignee_ids = []
        
        # 1. Get from card-level assignees
        card_assignees = source_card.get('assignees', [])
        for a in card_assignees:
            aid = str(a.get('id', ''))
            if aid and aid.isdigit():
                assignee_ids.append(aid)
        
        # 2. Get from assignee fields
        assignee_keywords = ['assignee', 'assign', 'team member', 'responsible', 'owner']
        for field in source_card.get('fields', []):
            field_id = field.get('field', {}).get('id', '').lower()
            field_label = field.get('field', {}).get('label', '').lower()
            field_name = field.get('name', '').lower()
            
            is_assignee = any(kw in field_id or kw in field_label or kw in field_name for kw in assignee_keywords)
            if is_assignee:
                ids = self._extract_user_ids_from_field(field)
                assignee_ids.extend(ids)
                
                val = field.get('value')
                if val and isinstance(val, str):
                    try:
                        parsed = json.loads(val)
                        if isinstance(parsed, list):
                            for item in parsed:
                                if isinstance(item, str):
                                    resolved = self._resolve_user_name_to_id(item)
                                    if resolved: assignee_ids.append(resolved)
                    except:
                        resolved = self._resolve_user_name_to_id(val)
                        if resolved: assignee_ids.append(resolved)
        
        clean_ids = list(set(aid for aid in assignee_ids if aid and str(aid).isdigit()))
        
        # Fallback to card creator if no assignees found
        if not clean_ids:
            creator = source_card.get('createdBy')
            if creator and creator.get('id'):
                clean_ids.append(str(creator['id']))
                
        return list(set(clean_ids))

    def _get_user_name_by_id(self, user_id: str) -> str:
        """Return a display label for a Pipefy user ID.
        
        Since we don't maintain a name lookup table, this just returns
        a formatted ID string. The actual user name is visible in Pipefy UI.
        """
        return f"User-{user_id}"

    def _resolve_dydx_assignee(self, source_card: Dict, source_assignee_id: str) -> Tuple[Optional[str], str]:
        """Resolve Mediamark assignee to a DYDX board-member user ID by email."""
        source_assignee_id = str(source_assignee_id or '').strip()

        # Fast path: if the incoming ID is already a DYDX board member, use it directly.
        if source_assignee_id in self.dydx_pipe_member_ids:
            for info in self.dydx_pipe_members_by_email.values():
                if info.get('id') == source_assignee_id:
                    return source_assignee_id, info.get('name') or f"User-{source_assignee_id}"
            return source_assignee_id, f"User-{source_assignee_id}"

        emails_to_try: List[str] = []
        matched_source_assignee = False

        # Otherwise, map Mediamark assignee -> email -> DYDX board member.
        for assignee in source_card.get('assignees', []):
            if str(assignee.get('id') or '') != source_assignee_id:
                continue
            matched_source_assignee = True
            email = (assignee.get('email') or '').strip().lower()
            if email:
                emails_to_try.append(email)

        # Fallback source for assignee email: Work Email / Email fields on the card.
        # Only use this when no explicit source assignee object was matched.
        if not matched_source_assignee:
            for field in source_card.get('fields', []):
                field_id = field.get('field', {}).get('id', '').lower()
                field_label = field.get('field', {}).get('label', '').lower()
                field_name = field.get('name', '').lower()
                if not any(p in field_id or p in field_label or p in field_name for p in ['work_email', 'work email', 'email']):
                    continue
                raw_val = field.get('value')
                if raw_val is None:
                    continue
                text = str(raw_val).strip().lower()
                if not text:
                    continue
                matches = re.findall(r"[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}", text)
                emails_to_try.extend(matches)

        seen: Set[str] = set()
        for email in emails_to_try:
            if not email or email in seen:
                continue
            seen.add(email)
            if email in self.dydx_pipe_members_by_email:
                info = self.dydx_pipe_members_by_email[email]
                return info['id'], info['name']

        if self.fallback_assignee_id:
            return self.fallback_assignee_id, self.fallback_assignee_name
        return None, ''

    def _get_priority_label_id_from_source(self, source_card: Dict) -> Optional[str]:
        """Resolve priority from source card labels (Low/High/Very high)."""
        # Priority is taken from source card labels by requirement.
        # If no matching label is present, default to low.
        labels = source_card.get('labels', []) or []
        for label in labels:
            name = (label.get('name') or '').strip().lower()
            if 'very high' in name and self.priority_labels.get('very_high'):
                return self.priority_labels['very_high']
            if name == 'high' and self.priority_labels.get('high'):
                return self.priority_labels['high']
            if name == 'low' and self.priority_labels.get('low'):
                return self.priority_labels['low']

        if self.priority_labels.get('low'):
            return self.priority_labels['low']
        if self.priority_labels.get('high'):
            return self.priority_labels['high']
        if self.priority_labels.get('very_high'):
            return self.priority_labels['very_high']
        return None
    
    # ============================================
    # FIND EXISTING DYDX CARDS
    # ============================================
    
    def find_active_dydx_card_by_source_id(self, source_card_id: str) -> Optional[Dict]:
        """Find the first active DYDX card linked to a source card."""
        cards = self.find_all_active_dydx_cards_by_source_id(source_card_id)
        return cards[0] if cards else None
    
    def find_all_active_dydx_cards_by_source_id(self, source_card_id: str) -> List[Dict]:
        """Find ALL active DYDX cards linked to a Mediamark source card via main_task_id field."""
        query = """query GetPipeCards($pipeId: ID!, $after: String) { 
            cards(pipe_id: $pipeId, first: 50, after: $after) { 
                pageInfo { hasNextPage endCursor }
                edges { node { 
                    id title url createdAt due_date
                    current_phase { id name } 
                    assignees { id name email }
                    labels { id name }
                    fields { field { id } value } 
                } } 
            } 
        }"""
        
        has_next = True
        cursor = None
        done_phase_id = self.dydx_phases.get('done')
        matching_cards = []
        
        try:
            while has_next:
                variables = {'pipeId': self.dydx_pipe_id, 'after': cursor}
                result = self.dydx_client.execute_query(query, variables)
                
                data = result.get('data', {}).get('cards', {})
                edges = data.get('edges', [])
                
                for edge in edges:
                    card = edge['node']
                    if card.get('current_phase', {}).get('id') == done_phase_id: 
                        continue
                        
                    for field in card.get('fields', []):
                        if field['field']['id'] == 'main_task_id':
                            if str(field.get('value', '')).strip() == str(source_card_id).strip():
                                card_assignees = card.get('assignees', [])
                                if card_assignees:
                                    card['_assignee_id'] = str(card_assignees[0].get('id', ''))
                                else:
                                    card['_assignee_id'] = None
                                matching_cards.append(card)
                                break
                
                page_info = data.get('pageInfo', {})
                has_next = page_info.get('hasNextPage', False)
                cursor = page_info.get('endCursor')
                
        except Exception as e:
            logger.error(f"Error finding active DYDX cards for MM card {source_card_id}: {e}")
            return []
            
        return matching_cards

    def find_dydx_card_for_assignee(self, source_card_id: str, assignee_id: str) -> Optional[Dict]:
        """Find the DYDX card for a specific source card + assignee combo."""
        all_cards = self.find_all_active_dydx_cards_by_source_id(source_card_id)
        for card in all_cards:
            card_assignee_id = card.get('_assignee_id')
            if card_assignee_id and str(card_assignee_id).strip() == str(assignee_id).strip():
                return card
        return None
    
    # ============================================
    # CREATE & CLOSE DYDX CARDS
    # ============================================
    
    def create_dydx_card_for_assignee(self, source_card: Dict, board_type: str, assignee_id: str, target_phase: str = None, skip_duplicate_check: bool = False) -> Optional[Dict]:
        """
        Create a DYDX Development Tasks card for one assignee.
        
        MEDIAMARK-SPECIFIC:
        - Title: [{Type of request}] Card Title
        - Client Name: "Mediamark" (hardcoded)
        - Project Name: "Mediamark Integration | {System Field}"
        - System Type: "Pipefy" (hardcoded)
        """
        source_card_id = source_card['id']
        field_values = self.extract_field_values(source_card)
        
        final_target_phase = target_phase
        current_phase_obj = source_card.get('current_phase', {})
        current_phase_name = current_phase_obj.get('name', '')
        current_phase_id = current_phase_obj.get('id', '')
        
        if not final_target_phase:
            final_target_phase = self._get_correct_dydx_phase(current_phase_name)
        
        # DUPLICATE CHECK
        if self._was_recently_created(source_card_id, assignee_id, final_target_phase):
            existing = self.find_dydx_card_for_assignee(source_card_id, assignee_id)
            return existing
        
        if not skip_duplicate_check:
            existing_card = self.find_dydx_card_for_assignee(source_card_id, assignee_id)
            if existing_card:
                raise ValueError(
                    f"DYDX card already exists for source card {source_card_id} "
                    f"(assignee {assignee_id}) → existing DYDX card {existing_card['id']}. "
                    f"No new card created."
                )
        
        dydx_assignee_id, dydx_assignee_name = self._resolve_dydx_assignee(source_card, assignee_id)
        if not dydx_assignee_id:
            logger.warning(f"MM card {source_card_id}: could not resolve DYDX assignee from source assignee {assignee_id}")
            return None
        
        # ---- Build DYDX card fields ----
        
        # Get description
        desc = self._get_description_from_card(source_card, field_values, board_type)
        
        # Get due date
        date_val = self.get_source_due_date(source_card, field_values)
        
        # Build the title with [{Type}] prefix
        title = self._build_card_title(source_card, field_values, board_type)
        
        # Build project name
        project_name = self._build_project_name(source_card, field_values)

        priority_value = self._get_priority_label_id_from_source(source_card)
        
        # Use the Mediamark card's creation date for date_added_to_board
        source_created_at = source_card.get('createdAt', '')
        if source_created_at:
            date_added_to_board = source_created_at.replace('Z', '').split('.')[0]
        else:
            date_added_to_board = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')

        # Assemble field values for DYDX card
        dydx_fields = [
            {'field_id': 'task_name', 'field_value': title},
            {'field_id': 'priority', 'field_value': str(priority_value) if priority_value else '315707448'},
            {'field_id': 'assignee', 'field_value': str(dydx_assignee_id)},  # required at creation
            {'field_id': 'partner', 'field_value': str(self.default_partner)},
            {'field_id': 'date_added_to_board', 'field_value': date_added_to_board},
            {'field_id': 'main_task_id', 'field_value': str(source_card_id)},
            {'field_id': 'client_name', 'field_value': HARDCODED_CLIENT_NAME},
            {'field_id': 'system_type', 'field_value': HARDCODED_SYSTEM_TYPE},
            {'field_id': 'project_name', 'field_value': project_name},
        ]
        
        if desc:
            dydx_fields.append({'field_id': 'task_description', 'field_value': desc})

        if source_card.get('url'):
            dydx_fields.append({'field_id': 'main_task_link', 'field_value': f"### [Open Task]({source_card['url']})"})

        if date_val:
            clean_date = date_val.replace('Z', '')
            dydx_fields.append({'field_id': 'due_date', 'field_value': clean_date})
            dydx_fields.append({'field_id': 'estimated_completion_date', 'field_value': clean_date})

        if current_phase_name:
            dydx_fields.append({'field_id': 'main_task_status_name', 'field_value': current_phase_name})
        if current_phase_id:
            dydx_fields.append({'field_id': 'main_task_status_id', 'field_value': current_phase_id})
        
        logger.info(
            f"Creating DYDX card for MM card {source_card_id} | "
            f"title='{title}', due_date={date_val}, "
            f"fields={json.dumps(dydx_fields, default=str)}"
        )
        result = self.dydx_client.create_card(
            self.dydx_pipe_id, 
            title, 
            dydx_fields, 
            due_date=date_val
        )
        new_card = result['data']['createCard']['card']
        logger.info(f"Created DYDX card {new_card['id']} for MM card {source_card_id} (assignee={dydx_assignee_name}, phase={final_target_phase})")
        
        self._record_creation(source_card_id, assignee_id, final_target_phase)
        
        # Assignee is already set via the 'assignee' field in dydx_fields above.
        # Calling set_card_assignees again would trigger a remove+add in Pipefy's activity log.
        
        if final_target_phase:
            target_phase_id = self.dydx_phases.get(final_target_phase.lower())
            if target_phase_id:
                try:
                    self.dydx_client.move_card_to_phase(new_card['id'], target_phase_id)
                except Exception as e:
                    if "already in the destination phase" not in str(e):
                        logger.warning(f"Failed to move new DYDX card {new_card['id']} (MM card {source_card_id}): {e}")
        
        return new_card

    def close_dydx_card(self, dydx_card_id: str, source_card: Dict = None) -> bool:
        """Close a DYDX card by moving it to the Done phase."""
        try:
            card = self.dydx_client.get_card(dydx_card_id)
            current_phase_id = card.get('data', {}).get('card', {}).get('current_phase', {}).get('id')
            done_phase_id = self.dydx_phases.get('done')
            if current_phase_id and done_phase_id and current_phase_id == done_phase_id:
                return True
        except Exception: pass

        try:
            self.dydx_client.update_card_field(dydx_card_id, 'test_environment', 'Production')
        except Exception: pass

        try:
            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT09:00:00Z")
            self.dydx_client.update_card_field(dydx_card_id, 'estimated_completion_date', today_str)
        except Exception: pass
        
        target_phase_id = self.dydx_phases.get('done')
        if target_phase_id:
            try:
                self.dydx_client.move_card_to_phase(dydx_card_id, target_phase_id)
                logger.info(f"Closed DYDX card {dydx_card_id}")
                return True
            except Exception as e:
                if "already in the destination phase" in str(e):
                    return True
                logger.error(f"Failed to close DYDX card {dydx_card_id} (move phase): {e}")
                return False
        return False
    
    # ============================================
    # SYNC CARD FIELDS
    # ============================================
    
    def _set_card_labels(self, card_id: str, label_ids: List[str]) -> Dict:
        """Set labels on a DYDX card."""
        mutation = """
        mutation UpdateCardLabels($cardId: ID!, $labelIds: [ID!]) {
            updateCard(input: { id: $cardId label_ids: $labelIds }) {
                card { id labels { id name } }
            }
        }
        """
        return self.dydx_client.execute_query(mutation, {
            'cardId': card_id,
            'labelIds': label_ids
        })
    
    def _update_card_title(self, card_id: str, title: str) -> Dict:
        """Update a DYDX card's title."""
        mutation = """
        mutation UpdateCard($cardId: ID!, $title: String!) {
            updateCard(input: { id: $cardId title: $title }) {
                card { id title }
            }
        }
        """
        return self.dydx_client.execute_query(mutation, {
            'cardId': card_id,
            'title': title
        })
    
    def _update_phase_tracking(self, dydx_card_id: str, source_card: Dict) -> None:
        """No-op: main_task_status_name/id are phase-locked on the DYDX board.
        
        These fields can only be written on the phase where the card was created.
        They are already set during card creation in create_dydx_card_for_assignee().
        Any attempt to update them after a move will always fail, so we skip entirely.
        """
        pass

    def _sync_card_fields(self, dydx_card_id: str, source_card: Dict, field_values: Dict, board_type: str, target_assignee_id: Optional[str] = None, dydx_card: Dict = None) -> None:
        """
        Sync fields from Mediamark source card to DYDX card.
        Only updates fields whose values have actually changed (diff-based).
        
        MEDIAMARK-SPECIFIC: Also syncs client name, project name, system type.
        """
        
        # Build a lookup of current DYDX field values for diff comparison
        current_dydx = {}
        if dydx_card:
            for f in dydx_card.get('fields', []):
                fid = f.get('field', {}).get('id', '')
                current_dydx[fid] = str(f.get('value', '') or '')
        
        def _changed(field_id: str, new_val) -> bool:
            """Return True only if the new value differs from the current DYDX value."""
            if not dydx_card:
                return True  # No existing card data — always update
            return str(new_val or '') != current_dydx.get(field_id, '')
        
        # Sync description
        try:
            desc = self._get_description_from_card(source_card, field_values, board_type)
            if desc and _changed('task_description', desc):
                self.dydx_client.update_card_field(dydx_card_id, 'task_description', desc)
        except Exception as e:
            logger.error(f"Failed to sync description for DYDX card {dydx_card_id}: {e}")
        
        # Sync hardcoded fields (Mediamark-specific) — only if changed
        try:
            if _changed('client_name', HARDCODED_CLIENT_NAME):
                self.dydx_client.update_card_field(dydx_card_id, 'client_name', HARDCODED_CLIENT_NAME)
        except Exception: pass
        
        try:
            if _changed('system_type', HARDCODED_SYSTEM_TYPE):
                self.dydx_client.update_card_field(dydx_card_id, 'system_type', HARDCODED_SYSTEM_TYPE)
        except Exception: pass
        
        try:
            project_name = self._build_project_name(source_card, field_values)
            if _changed('project_name', project_name):
                self.dydx_client.update_card_field(dydx_card_id, 'project_name', project_name)
        except Exception: pass

        try:
            if _changed('partner', str(self.default_partner)):
                self.dydx_client.update_card_field(dydx_card_id, 'partner', str(self.default_partner))
        except Exception: pass

        # Sync assignee — only if the current assignee list differs
        if target_assignee_id:
            try:
                current_assignees = set(str(a['id']) for a in (dydx_card or {}).get('assignees', []))
                if not dydx_card or current_assignees != {str(target_assignee_id)}:
                    self.dydx_client.set_card_assignees(dydx_card_id, [str(target_assignee_id)])
            except Exception as e:
                logger.error(f"Failed to sync assignee for DYDX card {dydx_card_id}: {e}")
        
        # Sync mapped fields — only if changed
        mapping = CHANGE_REQUEST_TO_DYDX_MAPPING if board_type == 'change_request' else SUPPORT_TO_DYDX_MAPPING
        for source_field, dydx_field in mapping.items():
            if dydx_field == 'priority':
                continue
            val = field_values.get(source_field)
            if val and _changed(dydx_field, val):
                try:
                    self.dydx_client.update_card_field(dydx_card_id, dydx_field, val)
                except Exception as e:
                    logger.error(f"Failed to sync field {dydx_field} for DYDX card {dydx_card_id}: {e}")

        # Sync labels/priority — only if changed
        try:
            labels = self.get_combined_labels(source_card, field_values)
            if labels:
                current_labels = set(str(l['id']) for l in (dydx_card or {}).get('labels', []))
                if not dydx_card or set(labels) != current_labels:
                    self._set_card_labels(dydx_card_id, labels)

            priority_value = self._get_priority_label_id_from_source(source_card)
            if priority_value and _changed('priority', str(priority_value)):
                self.dydx_client.update_card_field(dydx_card_id, 'priority', str(priority_value))
        except Exception as e:
            logger.error(f"Failed to sync labels/priority for DYDX card {dydx_card_id}: {e}")
        
        # Sync due date — only if changed
        try:
            date_val = self.get_source_due_date(source_card, field_values)
            if date_val:
                clean_date = date_val.replace('Z', '')
                # Compare against both the field value and card-level due_date
                current_card_due = (dydx_card or {}).get('due_date', '') or ''
                field_due_changed = _changed('due_date', clean_date)
                card_due_changed = not dydx_card or not current_card_due.startswith(clean_date[:19])
                
                if card_due_changed:
                    self.dydx_client.update_card_due_date(dydx_card_id, date_val)
                if field_due_changed:
                    self.dydx_client.update_card_field(dydx_card_id, 'due_date', clean_date)
                if _changed('estimated_completion_date', clean_date):
                    self.dydx_client.update_card_field(dydx_card_id, 'estimated_completion_date', clean_date)
        except Exception as e:
            logger.error(f"Failed to sync due date for DYDX card {dydx_card_id}: {e}")
    
    # ============================================
    # PER-ASSIGNEE SYNC LOGIC
    # ============================================
    
    def sync_assignees_to_dydx(self, source_card_id: str, board_type: str, target_phase: str = None, enable_move: bool = False, is_move_event: bool = False, source_card: Dict = None) -> Dict:
        """
        Core method for per-assignee sync (Mediamark → DYDX).
        Creates/closes/moves DYDX cards based on source card assignees.
        """
        sync_lock = self._get_sync_lock(source_card_id)
        
        if not sync_lock.acquire(blocking=False):
            return {'status': 'skipped_locked', 'created': [], 'closed': [], 'synced': []}
        
        try:
            if self._should_skip_sync(source_card_id):
                return {'status': 'skipped_dedup', 'created': [], 'closed': [], 'synced': []}
            
            # Fetch source card from Mediamark (skip if already provided)
            if source_card is None:
                source_data = self.mediamark_client.get_card(source_card_id)
                source_card = source_data['data']['card']
            field_values = self.extract_field_values(source_card)
            
            source_phase = source_card.get('current_phase', {}).get('name', '')
            correct_dydx_phase = target_phase or self._get_correct_dydx_phase(source_phase)
            is_backlog = 'backlog' in correct_dydx_phase.lower()
            
            # Source card assignee IDs belong to the Mediamark org.
            # For diffing against existing DYDX cards we must compare
            # using DYDX assignee IDs, otherwise moves look like
            # "removed + new" and cause close/recreate churn.
            source_assignee_ids = set(self._get_assignee_ids(source_card, field_values, is_backlog))
            current_assignee_ids = set()
            for source_assignee_id in source_assignee_ids:
                dydx_assignee_id, _ = self._resolve_dydx_assignee(source_card, source_assignee_id)
                if dydx_assignee_id:
                    current_assignee_ids.add(str(dydx_assignee_id))
            
            existing_dydx_cards = self.find_all_active_dydx_cards_by_source_id(source_card_id)
            dydx_cards_by_assignee: Dict[str, Dict] = {}
            cards_without_assignee = []
            
            for card in existing_dydx_cards:
                card_assignee_id = card.get('_assignee_id')
                if card_assignee_id:
                    dydx_cards_by_assignee[card_assignee_id] = card
                else:
                    cards_without_assignee.append(card)
        
            result = {'created': [], 'closed': [], 'synced': [], 'migrated': [], 'moved': []}
        
            # Ghost assignee handling
            if is_move_event and not current_assignee_ids and existing_dydx_cards:
                current_assignee_ids = set(dydx_cards_by_assignee.keys())
        
            if not current_assignee_ids:
                if self.fallback_assignee_id:
                    logger.info(f"MM card {source_card_id}: no assignee resolved, using fallback {self.fallback_assignee_name}")
                    current_assignee_ids = {str(self.fallback_assignee_id)}
                elif existing_dydx_cards:
                    current_assignee_ids = set(dydx_cards_by_assignee.keys())
                else:
                    logger.warning(f"MM card {source_card_id}: no assignee and no fallback, skipping")
                    return {'status': 'no_assignable_assignees', 'created': [], 'closed': [], 'synced': [], 'migrated': [], 'moved': []}
        
            # Close cards without assignees
            for old_card in cards_without_assignee:
                if not old_card or not old_card.get('id'):
                    continue
                self.close_dydx_card(old_card['id'], source_card)
                result['migrated'].append(old_card['id'])
        
            existing_assignee_ids = set(dydx_cards_by_assignee.keys())
            new_assignees = current_assignee_ids - existing_assignee_ids
            removed_assignees = existing_assignee_ids - current_assignee_ids
        
            # Create cards for new assignees (skip redundant duplicate scan — already checked above)
            for assignee_id in new_assignees:
                new_card = self.create_dydx_card_for_assignee(source_card, board_type, assignee_id, correct_dydx_phase, skip_duplicate_check=True)
                if new_card and new_card.get('id'):
                    result['created'].append(new_card['id'])
                else:
                    logger.warning(f"MM card {source_card_id}: no DYDX card created for assignee {assignee_id}")
        
            # Close cards for removed assignees
            for assignee_id in removed_assignees:
                dydx_card = dydx_cards_by_assignee[assignee_id]
                if not dydx_card or not dydx_card.get('id'):
                    continue
                self.close_dydx_card(dydx_card['id'], source_card)
                result['closed'].append(dydx_card['id'])
        
            # Sync/move remaining cards
            remaining_assignees = current_assignee_ids & existing_assignee_ids
            for assignee_id in remaining_assignees:
                dydx_card = dydx_cards_by_assignee[assignee_id]
                if not dydx_card or not dydx_card.get('id'):
                    continue
                dydx_phase = dydx_card.get('current_phase', {}).get('name', '').lower()
            
                if correct_dydx_phase.lower() not in dydx_phase and dydx_phase not in correct_dydx_phase.lower():
                    if enable_move:
                        target_phase_id = self.dydx_phases.get(correct_dydx_phase.lower())
                        if target_phase_id:
                            try:
                                self.dydx_client.move_card_to_phase(dydx_card['id'], target_phase_id)
                                logger.info(f"Moved DYDX card {dydx_card['id']}: '{dydx_phase}' → '{correct_dydx_phase}' (MM card {source_card_id})")
                                result['moved'].append(dydx_card['id'])
                            except Exception as e:
                                logger.error(f"Move failed for DYDX card {dydx_card['id']} (MM card {source_card_id}): {e}")
                                self.close_dydx_card(dydx_card['id'], source_card)
                                new_card = self.create_dydx_card_for_assignee(source_card, board_type, assignee_id, correct_dydx_phase)
                                result['closed'].append(dydx_card['id'])
                                if new_card and new_card.get('id'):
                                    result['created'].append(new_card['id'])
                    # Phase doesn't match but move not enabled — just record as synced
                    else:
                        result['synced'].append(dydx_card['id'])
                else:
                    # Phase already matches — diff-sync only changed fields
                    self._sync_card_fields(dydx_card['id'], source_card, field_values, board_type, target_assignee_id=None, dydx_card=dydx_card)
                    result['synced'].append(dydx_card['id'])
        
            self._record_sync(source_card_id)
            return result
            
        finally:
            sync_lock.release()

    def _handle_field_update(self, source_card_id: str, board_type: str) -> Dict:
        """Handle field updates: only sync changed fields on existing DYDX cards.
        
        Unlike card.create or card.move, field updates should NOT re-evaluate
        assignees, create new cards, or close old ones.  We simply find
        existing linked DYDX cards and push any changed field values.
        """
        if not self._start_processing(source_card_id):
            return {'updated': False, 'reason': 'already_processing'}
        
        try:
            source_data = self.mediamark_client.get_card(source_card_id)
            source_card = source_data['data']['card']
            field_values = self.extract_field_values(source_card)
            
            existing_dydx_cards = self.find_all_active_dydx_cards_by_source_id(source_card_id)
            if not existing_dydx_cards:
                return {'updated': False, 'reason': 'no_dydx_cards'}
            
            synced = []
            for dydx_card in existing_dydx_cards:
                dydx_card_id = dydx_card.get('id')
                if not dydx_card_id:
                    continue
                assignee_id = dydx_card.get('_assignee_id')
                self._sync_card_fields(
                    dydx_card_id, source_card, field_values, board_type,
                    target_assignee_id=None,  # don't touch assignees on field updates
                    dydx_card=dydx_card,
                )
                synced.append(dydx_card_id)
            
            self._record_sync(source_card_id)
            return {'updated': True, 'synced': synced}
        
        finally:
            self._end_processing(source_card_id)

    # ============================================
    # HANDLER METHODS — Called by webhooks
    # ============================================

    def _format_action_result(self, action: str, result: Dict, default_status: str = None) -> Dict:
        """Normalize response status so it reflects the requested action outcome."""
        payload = dict(result or {})
        created = len(payload.get('created', []) or [])
        moved = len(payload.get('moved', []) or [])
        synced = len(payload.get('synced', []) or [])
        closed = len(payload.get('closed', []) or [])

        if action == 'card.create':
            if created > 0:
                payload['status'] = 'created'
            elif moved > 0 or synced > 0:
                payload['status'] = 'already_exists_synced'
            else:
                payload['status'] = default_status or payload.get('status') or 'create_no_change'
        elif action == 'card.move':
            if moved > 0:
                payload['status'] = 'moved'
            elif created > 0:
                payload['status'] = 'move_created_new_card'
            elif synced > 0:
                payload['status'] = 'already_in_target_phase_synced'
            else:
                payload['status'] = 'move_no_change'
        elif default_status and not payload.get('status'):
            payload['status'] = default_status

        payload['requested_action'] = action
        payload['summary'] = {
            'created': created,
            'moved': moved,
            'synced': synced,
            'closed': closed,
        }
        return payload

    def handle_support_card_created(self, source_card_id: str) -> Dict:
        result = self.sync_assignees_to_dydx(source_card_id, 'support_ticket', is_move_event=True)
        return {'status': 'created', **result}

    def handle_support_in_progress(self, source_card_id: str) -> Dict:
        result = self.sync_assignees_to_dydx(source_card_id, 'support_ticket', target_phase='in progress', enable_move=True, is_move_event=True)
        return {'status': 'in_progress', **result}

    def handle_support_escalated(self, source_card_id: str) -> Dict:
        result = self.sync_assignees_to_dydx(source_card_id, 'support_ticket', target_phase='in progress', enable_move=False, is_move_event=True)
        return {'status': 'escalated', **result}

    def handle_support_testing(self, source_card_id: str) -> Dict:
        result = self.sync_assignees_to_dydx(source_card_id, 'support_ticket', target_phase='testing / comms', enable_move=False, is_move_event=True)
        return {'status': 'testing', **result}

    def handle_support_completed(self, source_card_id: str) -> Dict:
        all_cards = self.find_all_active_dydx_cards_by_source_id(source_card_id)
        if not all_cards: 
            return {'status': 'no_cards_found'}
        
        source_data = self.mediamark_client.get_card(source_card_id)
        source_card = source_data['data']['card']
        
        closed_ids = []
        for card in all_cards:
            self.close_dydx_card(card['id'], source_card)
            closed_ids.append(card['id'])
        
        return {'status': 'completed', 'closed_cards': closed_ids}

    def handle_support_comms(self, source_card_id: str) -> Dict:
        result = self.sync_assignees_to_dydx(
            source_card_id, 'support_ticket', 
            target_phase='testing / comms', enable_move=True, is_move_event=True
        )
        return {'status': 'comms_updated', **result}

    def handle_support_backlog_update(self, source_card_id: str) -> Dict:
        result = self.sync_assignees_to_dydx(
            source_card_id, 'support_ticket', 
            target_phase='backlog', enable_move=True, is_move_event=True
        )
        return {'status': 'backlog_updated', **result}

    def process_support_webhook(self, source_card_id: str, action: str, current_phase: str = None, previous_phase: str = None) -> Dict:
        """Route a support board webhook to the correct handler."""
        if action == 'card.create': 
            raw = self.handle_support_card_created(source_card_id)
            return self._format_action_result(action, raw, default_status='created')
        
        if action in ['card.field_update', 'card.update']:
            result = self._handle_field_update(source_card_id, 'support_ticket')
            return {'status': 'field_updated' if result.get('updated') else 'no_change', **result}
        
        if action == 'card.move':
            phase = current_phase or ''
            if self._phase_matches(phase, self.SUPPORT_COMPLETED_PHASES): 
                return self.handle_support_completed(source_card_id)
            if self._phase_matches(phase, self.SUPPORT_COMMS_PHASES):
                raw = self.handle_support_comms(source_card_id)
                return self._format_action_result(action, raw, default_status='comms_updated')
            if self._phase_matches(phase, self.SUPPORT_TESTING_PHASES): 
                raw = self.handle_support_testing(source_card_id)
                return self._format_action_result(action, raw, default_status='testing')
            if self._phase_matches(phase, self.SUPPORT_ESCALATED_PHASES): 
                raw = self.handle_support_escalated(source_card_id)
                return self._format_action_result(action, raw, default_status='escalated')
            if self._phase_matches(phase, self.SUPPORT_IN_PROGRESS_PHASES): 
                raw = self.handle_support_in_progress(source_card_id)
                return self._format_action_result(action, raw, default_status='in_progress')
            if self._phase_matches(phase, self.SUPPORT_BACKLOG_PHASES):
                raw = self.handle_support_backlog_update(source_card_id)
                return self._format_action_result(action, raw, default_status='backlog_updated')
            raw = self.sync_assignees_to_dydx(source_card_id, 'support_ticket', is_move_event=True)
            return self._format_action_result(action, raw, default_status='moved')
        
        return {'status': 'no_action'}

    # ============================================
    # CHANGE REQUEST HANDLERS
    # ============================================

    def process_change_request_webhook(self, source_card_id: str, action: str, current_phase: str = None, previous_phase: str = None) -> Dict:
        """Route a Change Request webhook to the correct handler."""
        if not current_phase and action in ['card.move', 'card.field_update', 'card.update']:
             try:
                source_data = self.mediamark_client.get_card(source_card_id)
                current_phase = source_data['data']['card'].get('current_phase', {}).get('name', '')
             except: pass

        if action == 'card.create': 
            raw = self.handle_cr_creation(source_card_id)
            return self._format_action_result(action, raw, default_status='created')

        if action == 'card.move':
            raw = self.handle_cr_phase_transition(source_card_id, current_phase, previous_phase)
            return self._format_action_result(action, raw, default_status='moved')

        if action in ['card.field_update', 'card.update']: 
            result = self._handle_field_update(source_card_id, 'change_request')
            return {'status': 'field_updated' if result.get('updated') else 'no_change', **result}
        
        return {'status': 'no_action'}

    def handle_cr_creation(self, source_card_id):
        """Handle Change Request card creation from Mediamark."""
        source_data = self.mediamark_client.get_card(source_card_id)
        source_card = source_data['data']['card']
        phase = source_card.get('current_phase', {}).get('name', '')
        
        if self._is_excluded_cr_phase(phase): 
            return {'status': 'excluded'}
        
        target_phase = self._get_correct_dydx_phase(phase)
        result = self.sync_assignees_to_dydx(source_card_id, 'change_request', target_phase=target_phase, source_card=source_card)
        return {'status': 'created', **result}

    def handle_cr_phase_transition(self, source_card_id: str, current_phase: str, previous_phase: str) -> Dict:
        """Handle Change Request phase transitions."""
        if not self._start_processing(source_card_id):
            return {'status': 'already_processing'}
        
        try:
            source_data = self.mediamark_client.get_card(source_card_id)
            source_card = source_data['data']['card']
            field_values = self.extract_field_values(source_card)
            
            actual_phase_obj = source_card.get('current_phase', {})
            actual_phase_name = actual_phase_obj.get('name', '')
            actual_phase_id = actual_phase_obj.get('id', '')
            
            if actual_phase_name and actual_phase_name.lower() != (current_phase or '').lower():
                current_phase = actual_phase_name
            
            if not self._has_phase_changed(source_card_id, actual_phase_name, actual_phase_id):
                return {'status': 'no_phase_change'}
            
            # CANCELLED
            if self._phase_matches(current_phase, self.CR_CANCELLED_PHASES):
                all_cards = self.find_all_active_dydx_cards_by_source_id(source_card_id)
                for dydx_card in all_cards:
                    target_phase_id = self.dydx_phases.get('cancelled')
                    try:
                        self.dydx_client.move_card_to_phase(dydx_card['id'], target_phase_id)
                    except Exception as e:
                        if "already in the destination phase" not in str(e):
                            logger.error(f"Failed to move DYDX card {dydx_card['id']} to cancelled (MM card {source_card_id}): {e}")
                return {'status': 'cancelled', 'cards_cancelled': len(all_cards)}

            # COMPLETED
            if self._phase_matches(current_phase, self.CR_COMPLETED_PHASES):
                all_cards = self.find_all_active_dydx_cards_by_source_id(source_card_id)
                for dydx_card in all_cards:
                    self.close_dydx_card(dydx_card['id'], source_card)
                return {'status': 'completed', 'cards_closed': len(all_cards)}

            # CLIENT APPROVAL (excluded)
            if self._is_excluded_cr_phase(current_phase):
                return {'status': 'excluded_phase'}

            target_dydx_phase = self._get_correct_dydx_phase(current_phase)
            
            all_cards = self.find_all_active_dydx_cards_by_source_id(source_card_id)
            cards_with_wrong_status = []
            
            for card in all_cards:
                if not self._dydx_card_matches_source_phase(card, current_phase):
                    cards_with_wrong_status.append(card)
            
            for card in cards_with_wrong_status:
                self.close_dydx_card(card['id'], source_card)
            
            result = self.sync_assignees_to_dydx(source_card_id, 'change_request', target_phase=target_dydx_phase, enable_move=True, is_move_event=True, source_card=source_card)
            result['closed_wrong_status'] = [c['id'] for c in cards_with_wrong_status]
            return {'status': 'synced', 'target_phase': target_dydx_phase, **result}
        
        finally:
            self._end_processing(source_card_id)

    # ============================================
    # CLEANUP METHODS
    # ============================================
    
    def find_orphaned_dydx_cards(self) -> List[Dict]:
        """Find DYDX cards whose Mediamark source cards are in Done/Completed."""
        orphaned = []
        
        query = """query GetPipeCards($pipeId: ID!, $after: String) { 
            cards(pipe_id: $pipeId, first: 50, after: $after) { 
                pageInfo { hasNextPage endCursor }
                edges { node { id title url current_phase { id name } fields { field { id } value } } } 
            } 
        }"""
        
        has_next = True
        cursor = None
        done_phase_id = self.dydx_phases.get('done')
        
        try:
            while has_next:
                variables = {'pipeId': self.dydx_pipe_id, 'after': cursor}
                result = self.dydx_client.execute_query(query, variables)
                
                data = result.get('data', {}).get('cards', {})
                edges = data.get('edges', [])
                
                for edge in edges:
                    card = edge['node']
                    if card.get('current_phase', {}).get('id') == done_phase_id:
                        continue
                    
                    source_card_id = None
                    for field in card.get('fields', []):
                        if field['field']['id'] == 'main_task_id':
                            source_card_id = str(field.get('value', '')).strip()
                            break
                    
                    if source_card_id:
                        try:
                            source_data = self.mediamark_client.get_card(source_card_id)
                            source_card = source_data.get('data', {}).get('card', {})
                            source_phase = source_card.get('current_phase', {}).get('name', '').lower()
                            
                            is_completed = any(x in source_phase for x in ['done', 'complete', 'closed', 'cancelled', 'canceled'])
                            
                            if is_completed:
                                orphaned.append({
                                    'dydx_card_id': card['id'],
                                    'source_card_id': source_card_id,
                                    'source_phase': source_phase
                                })
                        except Exception as e:
                            if 'not found' in str(e).lower() or '404' in str(e):
                                orphaned.append({
                                    'dydx_card_id': card['id'],
                                    'source_card_id': source_card_id,
                                    'source_phase': 'DELETED'
                                })
                
                page_info = data.get('pageInfo', {})
                has_next = page_info.get('hasNextPage', False)
                cursor = page_info.get('endCursor')
                
        except Exception as e:
            logger.error(f"Error finding orphaned cards: {e}")
        
        return orphaned
    
    def cleanup_orphaned_cards(self, dry_run: bool = True) -> Dict:
        """Find and optionally close orphaned DYDX cards."""
        orphaned = self.find_orphaned_dydx_cards()
        result = {'orphaned_count': len(orphaned), 'orphaned_cards': orphaned, 'dry_run': dry_run, 'closed_count': 0, 'errors': []}
        
        if not dry_run:
            for card in orphaned:
                try:
                    self.close_dydx_card(card['dydx_card_id'], None)
                    result['closed_count'] += 1
                except Exception as e:
                    result['errors'].append({'card_id': card['dydx_card_id'], 'error': str(e)})
        
        return result
