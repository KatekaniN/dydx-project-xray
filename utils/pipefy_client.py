#!/usr/bin/env python3
"""
Pipefy API Client — Wrapper for talking to Pipefy's GraphQL API.

=== KEY CLASSES ===
- PipefyClient:     Talks to ONE Pipefy organization (sends GraphQL requests).

"""

import os
import json
import time
import logging
import requests

from typing import Dict, List, Optional, Any

from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)

#Constants
PIPEFY_API_URL = "https://api.pipefy.com/graphql"
MAX_RETRIES = 3 # Number of times to retry a request if it fails
RETRY_DELAY = 2 # seconds to wait between retries


class PipefyClient:
    
    def __init__(self, api_key: str, org_name: str = ""):
       
        self.api_key = api_key
        self.org_name = org_name
        
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            # sending JSON data in the request body.
        }

    def _log_prefix(self) -> str:
        return f"[{self.org_name}]" if self.org_name else "[pipefy]"

    def _build_payload(self, query: str, variables: Optional[Dict] = None) -> Dict:
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        return payload

    def _post(self, payload: Dict) -> requests.Response:
        return requests.post(
            PIPEFY_API_URL,
            json=payload,
            headers=self.headers,
            timeout=30,
        )

    def _get_error_message(self, result: Dict) -> str:
        return result.get('errors', [{}])[0].get('message', 'Unknown GraphQL error')

    def _is_rate_limit_error(self, error_message: str) -> bool:
        error_text = error_message.lower()
        return 'rate limit' in error_text or 'throttl' in error_text

    def _raise_graphql_error(self, result: Dict) -> None:
        error_msg = self._get_error_message(result)
        logger.warning(f"{self._log_prefix()} GraphQL error: {error_msg}")
        logger.debug(f"{self._log_prefix()} Full error response: {json.dumps(result, indent=2)}")
        raise Exception(f"GraphQL error ({self.org_name}): {error_msg}")

    def _parse_response(self, response: requests.Response) -> Dict:
        response.raise_for_status()
        return response.json()

    def _handle_request_exception(self, attempt: int, error: requests.exceptions.RequestException) -> None:
        logger.warning(f"{self._log_prefix()} Request failed (attempt {attempt}/{MAX_RETRIES}): {error}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
            return
        raise Exception(f"API request failed after {MAX_RETRIES} attempts ({self.org_name}): {error}")

    def _handle_rate_limit(self, attempt: int) -> None:
        wait_time = RETRY_DELAY * attempt
        logger.warning(f"{self._log_prefix()} Rate limited. Waiting {wait_time}s... (attempt {attempt}/{MAX_RETRIES})")
        time.sleep(wait_time)
    
    def execute_query(self, query: str, variables: Dict = None) -> Dict:
        payload = self._build_payload(query, variables)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = self._post(payload)
                result = self._parse_response(response)
                if 'errors' in result:
                    error_message = self._get_error_message(result)
                    if self._is_rate_limit_error(error_message) and attempt < MAX_RETRIES:
                        self._handle_rate_limit(attempt)
                        continue
                    self._raise_graphql_error(result)
                return result

            except requests.exceptions.HTTPError:
                result = {}
                try:
                    result = response.json()
                except ValueError:
                    raise

                error_message = self._get_error_message(result)
                if self._is_rate_limit_error(error_message) and attempt < MAX_RETRIES:
                    self._handle_rate_limit(attempt)
                    continue
                self._raise_graphql_error(result)

            except requests.exceptions.RequestException as e:
                self._handle_request_exception(attempt, e)

        raise Exception(f"API request failed after {MAX_RETRIES} attempts ({self.org_name})")
    

    
    def get_card(self, card_id: str) -> Dict:
        """
        Fetch a single card's full details by its ID.
        """
        query = """
        query GetCard($cardId: ID!) {
            card(id: $cardId) {
                id
                title
                due_date
                url
                createdAt
                current_phase { id name }
                assignees { id name email }
                createdBy { id name email }
                labels { id name }
                fields {
                    name
                    value
                    array_value
                    field { id label type }
                }
            }
        }
        """
        return self.execute_query(query, {"cardId": card_id})
    
    def create_card(self, pipe_id: str, title: str, fields: List[Dict], 
                    due_date: str = None) -> Dict:
        """
        Create a new card in a Pipefy pipe (board).
        """
        mutation = """
        mutation CreateCard($pipeId: ID!, $title: String!, $fields: [FieldValueInput], $dueDate: DateTime) {
            createCard(input: {
                pipe_id: $pipeId
                title: $title
                fields_attributes: $fields
                due_date: $dueDate
            }) {
                card {
                    id
                    title
                    url
                    current_phase { id name }
                }
            }
        }
        """
        variables = {
            "pipeId": pipe_id,
            "title": title,
            "fields": fields,
        }
        if due_date:
            variables["dueDate"] = due_date
        
        return self.execute_query(mutation, variables)
    
    def update_card_field(self, card_id: str, field_id: str, value: Any) -> Dict:
        """
        Update a single field on an existing card.
        
        === PIPEFY FIELD TYPES ===
        - short_text / long_text: plain strings
        - select / radio: must match one of the predefined options
        - date / datetime: ISO date strings
        - number: numeric values (still sent as strings in GraphQL)
        - assignee_select: user IDs
        """
        # Convert lists/dicts to JSON strings — Pipefy expects string values for most fields.
        if isinstance(value, (list, dict)):
            value = json.dumps(value)
            # json.dumps() converts a Python object to a JSON string.
        
        mutation = """
        mutation UpdateCardField($cardId: ID!, $fieldId: ID!, $value: [UndefinedInput]) {
            updateCardField(input: {
                card_id: $cardId
                field_id: $fieldId
                new_value: $value
            }) {
                card { id title }
                success
            }
        }
        """
        return self.execute_query(mutation, {
            "cardId": card_id,
            "fieldId": field_id,
            "value": value if isinstance(value, list) else [str(value)]
            # Pipefy expects new_value as an array of strings.
            # Single values get wrapped in a list: "hello" → ["hello"]
        })
    
    def move_card_to_phase(self, card_id: str, phase_id: str) -> Dict:
        """
        Move a card to a different phase (column) on the board.
        """
        mutation = """
        mutation MoveCard($cardId: ID!, $phaseId: ID!) {
            moveCardToPhase(input: {
                card_id: $cardId
                destination_phase_id: $phaseId
            }) {
                card { id current_phase { id name } }
            }
        }
        """
        return self.execute_query(mutation, {
            "cardId": card_id,
            "phaseId": phase_id
        })
    
    def set_card_assignees(self, card_id: str, assignee_ids: List[str]) -> Dict:
        """
        Set the assignees on a card (replaces all current assignees).
        
        === NB ==

        This REPLACES all assignees — it doesn't add to the existing list.
        To add an assignee, you'd need to fetch the current list first, append, then set.
        """
        mutation = """
        mutation SetAssignees($cardId: ID!, $assigneeIds: [ID!]!) {
            updateCard(input: {
                id: $cardId
                assignee_ids: $assigneeIds
            }) {
                card { id assignees { id name } }
            }
        }
        """
        return self.execute_query(mutation, {
            "cardId": card_id,
            "assigneeIds": assignee_ids
        })
    
    def update_card_due_date(self, card_id: str, due_date: str) -> Dict:
        """
        Update a card's due date.
        """
        mutation = """
        mutation UpdateDueDate($cardId: ID!, $dueDate: DateTime) {
            updateCard(input: {
                id: $cardId
                due_date: $dueDate
            }) {
                card { id due_date }
            }
        }
        """
        return self.execute_query(mutation, {
            "cardId": card_id,
            "dueDate": due_date
        })


class DualOrgClients:
    """
    Holds API clients for BOTH Pipefy organizations (Up&Up + DYDX).
    
    This is the main class imported throughout the codebase:
        from utils.pipefy_client import DualOrgClients
        clients = DualOrgClients()
    
    It reads API keys from environment variables (set in your .env file),
    creates a PipefyClient for each org, and provides helper methods.
    
    === PROPERTIES ===
    - clients.upup_client → PipefyClient for Up&Up organization (source)
    - clients.dydx_client → PipefyClient for DYDX organization (destination)
    - clients.user_name_to_id → Dict mapping user names → Pipefy user IDs
    
    === HELPER METHODS ===
    - clients.get_user_id_by_name("Jess Shepherd") → "877678"
    """
    
    def __init__(self):
        """
        Initialize both Pipefy clients using API keys from environment variables.
        
        Reads from .env:
            UPUP_API_KEY  → API key for the Up&Up Pipefy organization
            DYDX_API_KEY  → API key for the DYDX Pipefy organization
        
        Raises:
            ValueError: If either API key is missing (you forgot to set .env).
        """
        upup_key = os.getenv('UPUP_API_KEY')
        dydx_key = os.getenv('DYDX_API_KEY')
        
        if not upup_key:
            raise ValueError(
                " UPUP_API_KEY not found in environment variables!\n"
                "   Create a .env file with: UPUP_API_KEY=your_key_here\n"
                "   Get your key from: Pipefy → Account Settings → Personal Access Tokens"
            )
        if not dydx_key:
            raise ValueError(
                "DYDX_API_KEY not found in environment variables!\n"
                "   Create a .env file with: DYDX_API_KEY=your_key_here\n"
                "   Get your key from: Pipefy → Account Settings → Personal Access Tokens"
            )
        
        self.upup_client = PipefyClient(upup_key, org_name="upup")
        self.dydx_client = PipefyClient(dydx_key, org_name="dydx")
        
        logger.info("Pipefy API clients initialized for Up&Up and DYDX")
        
        # ---- User Name → ID mapping ----
        # Many fields in Pipefy store assignees by NAME (e.g., "Jess Shepherd"),
        # but the API needs user IDs (e.g., "877678") to assign cards.
        # This dict provides the translation.
        #
        # TODO: Ideally, fetch this dynamically from the Pipefy API.
        # For now, it's hardcoded with known team members.
        self.user_name_to_id: Dict[str, str] = {
            # Add your team members here:
            # "Full Name": "pipefy_user_id",
            #
            # To find user IDs, go to Pipefy → Admin → Members, or use
            # a GraphQL query: query { me { id name } }
            # "jess shepherd": "877678",
            # "john doe": "123456",
        }
    
    def get_user_id_by_name(self, name: str) -> Optional[str]:
        """
        Look up a Pipefy user ID by their display name.
        
        Args:
            name: The user's display name (case-insensitive).
        
        Returns:
            The user's Pipefy ID as a string, or None if not found.
        
        === HOW IT WORKS ===
        1. Normalizes the name to lowercase for case-insensitive matching.
        2. First tries an EXACT match in user_name_to_id.
        3. If no exact match, tries PARTIAL matching (name contains or is contained).
        """
        if not name:
            return None
        
        name_lower = name.lower().strip()
        
        # Try exact match first
        if name_lower in self.user_name_to_id:
            return self.user_name_to_id[name_lower]
        
        # Try partial matching (handles "Jess" matching "jess shepherd")
        for stored_name, user_id in self.user_name_to_id.items():
            if name_lower in stored_name or stored_name in name_lower:
                return user_id
        
        logger.warning(f"User not found in name-to-ID mapping: '{name}'")
        return None
