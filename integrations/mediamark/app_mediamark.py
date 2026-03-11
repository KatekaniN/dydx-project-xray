#!/usr/bin/env python3
"""
Mediamark Webhook Server
Real-time sync from Mediamark → DYDX Development Tasks Management board.

=== WHAT THIS FILE DOES ===
This is the Flask web server for the Mediamark integration. It:
  1. Receives webhooks from Mediamark's Pipefy "Workflow Support" board
  2. Routes them to the MediamarkSync engine (sync_to_dydx.py)
  3. Runs a background card listener that polls for missed changes
  4. Sends logs to SolarWinds for remote monitoring

=== HOW TO RUN ===
  Development:  python integrations/mediamark/app_mediamark.py
  Production:   gunicorn -c gunicorn_config.py integrations.mediamark.app_mediamark:app
"""

import os
import sys
import logging
import json
import re
import uuid
import requests
import threading
import queue
from collections import OrderedDict
from datetime import datetime

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_THIS_DIR, '..', '..'))
sys.path.insert(0, _PROJECT_ROOT)

from flask import Flask, request, jsonify
from typing import Dict, Tuple

from integrations.mediamark.sync_to_dydx import MediamarkSync
from integrations.mediamark.card_listener import start_listener, get_listener, stop_listener

from dotenv import load_dotenv
_ENV_FILE = os.path.join(_THIS_DIR, '.env.mediamark')
if os.path.exists(_ENV_FILE):
    load_dotenv(_ENV_FILE)
else:
    load_dotenv()

log_queue = queue.Queue()


class SolarWindsWorker(threading.Thread): # inherits from threading.Thread to run in the background
    """Background thread that sends log messages to SolarWinds."""
    def __init__(self, url, token): # constructor takes the SolarWinds URL and API token
        super().__init__(daemon=True) # run threading from the parent class, set as daemon so it doesn't block app shutdown
        self.url = url
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream" # SolarWinds expects raw bytes, not JSON. We encode log messages as UTF-8 bytes before sending.
        })

    def run(self): 
        while True:
            try:
                record = log_queue.get()
                self.session.post(self.url, data=record.encode('utf-8'), timeout=5) #post the log message to SolarWinds url, encode the string as bytes, set a timeout to prevent hanging
                log_queue.task_done()
            except Exception:
                pass


class QueueHandler(logging.Handler): # Custom logging handler that puts log messages into the queue for the SolarWindsWorker to send.
    def emit(self, record): # This method is called by the logging framework for each log message. We format the message and put it in the queue.
        try:
            msg = self.format(record)
            log_queue.put(msg)
        except Exception:
            self.handleError(record)


# Create logger
logger = logging.getLogger(__name__) # __name__ is the name of the current module, so logs will be labeled with "app_mediamark" which helps identify where they come from in SolarWinds.
logger.setLevel(logging.INFO) # Set the logging level to INFO where we ignore debug messages but capture info, warnings, and errors.

# Console logging
console_handler = logging.StreamHandler() # Logs will also be printed to the console for local visibility when running the app directly.
console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_format)
logger.addHandler(console_handler)

SW_LOG_URL = os.getenv('SOLARWINDS_LOG_URL') 
SW_API_TOKEN = os.getenv('SOLARWINDS_TOKEN')

if SW_LOG_URL and SW_API_TOKEN:
    try:
        worker = SolarWindsWorker(SW_LOG_URL, SW_API_TOKEN)
        worker.start()
        sw_handler = QueueHandler()
        sw_formatter = logging.Formatter(
            '%(asctime)s [%(name)s] %(levelname)s %(message)s',
            datefmt='%Y-%m-%dT%H:%M:%S'
        )
        sw_handler.setFormatter(sw_formatter)
        logger.addHandler(sw_handler)
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)  # Ensure INFO logs from all modules (sync_to_dydx, card_listener, etc.) propagate to SolarWinds.
        root_logger.addHandler(sw_handler)  # Also add to root logger to capture logs from other modules.
    except Exception as e:
        logger.error(f"Failed to setup SolarWinds logging: {e}")

# CREATE THE FLASK APP & SYNC SERVICE

app = Flask(__name__)

# Initialize the Mediamark sync service
try:
    sync_service = MediamarkSync()
    logger.info(" MediamarkSync initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize MediamarkSync: {e}")
    raise e

# CARD CHANGE LISTENER (Background Polling)

ENABLE_LISTENER = os.getenv('ENABLE_CARD_LISTENER', 'true').lower() == 'true'
card_listener = None

if ENABLE_LISTENER:
    try:
        card_listener = start_listener(sync_service, sync_service.clients.mediamark_client)
        logger.info(" Mediamark card change listener ENABLED")
    except Exception as e:
        logger.error(f"Failed to start card listener: {e}")
else:
    logger.info(" Mediamark card change listener DISABLED")

# Store the single Workflow Support board pipe ID for webhook routing
# All Mediamark cards come from this one board.
SUPPORT_BOARD_PIPE_ID = os.getenv('MEDIAMARK_SUPPORT_BOARD_PIPE_ID')

# Debug/test endpoints are disabled in production.
# Set ENABLE_DEBUG_ENDPOINTS=true in .env to re-enable (development only).
ENABLE_DEBUG_ENDPOINTS = os.getenv('ENABLE_DEBUG_ENDPOINTS', 'false').lower() == 'true'

# ==========================================
# JOB TRACKER
# ==========================================
# In-memory store of background job states. Capped at 500 entries (oldest dropped first).
_JOB_STORE_MAX = 500
_job_store: OrderedDict = OrderedDict()  # job_id -> {status, card_id, action, started_at, finished_at, result, error}
_job_store_lock = threading.Lock()


def _create_job(card_id: str, action: str) -> str:
    """Register a new job and return its ID."""
    job_id = str(uuid.uuid4())
    with _job_store_lock:
        if len(_job_store) >= _JOB_STORE_MAX:
            _job_store.popitem(last=False)  # evict oldest
        _job_store[job_id] = {
            'status': 'running',
            'card_id': card_id,
            'action': action,
            'started_at': datetime.utcnow().isoformat() + 'Z',
            'finished_at': None,
            'result': None,
            'error': None,
        }
    return job_id


def _finish_job(job_id: str, result=None, error: str = None):
    """Mark a job as completed or failed."""
    with _job_store_lock:
        if job_id in _job_store:
            _job_store[job_id]['status'] = 'failed' if error else 'completed'
            _job_store[job_id]['finished_at'] = datetime.utcnow().isoformat() + 'Z'
            _job_store[job_id]['result'] = result
            _job_store[job_id]['error'] = error

# ==========================================
# ROUTES
# ==========================================

@app.route('/')
def index():
    """Root endpoint — confirms the server is running."""
    return jsonify({
        'status': 'online',
        'service': 'Mediamark → DYDX Webhook Server',
        'version': '1.0.0'
    })


@app.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({'status': 'healthy'}), 200


# ==========================================
# WEBHOOK DATA PARSING HELPERS
# ==========================================

def extract_phase_info(data: dict) -> Tuple[str, str]:
    """Extract current and previous phase names from a Pipefy webhook payload."""
    current_phase = None
    previous_phase = None

    raw_data = data.get('data', {}) # The "data" field in the webhook payload contains the main event information, including the card and its current phase. We start by getting this "data" object, which is where Pipefy puts the details of the card that triggered the webhook.
    card_data = raw_data.get('card', {}) # Within the "data" object, there is a "card" field that contains information about the card that triggered the webhook. We access this "card" object to get details about the card, including its current phase.

    current_phase_obj = card_data.get('current_phase', {}) 
    if current_phase_obj:
        current_phase = current_phase_obj.get('name') # The "current_phase" field within the "card" object contains information about the current phase of the card. We access this field to get the name of the current phase.

    if 'from' in raw_data: # The "from" field in the webhook payload indicates the previous state of the card before the change that triggered the webhook. We check if this "from" field exists to extract the previous phase information.
        from_obj = raw_data.get('from', {}) 
        if isinstance(from_obj, dict): 
            previous_phase = from_obj.get('name') # The "from" object may contain a "name" field that indicates the name of the previous phase. We access this field to get the previous phase name.

    if 'to' in raw_data: # The "to" field in the webhook payload indicates the new state of the card after the change that triggered the webhook. We check if this "to" field exists to extract the current phase information.
        to_obj = raw_data.get('to', {})
        if isinstance(to_obj, dict):
            current_phase = to_obj.get('name') or current_phase # The "to" object may contain a "name" field that indicates the name of the current phase. We access this field to get the current phase name. If it's not available, we fall back to the current_phase extracted from the "current_phase" field.

    if not current_phase and 'phase' in data:
        phase_obj = data.get('phase', {})
        if isinstance(phase_obj, dict):
            current_phase = phase_obj.get('name')
        elif isinstance(phase_obj, str):
            current_phase = phase_obj

    return current_phase, previous_phase


def sanitize_json_keys(text):
    """Attempt to fix malformed JSON from Pipefy."""
    if not text:
        return "{}"
    text = text.replace("'", '"')
    try:
        text = re.sub(r'(?<!")(\b\w+\b)(?=\s*:)', r'"\1"', text)
    except:
        pass
    return text



# MAIN WEBHOOK ENDPOINT


@app.route('/webhook/pipefy', methods=['POST', 'GET'])
def handle_pipefy_webhook():
    """
    Main webhook handler — receives events from Mediamark's Pipefy boards
    and routes them to the correct sync handler.
    """
    try:
        if request.method == 'GET':
            return jsonify({'status': 'ok'}), 200

        data = None

        # Attempt 1: Standard JSON parsing
        try:
            data = request.get_json(silent=True) # silent=True prevents Flask from raising an error if the JSON is malformed. If the payload is valid JSON, it will be parsed and stored in the "data" variable. If it's not valid JSON, "data" will be None and we can attempt other parsing strategies.
        except:
            pass

        # Attempt 2: Manual repair
        if not data:
            raw_data = request.get_data(as_text=True) # Get the raw request data as text. This is useful if the JSON is malformed and cannot be parsed by Flask's standard JSON parser.
            if raw_data:
                try:
                    data = json.loads(raw_data)
                except json.JSONDecodeError:
                    try:
                        clean_data = sanitize_json_keys(raw_data)
                        data = json.loads(clean_data)
                        logger.warning(" Repaired malformed JSON")
                    except:
                        logger.warning(" JSON Parse Failed. Ignoring.")
                        return jsonify({'status': 'ignored', 'message': 'Invalid JSON'}), 200

        if not data:
            return jsonify({'status': 'ok', 'message': 'No data found'}), 200

        action = data.get('action')

        if not action and request.form and 'payload' in request.form: # Some Pipefy webhooks send data as form-encoded with a "payload" field containing the JSON string. We check for this case and attempt to parse it.
            try:
                data = json.loads(request.form['payload'])
                action = data.get('action')
            except:
                pass

        if not action:
            return jsonify({'status': 'ok', 'message': 'Test webhook received'}), 200

        # Extract card and pipe identifiers
        raw_data_obj = data.get('data', {})
        card_data = raw_data_obj.get('card', {})
        card_id = card_data.get('id')
        pipe_data = card_data.get('pipe', {})
        pipe_id = pipe_data.get('id')

        if not card_id:
            logger.error('No card ID in webhook payload')
            return jsonify({'status': 'error', 'message': 'Missing card ID'}), 200

        current_phase, previous_phase = extract_phase_info(data)

        logger.info(f" [Mediamark] Received: card_id={card_id}, pipe_id={pipe_id}, action={action}")
        logger.info(f" Phases: current='{current_phase}', previous='{previous_phase}'")

        # Since all Mediamark cards come from the single "Workflow Support" board. The sync engine internally checks the "Support request type" field to determine if a card is a CR or support ticket.
        if str(pipe_id) == str(SUPPORT_BOARD_PIPE_ID):
            # Return 200 immediately so Pipefy doesn't retry — process in background.
            job_id = _create_job(card_id, action)
            def _process(jid=job_id):
                try:
                    result = sync_service.process_support_webhook(
                        source_card_id=card_id,
                        action=action,
                        current_phase=current_phase,
                        previous_phase=previous_phase
                    )
                    _finish_job(jid, result=result)
                    logger.info(f"Job {jid} completed for card {card_id}")
                except Exception as bg_err:
                    _finish_job(jid, error=str(bg_err))
                    logger.error(f"Job {jid} failed for card {card_id}: {bg_err}", exc_info=True)
            threading.Thread(target=_process, daemon=True).start()
            return jsonify({'status': 'accepted', 'card_id': card_id, 'job_id': job_id}), 200

        else:
            logger.info(f"Unknown Mediamark pipe ID: {pipe_id}") # If the pipe ID from the webhook doesn't match the known Workflow Support board, we log this as an unknown pipe and ignore the webhook.
            return jsonify({'status': 'ignored'}), 200

    except Exception as e:
        logger.error(f"Webhook processing error: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'error': str(e)}), 200


# TEST ENDPOINT

@app.route('/webhook/test', methods=['POST'])
def test_webhook():
    """Manual test endpoint — disabled in production. Set ENABLE_DEBUG_ENDPOINTS=true to use.
    
    POST body:
    {
        "board_type": "change_request" or "support",
        "card_id": "123456",
        "action": "card.move",
        "current_phase": "In Progress"
    }
    """
    if not ENABLE_DEBUG_ENDPOINTS:
        return jsonify({'status': 'disabled', 'message': 'Debug endpoints are disabled in production.'}), 403
    try:
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({
                'status': 'error',
                'error': 'Invalid JSON body. Use raw JSON with double quotes.',
                'example': {
                    'board_type': 'support',
                    'card_id': '1309314590',
                    'action': 'card.create',
                    'current_phase': 'NEW'
                }
            }), 400

        board_type = str(data.get('board_type', '')).strip().lower()
        card_id = data.get('card_id')
        action = data.get('action', 'card.create')
        current_phase = data.get('current_phase')
        previous_phase = data.get('previous_phase')

        if not board_type or not card_id:
            return jsonify({'error': 'Missing board_type or card_id'}), 400

        if board_type in ('support', 'support_ticket'):
            fn = sync_service.process_support_webhook
        elif board_type == 'change_request':
            fn = sync_service.process_change_request_webhook
        else:
            return jsonify({'error': "Invalid board_type. Use 'support' or 'change_request'."}), 400

        # Return 200 immediately and process in background.
        job_id = _create_job(card_id, action)
        def _process(jid=job_id):
            try:
                result = fn(card_id, action, current_phase, previous_phase)
                _finish_job(jid, result=result)
                logger.info(f"Job {jid} completed for card {card_id}")
            except Exception as bg_err:
                _finish_job(jid, error=str(bg_err))
                logger.error(f"Job {jid} failed for card {card_id}: {bg_err}", exc_info=True)
        threading.Thread(target=_process, daemon=True).start()
        return jsonify({'status': 'accepted', 'card_id': card_id, 'action': action, 'job_id': job_id}), 200

    except Exception as e:
        logger.error(f"Test endpoint error: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'error': str(e)}), 400


# JOB STATUS ENDPOINT

@app.route('/status/<job_id>', methods=['GET'])
def job_status(job_id):
    """Poll the status of a background sync job.

    Returns:
      status:      'running' | 'completed' | 'failed'
      card_id:     the Mediamark card ID that triggered the job
      action:      the webhook action (card.create, card.move, etc.)
      started_at:  ISO timestamp
      finished_at: ISO timestamp (null while running)
      result:      sync result payload (null while running or on failure)
      error:       error message (null on success)
    """
    with _job_store_lock:
        job = _job_store.get(job_id)
    if not job:
        return jsonify({'status': 'not_found', 'job_id': job_id}), 404
    return jsonify({'job_id': job_id, **job}), 200


@app.route('/status', methods=['GET'])
def recent_jobs():
    """List the most recent 20 jobs (newest first) so you can see what's been processed."""
    with _job_store_lock:
        items = list(_job_store.items())
    recent = [{'job_id': jid, **info} for jid, info in reversed(items[-20:])]
    return jsonify({'jobs': recent, 'total_tracked': len(items)}), 200


# DEBUG ENDPOINTS

@app.route('/webhook/debug', methods=['POST'])
def debug_webhook():
    """Accepts any POST and logs it — disabled in production."""
    if not ENABLE_DEBUG_ENDPOINTS:
        return jsonify({'status': 'disabled', 'message': 'Debug endpoints are disabled in production.'}), 403
    return jsonify({'status': 'logged'}), 200


@app.route('/debug/card/<card_id>', methods=['GET'])
def debug_card(card_id):
    """Inspect fields extracted from a Mediamark source card — disabled in production."""
    if not ENABLE_DEBUG_ENDPOINTS:
        return jsonify({'status': 'disabled', 'message': 'Debug endpoints are disabled in production.'}), 403
    try:
        source_data = sync_service.clients.mediamark_client.get_card(card_id)
        source_card = source_data['data']['card']

        fields_raw = []
        for field in source_card.get('fields', []):
            fields_raw.append({
                'field_id': field.get('field', {}).get('id'),
                'field_label': field.get('field', {}).get('label'),
                'field_name': field.get('name'),
                'value': field.get('value')
            })

        assignees = [
            {'id': a.get('id'), 'name': a.get('name'), 'email': a.get('email')}
            for a in source_card.get('assignees', [])
        ]

        field_values = sync_service.extract_field_values(source_card)

        return jsonify({
            'card_id': card_id,
            'title': source_card.get('title'),
            'due_date': source_card.get('due_date'),
            'current_phase': source_card.get('current_phase', {}).get('name'),
            'assignees_from_card': assignees,
            'fields_raw': fields_raw,
            'field_values_dict': {k: str(v)[:100] for k, v in field_values.items()}
        }), 200
    except Exception as e:
        err = str(e)
        logger.error(f"Debug error: {err}", exc_info=True)

        if 'Permission denied' in err:
            return jsonify({
                'status': 'error',
                'error': f'Permission denied for card {card_id}.',
                'hint': 'The MEDIAMARK_API_KEY does not have access to this card/board.'
            }), 403

        if 'not found' in err.lower() or 'does not exist' in err.lower():
            return jsonify({
                'status': 'error',
                'error': f'Card {card_id} was not found or is not accessible.'
            }), 404

        return jsonify({'status': 'error', 'error': err}), 500


# LISTENER CONTROL ENDPOINTS

@app.route('/listener/status', methods=['GET'])
def listener_status():
    """Get the status of the Mediamark card change listener."""
    listener = get_listener()
    if listener:
        return jsonify(listener.get_status()), 200
    else:
        return jsonify({'error': 'Listener not initialized', 'enabled': ENABLE_LISTENER}), 200


@app.route('/listener/start', methods=['POST'])
def listener_start():
    """Start the Mediamark card change listener."""
    global card_listener
    try:
        if card_listener:
            card_listener.start()
        else:
            card_listener = start_listener(sync_service, sync_service.clients.mediamark_client)
        return jsonify({'status': 'started'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/listener/stop', methods=['POST'])
def listener_stop():
    """Stop the Mediamark card change listener."""
    try:
        stop_listener()
        return jsonify({'status': 'stopped'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/listener/check', methods=['POST'])
def listener_force_check():
    """Force an immediate check for Mediamark card changes."""
    listener = get_listener()
    if not listener:
        return jsonify({'error': 'Listener not initialized'}), 400
    card_id = request.json.get('card_id') if request.json else None
    result = listener.force_check(card_id)
    return jsonify(result), 200


@app.route('/listener/sync/<card_id>', methods=['POST'])
def listener_sync_card(card_id):
    """Force sync a specific Mediamark card immediately — disabled in production."""
    if not ENABLE_DEBUG_ENDPOINTS:
        return jsonify({'status': 'disabled', 'message': 'Debug endpoints are disabled in production.'}), 403
    try:
        board_type = request.args.get('board_type', 'change_request')
        result = sync_service._handle_field_update(card_id, board_type)
        return jsonify(result), 200
    except Exception as e:
        logger.error(f"Sync error: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500


# CLEANUP ENDPOINTS

@app.route('/cleanup/orphaned', methods=['GET'])
def find_orphaned():
    """Find orphaned DYDX cards (Mediamark source cards are Done but DYDX cards still active)."""
    try:
        orphaned = sync_service.find_orphaned_dydx_cards()
        return jsonify({'count': len(orphaned), 'orphaned_cards': orphaned}), 200
    except Exception as e:
        logger.error(f"Error finding orphaned cards: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/cleanup/orphaned', methods=['POST'])
def cleanup_orphaned():
    """
    Clean up orphaned DYDX cards.
    POST {"dry_run": true} to preview, {"dry_run": false} to actually close.
    """
    try:
        data = request.json or {}
        dry_run = data.get('dry_run', True)
        result = sync_service.cleanup_orphaned_cards(dry_run=dry_run)
        return jsonify(result), 200
    except Exception as e:
        logger.error(f"Cleanup error: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/cleanup/card/<dydx_card_id>', methods=['POST'])
def close_single_card(dydx_card_id):
    """Manually close a single DYDX card (move to Done phase)."""
    try:
        sync_service.close_dydx_card(dydx_card_id, None)
        return jsonify({'status': 'closed', 'card_id': dydx_card_id}), 200
    except Exception as e:
        logger.error(f"Error closing card: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500



if __name__ == '__main__':
    port = int(os.getenv('FLASK_PORT', 5001))
    debug = os.getenv('FLASK_DEBUG', '0').lower() in ('1', 'true')
    app.run(host='0.0.0.0', port=port, debug=debug)
