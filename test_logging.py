"""
SolarWinds Logging Connection Test.

=== WHAT THIS SCRIPT DOES ===
Tests whether the app can successfully send log messages to SolarWinds
(a cloud monitoring/logging service). It:
1. Reads the SolarWinds URL and API token from .env
2. Sends a test log message via HTTP POST
3. Reports whether it succeeded or failed

Run this ONCE to verify your SolarWinds logging setup is working.

=== HOW TO RUN ===
  python test_logging.py

=== KEY CONCEPT: HTTP Authentication ===
The SolarWinds API uses "Bearer token" authentication:
- You include your API token in the HTTP header: "Authorization: Bearer <token>"
- The server checks this token to verify you're allowed to send logs.
"""

import os
import requests      # Library for making HTTP requests (GET, POST, etc.)
from dotenv import load_dotenv
import logging

# Load the .env file to get credentials
load_dotenv('integrations/mediamark/.env.mediamark')

print("--- SOLARWINDS CONNECTION TEST ---")

# 1. Get Credentials from environment variables
url = os.getenv('SOLARWINDS_LOG_URL')
token = os.getenv('SOLARWINDS_TOKEN')

print(f"URL:   {url}")
print(f"Token: {token[:10]}..." if token else "Token: NOT FOUND")
# token[:10] shows only first 10 chars — NEVER print full API keys/tokens!
# The "if token else" is a ternary expression:
#   value_if_true IF condition ELSE value_if_false

if not url or not token:
    print(" ERROR: Missing URL or Token in .env file.")
    exit(1)
    # exit(1) stops the script immediately. Exit code 1 = error (0 = success).

# 2. Prepare the HTTP request
headers = {
    "Authorization": f"Bearer {token}",
    # Bearer authentication — the standard way to send API tokens.
    "Content-Type": "application/octet-stream"
    # Tells the server we're sending raw bytes (not JSON).
}
log_message = "TEST LOG ENTRY: If you see this, SolarWinds logging is working!"

# 3. Send the test log
print("\nSending test log...")
try:
    response = requests.post(url, data=log_message.encode('utf-8'), headers=headers, timeout=10)
    # requests.post() sends an HTTP POST request.
    # url = where to send it
    # data = the body content (must be bytes, hence .encode('utf-8'))
    # headers = HTTP headers (auth, content type)
    # timeout = give up after 10 seconds if no response
    
    print(f"Response Code: {response.status_code}")
    # HTTP status codes: 200-299 = success, 400+ = error
    print(f"Response Body: {response.text}")
    # .text is the response body as a string
    
    if 200 <= response.status_code < 300:
        print("\n SUCCESS! The log was accepted. Check your dashboard now.")
    else:
        print("\n FAILED! SolarWinds rejected the request.")
except Exception as e:
    print(f"\n CRITICAL ERROR: Could not connect to SolarWinds.\n{e}")