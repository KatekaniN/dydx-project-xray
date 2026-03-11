import os

# Bind to all interfaces on the configured port
port = os.getenv('FLASK_PORT', '5001')
bind = f"0.0.0.0:{port}"

# Number of worker processes
# Formula: (2 * CPU cores) + 1
workers = 3

# Worker class — sync is fine for our webhook volume
worker_class = "sync"

# Timeout (seconds) — long enough for Pipefy API calls
timeout = 120

# Keep-alive for persistent connections
keepalive = 5

# Logging
accesslog = "-"   # stdout
errorlog  = "-"   # stderr
loglevel  = "info"

# Restart workers after this many requests to prevent memory leaks
max_requests = 1000
max_requests_jitter = 100
