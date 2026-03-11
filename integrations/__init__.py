# __init__.py — Makes this folder a Python "package".
#
# === WHAT IS __init__.py? ===
# In Python, a regular folder is just a folder. But if you put an __init__.py
# file inside it, Python treats that folder as a "package" — a collection of
# modules that can be imported with dot notation.
#
# Without this file: "from integrations.mediamark import ..." would FAIL.
# With this file:    "from integrations.mediamark import ..." works!
#
# This file can be empty (like this one) — its mere EXISTENCE is what matters.
# Think of it like a door sign that says "This room has importable code."
