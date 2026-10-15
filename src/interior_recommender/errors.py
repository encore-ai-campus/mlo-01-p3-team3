"""Keep useful provider diagnostics without logging request URLs or credentials."""
import os
import re

from google.genai.errors import APIError


def provider_message(error):
    if not isinstance(error, APIError):
        return None
    message = str(error.message or "")
    for name in ("GEMINI_API_KEY", "GOOGLE_API_KEY", "CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID"):
        secret = os.getenv(name)
        if secret:
            message = message.replace(secret, "[REDACTED]")
    message = re.sub(r"https?://\S+", "[URL REDACTED]", message)
    message = re.sub(r"AIza[\w-]+", "[REDACTED]", message)
    return message[:1500] or None
