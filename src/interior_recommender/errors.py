"""Keep useful provider diagnostics without logging request URLs or credentials."""
import os
import re

from google.genai.errors import APIError


class LeaseLost(Exception):
    """The caller must stop without persisting a stale result."""


class ProviderCallRequired(Exception):
    """Yield a synchronous stage to the asynchronous provider runner."""
    def __init__(self, key, kind, prompt, images, schema):
        self.key, self.kind, self.prompt, self.images, self.schema = key, kind, prompt, images, schema
        super().__init__(kind)


class RetryScheduled(Exception):
    def __init__(self, timestamp):
        self.timestamp = timestamp
        super().__init__("Retry scheduled")


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
