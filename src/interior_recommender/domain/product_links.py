"""Validate saved Danawa comparison URLs against the exact source product."""
from urllib.parse import parse_qs, urlsplit


def comparison_url(value, product_id):
    if not isinstance(value, str) or not value or any(c.isspace() or ord(c) < 32 for c in value):
        return None
    try:
        url = urlsplit(value)
        pid = product_id.removeprefix("danawa:")
        if (not product_id.startswith("danawa:") or not pid.isascii() or not pid.isdigit()
                or url.scheme != "https" or url.netloc != "prod.danawa.com"
                or url.path != "/info/" or url.fragment
                or parse_qs(url.query).get("pcode") != [pid]):
            return None
        return value
    except ValueError:
        return None
