"""Allowlisted local photo stickers, shared by validation and the editor."""
import json
from pathlib import Path

STICKERS = json.loads((Path(__file__).parent / 'static/sketch/stickers.json').read_text(encoding='utf-8'))
STICKER_IDS = frozenset(row['id'] for row in STICKERS)
