"""File implementation of ingestion recovery evidence and registration archives."""
from pathlib import Path
from ..files import atomic_json, read_json


class FileAudit:
    def __init__(self, root):
        self.root = Path(root)

    def write(self, relative, value):
        atomic_json(self.root / relative, value)

    def registration(self, key):
        path = self.root / 'registrations' / (key + '.json')
        return read_json(path) if path.exists() else None
