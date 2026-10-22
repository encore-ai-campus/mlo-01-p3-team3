"""Silver discovery scope and checkpoint identity."""
from ..infrastructure.checkpoint import InputCheckpoint


def bind_scan(cursor, scope):
    """A cursor from different owner inputs/flow cannot authorize skipping rows."""
    if cursor.get('scan_scope') == scope:return cursor
    return dict(bronze_version=cursor['bronze_version'],after=None,
        epoch=cursor.get('epoch',0)+1,completed=False,scan_scope=scope)



class Checkpoint(InputCheckpoint):
    stage, marker_name = 'SILVER', '_silver_target.json'
