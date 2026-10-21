"""Protected server defaults survive a restart from any operator's computer."""
from pathlib import Path
from .files import read_json
from .windows import record_limit
from .spark_runtime import cluster_options
from .errors import PipelineError


def load(root,base):
    root,base=Path(root),Path(base)
    path=root/'runtime-overrides.json';value={}
    if path.exists():
        if path.stat().st_size>8192:raise PipelineError('PIPELINE_RUNTIME_OVERRIDE_INVALID')
        value=read_json(path)
        if not isinstance(value,dict) or set(value)-{'window_records','spark_runtime_file'}:
            raise PipelineError('PIPELINE_RUNTIME_OVERRIDE_INVALID')
    records=record_limit(value.get('window_records',100))
    profile=Path(value.get('spark_runtime_file',str(base/'config/spark-runtime.json')))
    if not profile.is_absolute() or not profile.resolve().is_relative_to(base.resolve()) or not profile.is_file():
        raise PipelineError('PIPELINE_RUNTIME_PROFILE_OUTSIDE_BASE')
    runtime=read_json(profile);cluster_options(runtime)
    return records,str(profile.resolve()),runtime
