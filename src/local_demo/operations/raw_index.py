"""Index immutable files once; console requests only query metadata."""
import json
from pathlib import Path
from uuid import UUID
from django.conf import settings
from sketch.models import Project
from sketch.raw_samples import MAX_RAW_BYTES, strict_constant, validate_sample
from .tracking import record_raw_receipt
from .models import RawReceipt


def index_raw(limit=1000):
    summary={'indexed':0,'skipped':0,'invalid':0,'limit':limit}
    root=Path(settings.SKETCH_RAW_ROOT)
    for i,path in enumerate(sorted(root.glob('*.json'))):
        if i>=limit:break
        try:
            if path.stat().st_size>MAX_RAW_BYTES:summary['skipped']+=1;continue
            body=path.read_bytes();sample=json.loads(body,parse_constant=strict_constant)
            if not isinstance(sample,dict) or str(UUID(sample['sample_id']))!=path.stem:raise ValueError('sample_id')
            project=Project.objects.get(pk=UUID(sample['project_id']))
            try:
                warnings=validate_sample(sample,project);status='REVIEW' if warnings else 'PASS'
            except (ValueError,TypeError,KeyError,AttributeError) as error:
                status='INVALID';warnings=[str(error)];summary['invalid']+=1
            import hashlib
            if not RawReceipt.objects.filter(project=project,sample_id=sample['sample_id'],checksum=hashlib.sha256(body).hexdigest(),validation_status=status).exists():
                record_raw_receipt(project,sample,body,status,warnings,True);summary['indexed']+=1
        except (OSError,ValueError,TypeError,KeyError,Project.DoesNotExist):summary['skipped']+=1
    return summary
