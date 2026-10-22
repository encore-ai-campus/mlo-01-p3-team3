"""Gold owner inputs, fixed Silver versions, and output hashes."""
from uuid import UUID

from ..contracts import digest, utc
from ..errors import PipelineError
from ..silver.normalize import TABLES as SILVER_TABLES

VERSION = 'product-gold-1.0.1'
INPUT_TABLES = (*SILVER_TABLES, 'pipeline_evaluations')
TABLES = ('product_catalog', 'current_offers', 'offer_history', 'product_images',
          'product_links', 'quality_issues', 'quality_daily')
KEYS = {t: 'row_key' for t in (*TABLES, 'pipeline_evaluations')}


def semantic_hash(row):
    from .identity import VERSION as PRODUCT_VERSION
    excluded = {'record_hash', 'processing_run_id', 'processed_at'}
    if row.get('gold_policy_version') == PRODUCT_VERSION and 'output_manifest_json' not in row:
        excluded.add('silver_versions_json')
    return digest({k: v for k, v in row.items()
                   if k not in excluded})


def validate_inputs(value):
    if (not isinstance(value, dict) or value.get('contract_version') != 1
            or value.get('content_hash') != digest({k:v for k,v in value.items() if k != 'content_hash'})):
        raise PipelineError('GOLD_INPUT_HASH_INVALID')
    required = {'policy', 'product_states', 'embeddings', 'evaluation_selection', 'gallery_selection', 'price_confirmations'}
    if not required.issubset(value):
        raise PipelineError('GOLD_INPUTS_MUST_BE_EXPLICIT')
    policy = value['policy']
    if policy.get('evaluation_identity') not in {None, 'PRODUCT_INPUTS_V2'}:
        raise PipelineError('GOLD_IDENTITY_POLICY_UNSUPPORTED')
    if (not isinstance(policy, dict) or not policy.get('version') or not policy.get('evidence_ref')
            or policy.get('price_history_mode') not in {'UNRESOLVED', 'SNAPSHOT_V1', 'PRICE_EFFECT_V2'}
            or type(policy.get('price_max_age_seconds')) is not int or policy['price_max_age_seconds'] <= 0
            or not isinstance(policy.get('representative_price_kinds'), list)
            or len(set(policy['representative_price_kinds'])) != len(policy['representative_price_kinds'])
            or any(k not in {'SELLER_REPORTED', 'OFFER_REPORTED'} for k in policy['representative_price_kinds'])
            or type(policy.get('bootstrap_single_gallery')) is not bool):
        raise PipelineError('GOLD_POLICY_REQUIRED')
    for kind in required - {'policy'}:
        snapshot = value[kind]
        if (not isinstance(snapshot, dict) or snapshot.get('status') not in {'NOT_PROVIDED', 'PROVIDED'}
                or not isinstance(snapshot.get('rows'), list)
                or (snapshot['status'] == 'NOT_PROVIDED' and snapshot['rows'])
                or (snapshot['status'] == 'PROVIDED' and not snapshot.get('evidence_ref'))):
            raise PipelineError('GOLD_OWNER_INPUT_INVALID')
        for row in snapshot['rows']:
            if not isinstance(row, dict) or not row.get('product_key') or not row.get('evidence_ref'):
                raise PipelineError('GOLD_OWNER_ROW_INVALID')
            for name in ('checked_at', 'expires_at'):
                if row.get(name): utc(row[name])


def registration(value, rehearse=False):
    try:
        run = str(UUID(value['processing_run_id']))
        if (run != value['processing_run_id'] or value['stage'] != 'GOLD_BUILD'
                or value['status'] != ('FIXTURE' if rehearse else 'REGISTERED')
                or not value.get('evidence_ref')
                or (not rehearse and value.get('provider') != 'MYSQL_PIPELINE_RUNS_EXPORT')):
            raise ValueError()
        return run
    except (KeyError, ValueError, TypeError) as exc:
        raise PipelineError('GOLD_RUN_REGISTRATION_REQUIRED') from exc


def validate_versions(versions):
    if (set(versions) != set(INPUT_TABLES)
            or any(type(v) is not int or v < 0 for v in versions.values())):
        raise PipelineError('GOLD_ALL_FIXED_SILVER_VERSIONS_REQUIRED')


def active(row, as_of):
    return bool(row.get('checked_at') and row.get('expires_at')
                and utc(row['checked_at']) <= utc(as_of) < utc(row['expires_at']))
