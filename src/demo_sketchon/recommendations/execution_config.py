import hashlib
import json
import os
from pathlib import Path

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from interior_recommender.config import Settings
from interior_recommender.domain.recommendation import CONTEXT_VERSION, RankingPolicy


def scope():
    return hashlib.sha256(os.path.normcase(str(Path(settings.INTERIOR_RECOMMENDER_OUTPUT_ROOT).resolve())).encode()).hexdigest()


def limits():
    values = settings.RECOMMENDATION_EXECUTION
    if any(not isinstance(value, int) or isinstance(value, bool) or value <= 0 for value in values.values()):
        raise ImproperlyConfigured("추천 실행 제한은 양의 정수여야 합니다.")
    if values["heartbeat_seconds"] * 3 >= values["lease_seconds"]:
        raise ImproperlyConfigured("추천 lease는 heartbeat 간격의 3배보다 길어야 합니다.")
    if values["max_inflight"] > values["max_pending"]:
        raise ImproperlyConfigured("진행 상한은 접수 상한보다 클 수 없습니다.")
    return values


def config_key():
    value = {"products": str(settings.INTERIOR_RECOMMENDER_PRODUCTS),
             "preview_version": "product-preview-v1",
             "context_version": CONTEXT_VERSION, "ranking_defaults": RankingPolicy().model_dump(),
             "policy": settings.INTERIOR_RECOMMENDER_RANKING_POLICY,
             "demo": settings.INTERIOR_RECOMMENDER_DEMO, "models": Settings.from_env().model_dump(),
             "limits": limits(), "account": settings.RECOMMENDATION_API_ACCOUNT}
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()
