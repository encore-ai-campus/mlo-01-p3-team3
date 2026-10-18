"""Use the existing local recommendation engine inside the SketchOn process."""
import atexit
import threading

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from interior_recommender.application.direct import DirectStudio
from interior_recommender.assets import read_json

_studio = None
_guard = threading.Lock()


def studio():
    global _studio
    with _guard:
        if _studio is None:
            try:
                policy = settings.INTERIOR_RECOMMENDER_RANKING_POLICY
                _studio = DirectStudio(
                    settings.INTERIOR_RECOMMENDER_OUTPUT_ROOT,
                    products=settings.INTERIOR_RECOMMENDER_PRODUCTS,
                    demo=settings.INTERIOR_RECOMMENDER_DEMO,
                    policy=read_json(policy) if policy else None,
                )
            except (OSError, ValueError, KeyError) as error:
                raise ImproperlyConfigured("추천 카탈로그·모델·차원 설정을 확인해 주세요.") from error
        return _studio


def shutdown():
    if _studio is not None:
        _studio.pool.shutdown(wait=False, cancel_futures=True)


atexit.register(shutdown)
