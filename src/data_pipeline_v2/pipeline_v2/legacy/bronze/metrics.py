"""Per-invocation timing, including callback/heartbeat threads; no persisted cache."""
from contextlib import contextmanager
from contextvars import ContextVar
from threading import Lock
from time import monotonic

_current=ContextVar('bronze_profile',default=None)


class Profile:
    def __init__(self):self.values={};self.lock=Lock()
    def __enter__(self):self.token=_current.set(self);return self
    def __exit__(self,*args):_current.reset(self.token)
    def add(self,name,value=1):
        with self.lock:self.values[name]=self.values.get(name,0)+value
    def add_max(self,name,value):
        with self.lock:
            previous=self.values.get(name)
            self.values[name]=value if previous is None else max(previous,value)
    def snapshot(self):
        with self.lock:return dict(self.values)


def current():return _current.get()


@contextmanager
def bind(profile):
    token=_current.set(profile)
    try:yield
    finally:_current.reset(token)


def bump(name,value=1):
    profile=current()
    if profile is not None:profile.add(name,value)


def maximum(name,value):
    profile=current()
    if profile is not None:profile.add_max(name,value)


@contextmanager
def timed(name,*,track_max=False):
    started=monotonic()
    try:yield
    finally:
        elapsed=round((monotonic()-started)*1000)
        bump(name+'_calls');bump(name+'_ms',elapsed)
        if track_max:maximum(name+'_max_ms',elapsed)
