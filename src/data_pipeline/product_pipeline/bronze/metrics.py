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


@contextmanager
def timed(name):
    started=monotonic()
    try:yield
    finally:
        bump(name+'_calls');bump(name+'_ms',round((monotonic()-started)*1000))
