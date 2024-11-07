import os
import hashlib
import time
import pickle
import logging


logger = logging.getLogger(__name__)


def _hash_key(key: str):
    if not isinstance(key, str):
        key = str(key)
    return hashlib.md5(key.encode()).hexdigest()


class _CacheObject:
    def __init__(self, key, value, lifetime):
        self.key = key
        self.value = value
        self.lifetime = lifetime


class CacheInterface:
    def has(self, key) -> bool:
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError

    def cache(self, key, value, ttl):
        raise NotImplementedError

    def remove(self, key):
        raise NotImplementedError


class PickleFileCache(CacheInterface):
    def __init__(self, fname):
        self.fname = fname
        self.cache = {}
        self._handle = None
        self.unwritten_count = 0
        self._load_cache()

    def has(self, key):
        return _hash_key(key) in self.cache

    def get(self, key) -> _CacheObject:
        obj: _CacheObject = self.cache.get(_hash_key(key), None)
        if obj is None:
            return None
        now = time.time()
        if obj.lifetime - now > 0:
            return obj.value
        else:
            del self.cache[key]
            return None

    def set_item(self, key, value, ttl):
        logger.debug(f"Caching value with key {key}: <{value}>")
        key = _hash_key(key)
        now = time.time()
        lifetime = now + ttl
        if key in self.cache:
            obj = self.cache.get(key)
            obj.value = value
            obj.lifetime = lifetime
        else:
            obj = _CacheObject(key=key, value=value, lifetime=lifetime)
            self.cache[key] = obj
        self.unwritten_count += 1
        if self.unwritten_count > 2:
            self._update_fc()

    def close(self):
        self._update_fc()

    def _update_fc(self):
        with open(self.fname, 'wb') as f:
            pickle.dump(self.cache, f)
        self.unwritten_count = 0

    def _load_cache(self):
        if not os.path.isfile(self.fname):
            return
        else:
            with open(self.fname, 'rb') as f:
                self.cache = pickle.load(f)

