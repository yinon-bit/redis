"""Versioned dataset identity for reusable benchmark seeds."""
import hashlib
import json

def identity(case):
    fields = ('small_keys', 'big_keys', 'hash_keys', 'small_bytes', 'big_bytes',
              'hash_fields', 'hash_field_bytes', 'payload_generator',
              'insertion_order', 'insertion_seed')
    return dict(schema='redis-benchmark-seed/1', **{k: case.get(k, 0) for k in fields})

def key(case):
    return hashlib.sha256(json.dumps(identity(case), sort_keys=True).encode()).hexdigest()
