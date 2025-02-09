import mmh3
from typing import Any

def _int32_to_64(left_id, right_id):
    left_id = left_id & 0xFFFFFFFF
    right_id = right_id & 0xFFFFFFFF
    result = (left_id << 32) | right_id
    if result >= 0x8000000000000000:
        result -= 0x10000000000000000
    return result

def hash_dict(element: dict) -> int:
    left_id, right_id = mmh3.hash64(str(element.items()).encode())
    return _int32_to_64(left_id, right_id)


def hash_list(to_hash: list) -> int:
    return hash(str(to_hash))


def hash_scalar(to_hash: Any) -> int:
    left_id, right_id = mmh3.hash64(str(to_hash).encode())
    return _int32_to_64(left_id, right_id)
