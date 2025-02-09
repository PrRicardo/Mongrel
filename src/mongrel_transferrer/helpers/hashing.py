from typing import Any
import mmh3


def hash_dict(element: dict) -> int:
    left_id, right_id = mmh3.hash64(str(element.items()).encode())
    left_id = left_id & 0xFFFFFFFF
    right_id = right_id & 0xFFFFFFFF
    return (left_id << 32) | right_id


def hash_list(to_hash: list) -> int:
    return hash(str(to_hash))


def hash_scalar(to_hash: Any) -> int:
    hashed_vals = mmh3.hash64(str(to_hash).encode())
    return hashed_vals[0] + hashed_vals[1]
