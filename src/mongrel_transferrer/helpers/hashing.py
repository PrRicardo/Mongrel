from typing import Any
import mmh3


def hash_dict(element: dict) -> int:
    hashed_vals = mmh3.hash64(str(element.items()).encode())
    return hashed_vals[0] + hashed_vals[1]

def hash_list(to_hash:list) -> int:
    return hash(str(to_hash))

def hash_scalar(to_hash:Any)-> int:
    hashed_vals = mmh3.hash64(str(to_hash).encode())
    return hashed_vals[0] + hashed_vals[1]