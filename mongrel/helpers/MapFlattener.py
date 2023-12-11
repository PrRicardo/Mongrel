from functools import (partial,
                       singledispatch)
from itertools import chain
from typing import (Dict,
                    List,
                    TypeVar)

Serializable = TypeVar('Serializable', None, int, bool, float, str,
                       dict, list, tuple)
Array = List[Serializable]
Object = Dict[str, Serializable]

"""
Code written by Azat Ibrakov in https://stackoverflow.com/questions/51359783/how-to-flatten-multilevel-nested-json
"""


def flatten(object_: Object,
            *,
            path_separator: str = '.') -> Array[Object]:
    """
    This method flattens a document. This means that the structure is broken down of a dictionary containing a list
    to a list containing multiple dictionaries. This takes quite some time for big dictionaries with multiple lists.
    example
    {
        a:a_val,
        b:b_val,
        c:c_val,
        d:[
            {
                e:e_val1
                f:[
                    f_val1,
                    f_val2
                ]
            },
            {
                e:e_val2
                f:[
                    f_val3,
                    f_val4
                ]
            },
        ]
    }
    will be converted to:
    [
    { a:a_val, b:b_val, c:c_val, d.e:e_val1, d.e.f:f_val1},
    { a:a_val, b:b_val, c:c_val, d.e:e_val1, d.e.f:f_val2},
    { a:a_val, b:b_val, c:c_val, d.e:e_val2, d.e.f:f_val3},
    { a:a_val, b:b_val, c:c_val, d.e:e_val2, d.e.f:f_val4},
    ]

    CAREFUL when using on data structures like this
    {
        a:[
            a_val1,
            a_val2
        ],
        b:[
            b_val1,
            b_val2
        ]
    }
    Due to a and b being on the same layer a cartesian product needs to be applied!
    [
        {a: a_val1, b:b_val1},
        {a: a_val1, b:b_val2},
        {a: a_val2, b:b_val1},
        {a: a_val2, b:b_val2}
    ]
    To reduce this effect the transferrer pre-filters the json
    :param object_: a document containing lists and/or dictionaries
    :param path_separator: the path seperator to be used for aggregated path descriptions
    :return: the flattened dictionary as a list
    """
    keys = set(object_)
    result = [dict(object_)]
    while keys:
        key = keys.pop()
        new_result = []
        for index, record in enumerate(result):
            try:
                value = record[key]
            except KeyError:
                new_result.append(record)
            else:
                if isinstance(value, dict):
                    del record[key]
                    new_value = flatten_nested_objects(
                        value,
                        prefix=key + path_separator,
                        path_separator=path_separator
                    )
                    keys.update(new_value.keys())
                    new_result.append({**new_value, **record})
                elif isinstance(value, list):
                    del record[key]
                    new_records = [
                        flatten_nested_objects(sub_value,
                                               prefix=key + path_separator,
                                               path_separator=path_separator)
                        for sub_value in value
                    ]
                    keys.update(chain.from_iterable(map(dict.keys,
                                                        new_records)))
                    if new_records:
                        new_result.extend({**new_record, **record}
                                          for new_record in new_records)
                    else:
                        new_result.append(record)
                else:
                    new_result.append(record)
        result = new_result
    return result


@singledispatch
def flatten_nested_objects(object_: Serializable,
                           *,
                           prefix: str = '',
                           path_separator: str) -> Object:
    """
    Flattens the object, dicts and lists get iterated over and call this function recursively
    :param object_: the object to flatten
    :param prefix: the current prefix that's going to identify the object
    :param path_separator: the path seperator used for the prefix
    :return: the flattened object
    """
    return {prefix[:-len(path_separator)]: object_}


@flatten_nested_objects.register(dict)
def _(object_: Object,
      *,
      prefix: str = '',
      path_separator: str) -> Object:
    """
    Overload for dict data type
    """
    result = dict(object_)
    for key in list(result):
        result.update(flatten_nested_objects(result.pop(key),
                                             prefix=(prefix + key
                                                     + path_separator),
                                             path_separator=path_separator))
    return result


@flatten_nested_objects.register(list)
def _(object_: Array,
      *,
      prefix: str = '',
      path_separator: str) -> Object:
    """
    Overload for list data_type
    """
    return {prefix[:-len(path_separator)]: list(map(partial(
        flatten_nested_objects,
        path_separator=path_separator),
        object_))}
