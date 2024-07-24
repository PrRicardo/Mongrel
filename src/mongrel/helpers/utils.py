def decide_sql_definition(type1, type2):
    type1 = type1.lower()
    type2 = type2.lower()
    if type1 == type2:
        return type1

    if type1.startswith('character varying') and type2.startswith('character varying'):
        len1 = int(type1.split('(')[1].strip(')'))
        len2 = int(type2.split('(')[1].strip(')'))
        return f'character varying({max(len1, len2)})'

    if 'character varying' in (type1, type2) or 'text' in (type1, type2):
        return 'text'

    numeric_types = ['smallint', 'integer', 'bigint', 'float', 'numeric']
    if type1 in numeric_types and type2 in numeric_types:
        return 'float' if 'float' in (type1, type2) else max(type1, type2, key=numeric_types.index)

    if type1.startswith('numeric') and type2.startswith('numeric'):
        p1, s1 = map(int, type1.split('(')[1].strip(')').split(','))
        p2, s2 = map(int, type2.split('(')[1].strip(')').split(','))
        return f'numeric({max(p1, p2)},{max(s1, s2)})'

    # Date and time types
    if 'date' in (type1, type2) and 'timestamp' in (type1, type2):
        return 'timestamp'

    # Boolean type
    if type1 == 'boolean' and type2 == 'boolean':
        return 'boolean'

    raise ValueError(f'Incompatible types: {type1}, {type2}')
