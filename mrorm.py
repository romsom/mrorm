#!/usr/bin/env python3

from typing import Dict, List, Type
import sqlite3
import sys
from db_base import DB_Element, DB_Element_With_Foreign_Key

db_classes: Dict[str, Type] = {}

def get_tables(con):
    cursor = con.cursor()
    tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    res = [t[0] for t in tables]
    cursor.close()
    return res

def get_cols_for_table(con, table: str):
    cursor = con.cursor()
    cursor.execute("SELECT * FROM mydata",)
    res = [d[0] for d in cursor.description]
    cursor.close()
    return res

def get_db_sub_class(name: str, cols: List[str], pk: List[str]) -> Type:
    # TODO: constraints (primary, foreign)
    class _DB_Element(DB_Element):
        primary_key = pk
        _table_name = name
        _cols = cols
        def __init__(self, **kwargs):
            # set all supplied attributes
            for col in self._cols:
                if col in kwargs.keys():
                    self.__dict__[col] = kwargs[col]
                else:
                    self.__dict__[col] = None
                remaining_keys = list(set(kwargs.keys()) - set(self._cols))
                if len(remaining_keys) > 0:
                    print(f'{self._table_name}: superfluous parameters in constructor: {remaining_keys}', file=sys.stderr)

    return _DB_Element


if __name__ == '__main__':
    con = sqlite3.connect(sys.argv[1])
    tables = get_tables(con)
    for table in tables:
        print(table)
        cols = get_cols_for_table(con, table)
        print(cols)
        # TODO: primary key
        db_classes[table] = get_db_sub_class(table, cols, [])

    new_data = db_classes['mydata'](name="hello", id=42)
    new_data.insert(con)
    null_data = db_classes['mydata']()
    print(null_data.lookup(con))
    con.close()
