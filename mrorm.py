#!/usr/bin/env python3

import sqlite3
import sys
from db_base import DB_Element, DB_Element_With_Foreign_Key

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


if __name__ == '__main__':
    con = sqlite3.connect(sys.argv[1])
    tables = get_tables(con)
    for table in tables:
        print(table)
        print(get_cols_for_table(con, table))
    con.close()
