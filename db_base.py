from __future__ import annotations  # postpone annotation evaluation in python < 3.10
from typing import Dict, List, ClassVar, Type, Tuple
import logging
# setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class DB_Element:
    primary_key: List[str]
    _table_name: str

    def __eq__(self, other):
        '''Check for equality based on primary key.'''
        # TODO maybe change to get_attributes
        for k in self.primary_key:
            if self.__dict__[k] != other.__dict__[k]:
                return False
        return True

    def __hash__(self):
        '''Calculate hash based on primary key.'''
        # TODO maybe change to get_attributes
        return sum(hash(self.__dict__[k]) for k in self.primary_key)

    def __str__(self):
        pk = self.primary_key
        opt_keys = set(self.get_attributes()) - set(pk)
        components = [f'{k}: {self.__dict__[k]}' for k in pk]
        opt_components = [f'{k}: {self.__dict__[k]}' for k in opt_keys if self.__dict__[k] is not None]
        component_str = ', '.join(components + opt_components)
        return f'{type(self).__name__}({component_str})'

    @classmethod
    def FromDict(cls, etdict):
        '''Return an object from the information in etdict or None, if the information is incomplete'''
        # TODO consolidate etid vs id: only etid
        pk = cls.primary_key
        for k in pk:
            if k not in etdict:
                logger.error('converting incompatible dict')
                return None
        return cls(**etdict)

    @classmethod
    def FromDBDict(cls, etdict):
        return cls.FromDict({cls.convert_key_from_schema_to_class(k): v for k, v in etdict.items()})

    @classmethod
    def DictFromDBEntry(cls, cols, row):
        '''Create a dict from a DB entry, possibly converting keys from schema to class format.'''
        class_cols = tuple(cls.convert_key_from_schema_to_class(k) for k in cols)
        return {class_cols[i].lower(): row[i] for i in range(len(class_cols))}

    @classmethod
    def FromDBEntry(cls, cols, row):
        '''Create object from a DB entry.'''
        d = cls.DictFromDBEntry(cols, row)
        return cls.FromDBDict(d)

    @classmethod
    def Dummy(cls):
        '''Construct a dummy object from None-values'''
        return cls(*[None for _ in cls.primary_key])

    def to_dict(self):
        base_dict = self.__dict__
        return {k: base_dict[k] for k in self.get_attributes()}

    @classmethod
    def get_attributes(cls):
        '''Get a list of attributes of the DB object. All members which should *not* appear here must be private (start with "_").'''
        dummy = cls.Dummy()
        return [k for k in dummy.__dict__.keys() if not k.startswith('_')]

    def commit(self, etdb):
        etdb.commit()

    def update_clause(self):
        '''Create update clause using server side information and type information from this object, which may contain data from remote data. The type information is strictly only used to decide whether to generate a "key=?" or "key is NULL".'''
        class_keys = self.get_attributes()
        keys = [self.convert_key_from_class_to_schema(k) for k in class_keys]
        # update clause contains "=?" in every case because we want to be able to set a field to NULL
        update_elements = [(True, f'{keys[i]}=?') for i in range(len(class_keys))]
        # select clause contains either "=?" or "is NULL"
        # in the latter case there is no data element to be supplied to the call to execute, so we need to discern them using the first element in the tuple
        where_elements = [(True, f'{keys[i]}=?')
                          if self.__dict__[class_keys[i]] is not None
                          else (False, f'{keys[i]} is NULL')
                          for i in range(len(class_keys))]

        clause = f'UPDATE {self._table_name} set ' + ', '.join([elm[1] for elm in update_elements]) + ' where ' + ' and '.join([elm[1] for elm in where_elements]) + ';'
        # lists of class keys to be used for the call to execute per object
        update_keys = class_keys
        select_keys = [class_keys[i] for i in range(len(class_keys))
                       if where_elements[i][0] is True]

        return update_keys, select_keys, clause

    def update(self, new, etdb):
        new_keys, old_keys, clause = self.update_clause()
        c = etdb.conn.cursor()
        elems = tuple([new.__dict__[k] for k in new_keys] + [self.__dict__[k] for k in old_keys])
        logger.info(f'elems: {elems}')
        c.execute(clause, elems)
        self.commit(etdb)

    def where_clause_keys(self, strict=True, only_keys=None):
        if strict:
            class_keys = self.get_attributes()
        else:
            class_keys = self.primary_key

        if only_keys is not None:
            class_keys = [k for k in class_keys if k in only_keys]
        schema_keys = [self.convert_key_from_class_to_schema(k) for k in class_keys]
        return class_keys, schema_keys

    def where_clause(self, strict=False, only_keys=None, conjunction=True, fuzzy=False):
        '''Create select clause using server side information and type information from this object, which may contain data from remote data. The type information is strictly only used to decide whether to generate a "key=?" or "key is NULL".'''
        class_keys, keys = self.where_clause_keys(strict, only_keys)
        # select clause contains either "=?" or "is NULL"
        # in the latter case there is no data element to be supplied to the call to execute, so we need to discern them using the first element in the tuple
        if fuzzy:
            where_elements = [(True, f'{keys[i]} like ?')
                              if self.__dict__[class_keys[i]] is not None
                              else (False, f'{keys[i]} is NULL')
                              for i in range(len(class_keys))]
        else:
            where_elements = [(True, f'{keys[i]}=?')
                              if self.__dict__[class_keys[i]] is not None
                              else (False, f'{keys[i]} is NULL')
                              for i in range(len(class_keys))]
        if conjunction:
            clause = ' and '.join([elm[1] for elm in where_elements])
        else:
            clause = ' or '.join([elm[1] for elm in where_elements])
        # list of class keys to be used for the call to execute
        select_keys = [class_keys[i] for i in range(len(class_keys)) if where_elements[i][0] is True]

        return select_keys, clause

    def select_clause(self, return_all_cols=True, only_keys=None):
        if return_all_cols:
            return '*'
        schema_keys = [self.convert_key_from_class_to_schema(k) for k in only_keys]
        return ', '.join(schema_keys)

    def lookup_for_keys(self, only_keys, etdb, conjunction=True, fuzzy=False, return_all_cols=True):
        class_keys, where_clause = self.where_clause(True, only_keys, conjunction, fuzzy)
        select_clause = self.select_clause(return_all_cols, only_keys)
        c = etdb.conn.cursor()
        elems = tuple(self.__dict__[k] for k in class_keys)
        c.execute(f'select {select_clause} from {self._table_name} where {where_clause}', elems)
        return [col[0] for col in c.description], c.fetchall()

    def lookup_exactly(self, etdb):
        class_keys, where_clause = self.where_clause(strict=True)
        c = etdb.conn.cursor()
        elems = tuple(self.__dict__[k] for k in class_keys)
        c.execute(f'select * from {self._table_name} where {where_clause}', elems)
        return [col[0] for col in c.description], c.fetchall()

    def lookup(self, etdb):
        '''Return either exactly one or zero row in the form (cols, rows).'''
        class_keys, where_clause = self.where_clause(strict=False)
        c = etdb.conn.cursor()
        elems = tuple(self.__dict__[k] for k in class_keys)
        c.execute(f'select * from {self._table_name} where {where_clause}', elems)
        return [col[0] for col in c.description], c.fetchall()

    def insert_clause(self):
        class_keys = self.get_attributes()
        keys = [self.convert_key_from_class_to_schema(k) for k in class_keys]
        column_elements = [(class_keys[i], keys[i]) for i in range(len(class_keys))
                           if self.__dict__[class_keys[i]]]
        column_clause = ', '.join([key for _, key in column_elements])
        values_clause = ', '.join(['?' for _ in column_elements])

        return [class_key for class_key, _ in column_elements], column_clause, values_clause

    def insert(self, etdb):
        class_keys, column_clause, values_clause = self.insert_clause()
        c = etdb.conn.cursor()
        c.execute(f'insert into {self._table_name} ({column_clause}) values ({values_clause})',
                  tuple(self.__dict__[class_key] for class_key in class_keys))
        self.commit(etdb)

    def delete_clause(self):
        raise NotImplementedError('use where_clause instead')

    def delete(self, etdb):
        class_keys, where_clause = self.where_clause(strict=False)
        c = etdb.conn.cursor()
        elems = tuple(self.__dict__[k] for k in class_keys)
        c.execute(f'delete from {self._table_name} where {where_clause}', elems)
        self.commit(etdb)

    def remove(self, etdb):
        return self.delete(etdb)

    @classmethod
    def convert_key_from_class_to_schema(cls, k):
        '''Override this method in sub class if schema and class names differ'''
        return k

    @classmethod
    def convert_key_from_schema_to_class(cls, k):
        '''Override this method in sub class if schema and class names differ'''
        return k

    @classmethod
    def get_descriptions(cls):
        raise(NotImplementedError(f'All sub classes of {cls} need to implement this function!'))

    @classmethod
    def prefix(cls):
        raise(NotImplementedError(f'All sub classes of {cls} need to implement this function!'))


class DB_Element_With_Foreign_Key(DB_Element):
    '''Class for tables which contain foreign keys. All attributes of the primary key of referenced objects are inserted flat as attributes.'''

    # Foreign element classes
    _elements: ClassVar[Dict[str, Type]] = {}
    # Map native to foreign key names
    _primary_to_foreign_key: ClassVar[Dict[str, str]] = {}
    _foreign_to_primary_key: ClassVar[Dict[str, str]] = {fk: pk for pk, fk
                                                         in _primary_to_foreign_key.values()}

    def __init__(self, table_name, referenced_elements: Dict[str, DB_Element]):
        super().__init__()
        self._referenced_elements = referenced_elements

        # add primary key of all referenced tables
        for name, db_elm in referenced_elements.items():
            for pk in db_elm.primary_key:
                self.__dict__[self.primary_to_foreign_key(type(db_elm), pk, name)] = db_elm.__dict__[pk]

    def get_referenced_element(self, name, etdb):
        '''Lookup and return referenced element in db'''
        elm = self._referenced_elements[name]
        cols, rows = elm.lookup(etdb)
        if len(rows) > 0:
            return elm.FromDBEntry(cols, rows[0])
        return None

    def LookupForElements(self, element_names: List[str], etdb) -> List[DB_Element_With_Foreign_Key]:
        keys = [self.primary_to_foreign_key(type(self._referenced_elements[name]), k, name)
                for name in element_names
                for k in self._referenced_elements[name].primary_key]
        cols, rows = self.lookup_for_keys(keys, etdb)
        return [self.FromDBEntry(cols, row) for row in rows]

    @classmethod
    def get_attributes(cls):
        '''Get a list of attributes of the DB object. All members which should *not* appear here must be private (start with "_").'''
        # _elm_attrs = [(elm_cls, elm_cls.get_attributes()) for elm_cls in cls.references]
        # elm_attrs = [cls.primary_to_foreign_key(elm_cls, key) for elm_cls, keys in _elm_attrs for key in keys]
        dummy_elms = {elm_name: elm_cls.Dummy() for elm_name, elm_cls in cls._elements.items()}
        return [k for k in cls(**dummy_elms).__dict__.keys() if not k.startswith('_')]

    @classmethod
    def FromDict(cls, dbdict):
        '''Return an object from the information in etdict or None, if the information is incomplete'''
        # TODO handle None class values from foreign_to_primary_key
        # TODO handle incomplete/malformed dicts
        elems_params = [(elem_name, primary_key, value)
                        for elem_name, primary_key, value in [(*cls.foreign_to_primary_key(k), dbdict[k]) for k in dbdict]]
        elems_param_dict = {}
        non_foreign_param_dict = {}

        for elem_name, primary_key, value in elems_params:
            # non-foreign attributes
            if elem_name is None:
                non_foreign_param_dict[primary_key] = value
            # foreign attributes
            elif elem_name not in elems_param_dict:
                elems_param_dict[elem_name] = {primary_key: value}
            else:
                elems_param_dict[elem_name][primary_key] = value
        # TODO use elm_cls.FromDict?
        elems = {elem_name: cls._elements[elem_name](**elem_params)
                 for elem_name, elem_params in elems_param_dict.items()}

        # initialize missing elements with dummys
        for elem_name, elem in cls._elements.items():
            if elem_name not in elems:
                elems[elem_name] = elem.Dummy()

        return cls(**elems, **non_foreign_param_dict)
    # def where_clause_keys(self, strict=False):
    #     class_foreign_keys = (self.primary_to_foreign_key(cls, key) for cls in self._referenced_tables for key in cls.primary_key)
    #     # convert keys from class foreign key to schema foreign key
    #     foreign_keys = [self.convert_key_from_class_to_schema(key) for key in class_foreign_keys]
    #     if strict:
    #         own_class_keys = self.get_attributes()
    #     else:
    #         own_class_keys = self.primary_key
    #     own_class_non_foreign_keys = [k for k in own_class_keys if k not in class_foreign_keys]
    #     own_non_foreign_keys = [self.convert_key_from_class_to_schema(k) for k in own_class_non_foreign_keys]
    #     return class_foreign_keys + own_class_keys, foreign_keys + own_non_foreign_keys

    @classmethod
    def primary_to_foreign_key(cls, other_cls, k, elm_name):
        '''Convert primary class key to class foreign key. Override if you need to!'''
        if k.lower() in cls._primary_to_foreign_key:
            return cls._primary_to_foreign_key[k.lower()]
        return k

    @classmethod
    def foreign_to_primary_key(cls, k):
        '''Convert class foreign key to primary class key. Override if you need to!'''
        return (None, k)

    @classmethod
    def get_descriptions(cls):
        raise(NotImplementedError('Automatic description inference is not implemented yet!'))

    @classmethod
    def primary_key(cls):
        raise(NotImplementedError('Automatic primary key inference is not implemented yet!'))
