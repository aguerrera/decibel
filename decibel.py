#!/usr/bin/env python

#
# decibel mini obj mapper
# copyright andy guerrera
# public domain license

import psycopg2
import psycopg2.extras
import datetime

object_map = {}
sql_map = {}

class ObjectMap(object):

    def __init__(self, cols=None, id_col=None, table=None, obj=None):
        self.cols = cols
        self.id_col = id_col
        if obj:
            newobj = object.__new__(obj)
            self.cls = newobj.__class__
        self.table = table
        if (self.cls):
            object_map[self.cls] = self


    def get_non_id_cols(self):
        nidcols = self.cols
        if self.id_col in nidcols:
            nidcols.remove(self.id_col)
        return nidcols

    def get_all_cols(self):
        allcols = self.cols
        if not self.id_col in allcols:
            allcols.insert(0, self.id_col)
        return allcols

    def get_insert_sql(self):
        key = self.table + "_insert"
        if sql_map.has_key(key):
            return sql_map[key]
        inscols = self.get_non_id_cols()
        sql = "insert into {0} ({1}) values ({2}) returning {3}".format(self.table, ",".join(inscols), ",".join('%s' for i in range(len(inscols))), self.id_col)
        sql_map[key] = sql
        return sql

    def get_update_sql(self):
        key = self.table + "_update"
        if sql_map.has_key(key):
            return sql_map[key]
        updcols = self.get_non_id_cols()
        upd = ",".join([c+'=%s' for c in updcols])
        wc = "{0}=%s".format(self.id_col)
        sql = "update {0} set {1} where {2}".format(self.table, upd, wc)
        sql_map[key] = sql
        return sql

    def get_delete_sql(self):
        key = self.table + "_delete"
        if sql_map.has_key(key):
            return sql_map[key]
        wc = "{0}=%s".format(self.id_col)
        sql = "delete from {0} where {1}".format(self.table, wc)
        sql_map[key] = sql
        return sql

    def get_select_all_sql(self):
        key = self.table + "_select_all"
        if sql_map.has_key(key):
            return sql_map[key]
        selcols =  self.get_all_cols()
        sql = "select {0} from {1}".format(",".join(selcols), self.table)
        sql_map[key] = sql
        return sql

    def get_select_sql(self):
        key = self.table + "_select"
        if sql_map.has_key(key):
            return sql_map[key]
        selcols =  self.get_all_cols()
        wc = "{0}=%s".format(self.id_col)
        sql = "select {0} from {1} where {2}".format(",".join(selcols), self.table, wc)
        sql_map[key] = sql
        return sql

class Decibel(object):

    has_conn = False
    conn = None
    dsn = None

    def __init__(self, dsn=None, conn=None):
        self.dsn = dsn
        if conn:
            self.conn = conn
            self.has_conn = True

    def __del__(self):
        self.close()

    def set_conn(self, conn):
        self.has_conn = true
        self.conn = conn

    def get_conn(self):
        if not self.conn and not self.dsn:
            return None
        if not self.conn:
            self.conn = psycopg2.connect(self.dsn)
        self.has_conn = True
        return self.conn

    def insert(self, m):
        if not self.has_conn:
            self.get_conn()
        om = object_map[m.__class__]        
        sql = om.get_insert_sql()
        sql = self.sql_check(sql)
        cur = self.conn.cursor()
        plist = []
        for c in om.get_non_id_cols():
            plist.append(getattr(m, c))
        prms = tuple(plist)
        try:
            cur.execute(sql, prms)
            self.conn.commit()
        except Exception, e:
            self.conn.rollback()
            raise
        finally:
            pass            
        r = cur.fetchone()
        id = None
        if len(r) > 0:
            id = r[0]
        return id

    def update(self, m):
        if not self.has_conn:
            self.get_conn()
        om = object_map[m.__class__]        
        sql = om.get_update_sql()
        sql = self.sql_check(sql)
        cur = self.conn.cursor()
        plist = []
        for c in om.get_non_id_cols():
            plist.append(getattr(m, c))
        plist.append(getattr(m, om.id_col))
        prms = tuple(plist)
        try:
            cur.execute(sql, prms)
            self.conn.commit()
        except Exception, e:
            self.conn.rollback()
            raise
        finally:
            pass

    def delete(self, M, id):
        if not self.has_conn:
            self.get_conn()
        mobj = object.__new__(M)
        sql = object_map[mobj.__class__].get_delete_sql()
        sql = self.sql_check(sql)
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (id,))
            self.conn.commit()
        except Exception, e:
            self.conn.rollback()
            raise
        finally:
            pass            

    def list_all(self, M, *args, **kwargs):
        mobj = object.__new__(M)
        sql = object_map[mobj.__class__].get_select_all_sql();
        items = self.query(M, sql, *args, **kwargs)
        return items

    def find_by_id(self, M, id):
        if not self.has_conn:
            self.get_conn()
        mobj = object.__new__(M)
        om = object_map[mobj.__class__]
        sql = om.get_select_sql()
        return self.select_one(M, sql, (id,))

    def select_one(self, M, sql, *args):
        if not self.has_conn:
            self.get_conn()
        sql = self.sql_check(sql)
        prms =  tuple(args)
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            cur.execute(sql, prms)
        except Exception, e:
            self.conn.rollback()
            raise
        finally:
            pass            
        r = cur.fetchone()
        if r:
            o = self.map_object(M, r)
    	    return o
    	return None

    def mogrify(self, M, sql, *args, **kwargs):
        if not self.has_conn:
            self.get_conn()
        filtersql = ""
        if kwargs.has_key('filtersql'):
            filtersql = kwargs['filtersql']
        if filtersql:
            sql = sql + filtersql;
        sql = self.sql_check(sql)
        prms = tuple(args)
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return cur.mogrify(sql, prms)

    def query(self, M, sql, *args, **kwargs):
        if not self.has_conn:
            self.get_conn()
        filtersql = ""
        if kwargs.has_key('filtersql'):
            filtersql = kwargs['filtersql']
        if filtersql:
            sql = sql + filtersql;
        sql = self.sql_check(sql)
        prms = tuple(args)
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            cur.execute(sql, prms)
        except Exception, e:
            self.conn.rollback()
            raise
        finally:
            pass            
        rs = cur.fetchall()
        items = []
        for r in rs:
            o = self.map_object(M, r)
            items.append(o)
        return items

    def map_object(self, M, r):
        o = object.__new__(M)
        for k in r.keys():
            setattr(o, k, r[k])
        return o

    def execute(self, sql, *args):
        if not self.has_conn:
            self.get_conn()
        sql = self.sql_check(sql)
        prms = tuple(args)
        cur = self.conn.cursor()
        try:
            cur.execute(sql, prms)
        except Exception, e:
            self.conn.rollback()
            raise
        finally:
            pass            

    def close(self):
        if self.has_conn:
            self.conn.close()

    def sql_check(self, sql):
        if not sql.endswith(";"):
            sql = sql + ";"
        return sql

