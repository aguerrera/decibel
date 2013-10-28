#!/usr/bin/env python

#
# tests for decibel.py database layer
#
#
# copyright andy guerrera
#

import datetime
import unittest
import uuid

from hapn.core import decibel 

_connectionString = "dbname='testdb' user='postgres' host='localhost' password='hey-there-you-dude'"

class ThingyDo(object):
    id = 0L
    name = ""
    created = None
    active = False

create_thing_table_sql = """
CREATE TABLE thing
(
  id bigserial NOT NULL,
  active boolean,
  created timestamp without time zone,
  name text,
  CONSTRAINT thing_pkey PRIMARY KEY (id )
)
WITH (
  OIDS=FALSE
);
ALTER TABLE thing
  OWNER TO postgres;
"""

def decibel_test_setup():
    cols = ["id", "name", "created", "active"]
    om = decibel.ObjectMap(id_col="id", cols=cols, obj=ThingyDo, table="thing")
    db = decibel.Decibel(connectionString=_connectionString)
    return db



class DecibelTests(unittest.TestCase):
    db = None
    def setUp(self):
        self.db = decibel_test_setup()

    def tearDown(self):
        ts = self.db.list_all(ThingyDo)
        for t in ts:
            self.db.delete(ThingyDo, t.id)

    def create_random_thingy(self):
        t = ThingyDo()
        t.name = uuid.uuid4().hex
        t.created = datetime.datetime.now()
        t.active = True
        id = self.db.insert(t)
        return id

    def test_insert(self):
        id1 = self.create_random_thingy()
        self.assertTrue(id1 != None)

    def test_update(self):
        id1 = self.create_random_thingy()
        self.assertTrue(id1 != None)
        t = self.db.find_by_id(ThingyDo, id1)
        self.assertEqual(id1, t.id)
        t.name = "NEW Name"
        self.db.update(t)
        t2 = self.db.find_by_id(ThingyDo, id1)
        self.assertEqual(t2.name, t.name)

    def test_delete(self):
        id1 = self.create_random_thingy()
        self.assertTrue(id1 != None)
        self.db.delete(ThingyDo, id1)
        t = self.db.find_by_id(ThingyDo, id1)
        self.assertEqual(t, None)

    def test_select_one(self):
        name = uuid.uuid4().hex + 'random-name'
        t = ThingyDo()
        t.name = name
        t.created = datetime.datetime.now()
        t.active = True
        id = self.db.insert(t)
        self.assertTrue(id)
        t2 = self.db.select_one(ThingyDo, "select * from thing where name=%s", name)
        self.assertTrue(t2.id)
        t3 = self.db.select_one(ThingyDo, "select * from thing where name=%s", "some garbage" + name)
        self.assertFalse(t3)

    def test_query(self):
        id1 = self.create_random_thingy()
        id2 = self.create_random_thingy()
        id3 = self.create_random_thingy()
        self.assertTrue(id1 < id3)
        ts = self.db.query(ThingyDo, "select * from thing where id>%s", id1)
        self.assertTrue(len(ts) > 0)

    def test_list_all(self):
        id1 = self.create_random_thingy()
        id2 = self.create_random_thingy()
        id3 = self.create_random_thingy()
        ts = self.db.list_all(ThingyDo, "andy", filtersql=' where name=%s')
        self.assertFalse(len(ts) > 0)
        ts = self.db.list_all(ThingyDo)
        self.assertTrue(len(ts) > 0)


if __name__ == '__main__':
    unittest.main()