# -*- coding: utf-8 -*-
from __future__ import with_statement

import time, random
from nose import with_setup
from pymogile import Client, Admin

TEST_NS = "mogilefs.client::test_client"
HOSTS   = ["127.0.0.1:7001"]

moga = Admin(HOSTS)

def _setup():
    try:
        moga.create_domain(TEST_NS)
    except:
        pass

def _teardown():
    pass

def test_new_file():
    cl = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())

    with cl.new_file(key) as fp:
        assert fp.__exit__
        fp.write(key)

    assert cl.get_paths(key)
    assert cl.get_file_data(key) == key

def test_read_file():
    cl = Client(TEST_NS, HOSTS)

    cl = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())
    cl.store_content(key, key)

    with cl.read_file(key) as fp:
        assert fp.read() == key

