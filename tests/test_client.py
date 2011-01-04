# -*- coding: utf-8 -*-
import time
import random
from cStringIO import StringIO
from nose import with_setup
from pymogile import Client, Admin, MogileFSError

TEST_NS = "mogilefs.client::test_client"
HOSTS   = ["127.0.0.1:7001"]

moga = Admin(HOSTS)

def _setup():
    try:
        moga.create_domain(TEST_NS)
    except:
        pass

def _teardown():
    #moga.delete_domain(TEST_NS)
    pass

def test_sleep():
    client = Client(TEST_NS, HOSTS)
    client.sleep(1)

def test_list_keys():
    keys = ["spam", "egg", "ham"]
    domain = "test:list_keys:%s:%s:%s" % (random.random(), time.time(), TEST_NS)
    moga.create_domain(domain)
    mogc = Client(domain, HOSTS)

    for k in keys:
        mogc.store_content(k, k)

    try:
        files = mogc.list_keys()
        assert len(files) == 3

        files = mogc.list_keys(limit=1)
        assert len(files) == 1

        files = mogc.list_keys(prefix='sp')
        assert len(files) == 1
    finally:
        for k in keys:
            mogc.delete(k)
        moga.delete_domain(domain)

@with_setup(_setup, _teardown)
def test_new_file():
    client = Client(TEST_NS, HOSTS)

    key = 'test_file_%s_%s' % (random.random(), time.time())
    fp = client.new_file(key)
    assert fp is not None

    data = "0123456789" * 50
    fp.write(data)
    fp.close()

    paths = client.get_paths(key)
    #assert len(paths) > 1, "should exist in one ore more places"
    assert paths

    content = client.get_file_data(key)
    assert content == data

@with_setup(_setup, _teardown)
def test_new_large_file():
    client = Client(TEST_NS, HOSTS)

    key = 'test_file_%s_%s' % (random.random(), time.time())
    fp = client.new_file(key, largefile=True)
    assert fp is not None

    for x in xrange(50):
        fp.write("0123456789")
    fp.close()

    paths = client.get_paths(key)
    #assert len(paths) > 1, "should exist in one ore more places"
    assert paths

    content = client.get_file_data(key)
    assert content == "0123456789" * 50

@with_setup(_setup, _teardown)
def test_new_file_unexisting_class():
    def func(largefile):
        client = Client(TEST_NS, HOSTS)

        key = 'test_file_%s_%s' % (random.random(), time.time())
        try:
            fp = client.new_file(key, cls='spam')
        except MogileFSError:
            pass
        else:
            assert False

    for largefile in (True, False):
        yield func, largefile

@with_setup(_setup, _teardown)
def test_new_file_unexisting_domain():
    def func(largefile):
        client = Client('spamdomain', HOSTS)

        key = 'test_file_%s_%s' % (random.random(), time.time())
        try:
            fp = client.new_file(key)
        except MogileFSError, e:
            pass
        else:
            assert False

    for largefile in (True, False):
        yield func, largefile

@with_setup(_setup, _teardown)
def test_closed_file():
    def func(largefile):
        client = Client(TEST_NS, HOSTS)
        key = 'test_file_%s_%s' % (random.random(), time.time())
        fp = client.new_file(key, largefile=largefile)
        fp.write("spam")
        fp.close()

        try:
            fp.write("egg")
        except:
            pass
        else:
            assert False, "operation not permitted to closed file"

        try:
            fp.read()
        except:
            pass
        else:
            assert False, "operation not permitted to closed file"

        try:
            fp.seek(0)
        except:
            pass
        else:
            assert False, "operation not permitted to closed file"

        try:
            fp.tell()
        except:
            pass
        else:
            assert False, "operation not permitted to closed file"

    for largefile in (False, True):
        yield func, largefile

@with_setup(_setup, _teardown)
def test_readonly_file():
    client = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())
    client.store_content(key, "SPAM")

    fp = client.read_file(key)
    try:
        fp.write("egg")
    except:
        pass
    else:
        assert False, "operation not permitted to read-only file"

@with_setup(_setup, _teardown)
def test_seek():
    def func(largefile):
        client = Client(TEST_NS, HOSTS)
        key = 'test_file_%s_%s' % (random.random(), time.time())

        fp = client.new_file(key, largefile=largefile)
        fp.write("SPAM")
        fp.seek(1)
        assert fp.tell() == 1
        fp.write("p")
        fp.close()

        assert client.get_file_data(key) == "SpAM"

    for largefile in (False, True):
        yield func, largefile

@with_setup(_setup, _teardown)
def test_seek_negative():
    def func(largefile):
        client = Client(TEST_NS, HOSTS)
        key = 'test_file_%s_%s' % (random.random(), time.time())

        fp = client.new_file(key, largefile=largefile)
        fp.write("SPAM")
        fp.seek(-10)
        assert fp.tell() == 0
        fp.write("s")
        fp.close()

        assert client.get_file_data(key) == "sPAM"

    for largefile in (False, True):
        yield func, largefile

@with_setup(_setup, _teardown)
def test_seek_read():
    client = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())

    client.store_content(key, "0123456789")

    fp = client.read_file(key)
    fp.seek(1)
    assert fp.tell() == 1
    content = fp.read(3)

    assert content == "123"
    assert fp.tell() == 4

@with_setup(_setup, _teardown)
def test_rename():
    client = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())
    client.new_file(key).write(key)
    paths = client.get_paths(key)
    assert paths

    newkey = 'test_file2_%s_%s' % (random.random(), time.time())
    client.rename(key, newkey)
    paths = client.get_paths(newkey)
    assert paths

    content = client.get_file_data(newkey)
    assert content == key

@with_setup(_setup, _teardown)
def test_rename_dupliate_key():
    client = Client(TEST_NS, HOSTS)
    key1 = 'test_file_%s_%s' % (random.random(), time.time())
    key2 = 'key2:' + key1

    client.store_content(key1, key1)
    client.store_content(key2, key2)

    try:
        client.rename(key1, key2)
    except MogileFSError, e:
        pass
    else:
        assert False

@with_setup(_setup, _teardown)
def test_store_file():
    client = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())

    data = ''.join(random.choice("0123456789") for x in xrange(8192 * 2))
    input = StringIO(data)
    length = client.store_file(key, input)
    assert length == len(data)

    content = client.get_file_data(key)
    assert content == data

@with_setup(_setup, _teardown)
def test_store_content():
    client = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())

    data = ''.join(random.choice("0123456789") for x in xrange(8192 * 2))
    length = client.store_content(key, data)
    assert length == len(data)

    content = client.get_file_data(key)
    assert content == data

@with_setup(_setup, _teardown)
def test_read_file():
    client = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())
    client.store_content(key, key)

    fp = client.read_file(key)
    assert fp is not None
    assert key == fp.read()

@with_setup(_setup, _teardown)
def test_delete():
    client = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())

    client.new_file(key).write("SPAM")
    paths = client.get_paths(key)
    assert paths

    client.delete(key)
    paths = client.get_paths(key)
    assert not paths

@with_setup(_setup, _teardown)
def test_mkcol():
    client = Client(TEST_NS, HOSTS)
    for x in xrange(0, 1000):
        key = 'test_file_%s_%s_%d' % (random.random(), time.time(), x)
        client.new_file(key).write("SPAM%s" % x)
        paths = client.get_paths(key)
        assert paths

@with_setup(_setup, _teardown)
def test_edit_file():
    # TODO
    # PASS
    return

    cl = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())

    cl.store_content(key, "SPAM")
    assert cl.get_paths(key)
    assert cl.get_file_data(key) == "SPAM"

    fp = cl.edit_file(key)
    assert fp
    fp.write("s")
    fp.seek(2)
    fp.write("a")
    fp.close()

    assert cl.get_file_data(key) == "sPaM"

@with_setup(_setup, _teardown)
def test_file_like_object():
    def func(largefile):
        client = Client(TEST_NS, HOSTS)
        key = 'test_file_%s_%s' % (random.random(), time.time())

        fp = client.new_file(key, largefile=largefile)
        fp.write("spam\negg\nham\n")

        fp.seek(0)
        line = fp.readline()
        assert line == "spam\n"
        line = fp.readline()
        assert line == "egg\n"
        line = fp.readline()
        assert line == "ham\n"
        line = fp.readline()
        assert line == ''

        fp.seek(0)
        lines = fp.readlines()
        assert lines == ["spam\n", "egg\n", "ham\n"]

        fp.close()

    for largefile in (False, True):
        yield func, largefile
