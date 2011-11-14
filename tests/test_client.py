#! coding: utf-8
# pylint: disable-msg=W0311
import time
import random
import unittest
from cStringIO import StringIO
from pymogile import Client, Admin, MogileFSError

TEST_NS = "mogilefs.client::test_client"
HOSTS   = ["127.0.0.1:7001"]

class TestClient(unittest.TestCase):
  def setUp(self):
    self.moga = Admin(HOSTS)
    try:
      self.moga.create_domain(TEST_NS)
    except MogileFSError:
      pass
  
  def tearDown(self):
    try:
      self.moga.delete_domain(TEST_NS)
    except MogileFSError:
      pass
  
  def test_sleep(self):
    client = Client(TEST_NS, HOSTS)
    self.assertEqual(client.sleep(1), True)
  
  def test_list_keys(self):
    keys = ["spam", "egg", "ham"]
    domain = "test:list_keys:%s:%s:%s" % (random.random(), time.time(), TEST_NS)
    self.moga.create_domain(domain)
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
      self.moga.delete_domain(domain)

  def test_new_file(self):
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
#  
#  def test_new_large_file(self):
#    client = Client(TEST_NS, HOSTS)
#
#    key = 'test_file_%s_%s' % (random.random(), time.time())
#    fp = client.new_file(key, largefile=True)
#    assert fp is not None
#
#    for _ in xrange(50):
#      fp.write("0123456789")
#    fp.close()
#
#    paths = client.get_paths(key)
#    #assert len(paths) > 1, "should exist in one ore more places"
#    assert paths
#
#    content = client.get_file_data(key)
#    assert content == "0123456789" * 50
  
  def test_new_file_unexisting_class(self):
    client = Client(TEST_NS, HOSTS)

    key = 'test_file_%s_%s' % (random.random(), time.time())
    self.assertRaises(MogileFSError, client.new_file, key, 'unexisting')
  
  def test_new_file_unexisting_domain(self):
    client = Client('unexisting_domain', HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())
    self.assertRaises(MogileFSError, client.new_file, key)
  
  def test_closed_file(self):
    client = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())
    fp = client.new_file(key)
    fp.write("spam")
    fp.close()

    self.assertRaises(ValueError, fp.write, "egg")
    self.assertRaises(ValueError, fp.read)
    self.assertRaises(ValueError, fp.seek, 0)
    self.assertRaises(ValueError, fp.tell)

  
#  def test_readonly_file(self):
#    client = Client(TEST_NS, HOSTS)
#    key = 'test_file_%s_%s' % (random.random(), time.time())
#    client.store_content(key, "SPAM")
#
#    fp = client.read_file(key)
#    try:
#      fp.write("egg")
#    except:
#      pass
#    else:
#      assert False, "operation not permitted to read-only file"
  
  def test_seek(self):
    client = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())

    fp = client.new_file(key)
    fp.write("SPAM")
    fp.seek(1)
    self.assertEqual(fp.tell(), 1)
    
    fp.write("p")
    fp.close()
    self.assertEqual(client.get_file_data(key), "SpAM")
  
  def test_seek_negative(self):
    client = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())

    fp = client.new_file(key)
    fp.write("SPAM")
    fp.seek(-10)
    self.assertEqual(fp.tell(), 0)
    
    fp.write("s")
    fp.close()
    self.assertEqual(client.get_file_data(key), "sPAM")
  
  def test_seek_read(self):
    client = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())

    client.store_content(key, "0123456789")

    fp = client.read_file(key)
    fp.seek(1)
    self.assertEqual(fp.tell(), 1)
    
    content = fp.read(3)
    assert content == "123"
    self.assertEqual(fp.tell(), 4)

  def test_rename(self): 
    client = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())
    client.new_file(key).write(key)
    paths = client.get_paths(key)
    self.assertTrue(paths)

    newkey = 'test_file2_%s_%s' % (random.random(), time.time())
    client.rename(key, newkey)
    paths = client.get_paths(newkey)
    self.assertTrue(paths)

    content = client.get_file_data(newkey)
    assert content == key

  def test_rename_dupliate_key(self): 
    client = Client(TEST_NS, HOSTS)
    key1 = 'test_file_%s_%s' % (random.random(), time.time())
    key2 = 'key2:' + key1

    client.store_content(key1, key1)
    client.store_content(key2, key2)

    self.assertEqual(client.rename(key1, key2), False)

  def test_store_file(self): 
    client = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())

    data = ''.join(random.choice("0123456789") for _ in xrange(8192 * 2))
    fp = StringIO(data)
    length = client.store_file(key, fp)
    self.assertEqual(length, len(data))

    content = client.get_file_data(key)
    self.assertEqual(content, data)

  def test_store_content(self): 
    client = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())

    data = ''.join(random.choice("0123456789") for _ in xrange(8192 * 2))
    length = client.store_content(key, data)
    self.assertEqual(length, len(data))

    content = client.get_file_data(key)
    self.assertEqual(content, data)

  def test_read_file(self): 
    client = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())
    client.store_content(key, key)

    fp = client.read_file(key)
    self.assertNotEqual(fp, None)
    self.assertEqual(key, fp.read())

  def test_delete(self): 
    client = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())

    client.new_file(key).write("SPAM")
    paths = client.get_paths(key)
    self.assertTrue(paths)

    client.delete(key)
    paths = client.get_paths(key)
    self.assertFalse(paths)

  def test_mkcol(self): 
    client = Client(TEST_NS, HOSTS)
    for x in xrange(0, 10):
      key = 'test_file_%s_%s_%d' % (random.random(), time.time(), x)
      client.new_file(key).write("SPAM%s" % x)
      paths = client.get_paths(key)
      self.assertTrue(paths)


  def test_edit_file(self):
    cl = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())

    cl.store_content(key, "SPAM")
    self.assertTrue(cl.get_paths(key))
    self.assertEqual("SPAM", cl.get_file_data(key))

    fp = cl.edit_file(key)
    self.assertTrue(fp)
    fp.write("s")
    fp.seek(2)
    fp.write("a")
    fp.close()

    self.assertEqual("sPaM", cl.get_file_data(key))

  def test_append_file(self):
    cl = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())

    cl.store_content(key, "SPAM")
    self.assertTrue(cl.get_paths(key))
    self.assertEqual("SPAM", cl.get_file_data(key))

    fp = cl.edit_file(key)
    self.assertTrue(fp)
    fp.seek(4)
    fp.write("HamEggs")
    fp.close()

    self.assertEqual("SPAMHamEggs", cl.get_file_data(key))
  
  def test_file_like_object(self): 
    client = Client(TEST_NS, HOSTS)
    key = 'test_file_%s_%s' % (random.random(), time.time())

    fp = client.new_file(key)
    fp.write("spam\negg\nham\n")

    fp.seek(0)
    line = fp.readline()
    self.assertEqual(line, "spam\n")
    line = fp.readline()
    
    self.assertEqual(line, "egg\n")
    line = fp.readline()
    
    self.assertEqual(line, "ham\n")
    
    line = fp.readline()
    self.assertEqual(line, '')

    fp.seek(0)
    lines = fp.readlines()
    self.assertEqual(lines, ["spam\n", "egg\n", "ham\n"])

    fp.close()
          
if __name__ == "__main__":
  unittest.main()
