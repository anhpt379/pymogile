#! coding: utf-8
# pylint: disable-msg=W0311
import unittest
from pymogile import Admin


DOMAIN   = "testdomain"
CLASS    = "testclass"
TRACKERS = ["127.0.0.1:7001"]

class AdminTest(unittest.TestCase):
  def setUp(self):
    self.mogilefs = Admin(TRACKERS)
    try:
      self.mogilefs.delete_class(DOMAIN, CLASS)
      self.mogilefs.delete_domain(DOMAIN)
    except:
      pass
  
  def test_create_delete_domain(self):  
    self.assertEqual(self.mogilefs.delete_domain(DOMAIN), False)
    self.assertEqual(self.mogilefs.create_domain(DOMAIN), True)
    self.assertEqual(self.mogilefs.create_domain(DOMAIN), False)
    self.assertEqual(self.mogilefs.delete_domain(DOMAIN), True)
  
  def test_get_domains(self):
    self.mogilefs.create_domain(DOMAIN)
    self.mogilefs.create_class(DOMAIN, CLASS, 2)
    
    ret = self.mogilefs.get_domains()
    assert DOMAIN in ret
    assert CLASS in ret[DOMAIN], ret
    assert ret[DOMAIN][CLASS] == 2, ret
    
    self.mogilefs.delete_domain(DOMAIN)
  
  def test_create_delete_class(self):
    self.assertEqual(self.mogilefs.create_domain(DOMAIN), True)
    self.assertEqual(self.mogilefs.create_class(DOMAIN, CLASS, 2), True)
    self.assertEqual(self.mogilefs.create_class(DOMAIN, CLASS, 2), False)
    self.assertEqual(self.mogilefs.delete_class(DOMAIN, CLASS), True)
    self.assertEqual(self.mogilefs.delete_class(DOMAIN, CLASS), False)
  
  def test_get_server_settings(self):
    res = self.mogilefs.server_settings()
    assert res is not None
    assert 'schema_version' in res
  
  def test_set_server_setting(self):
    self.mogilefs.set_server_setting("memcache_servers", "127.0.0.1:11211")
    res = self.mogilefs.server_settings()
    assert res['memcache_servers'] == '127.0.0.1:11211'
  
  def test_get_devices(self):
    devices = self.mogilefs.get_devices()
    assert devices
    for d in devices:
      assert d.has_key('devid')
      assert d.has_key('hostid')
      assert d.has_key('status')
      assert d.has_key('observed_state')
      assert d.has_key('utilization')
      assert d.has_key('mb_total')
      assert d.has_key('mb_used')
      assert d.has_key('weight')
      #assert isinstance(d['mb_total'], (int, long))
      #assert isinstance(d['mb_used'], (int, long))
      #assert isinstance(d['weight'], (int, long))
  
  def test_create_delete_host(self):
    self.assertEqual(self.mogilefs.create_host('testhost', '192.168.0.1', 7500), 
                     {'status': 'down', 'hostid': '1', 'http_port': '7500', 'hostname': 'testhost', 'hostip': '192.168.0.1'})
    self.assertEqual(self.mogilefs.update_host('testhost', port=7501, status='alive'), 
                     {'status': 'alive', 'hostid': '1', 'http_port': '7501', 'hostname': 'testhost', 'hostip': '192.168.0.1'})
    self.assertEqual(self.mogilefs.delete_host('testhost'), True)  
  
#  def test_update_device(self):
#      ## TODO
#      self.mogilefs.update_device('colinux', 1, status='down', weight=80)
#      self.mogilefs.update_device('colinux', 1, status='alive', weight=100)
#  
#  def test_change_device_state(self):
#      ## TODO
#      self.mogilefs.change_device_state('colinux', 1, 'down')
#      self.mogilefs.change_device_state('colinux', 1, 'alive')
#  
#  def test_change_device_weight(self):
#      ## TODO
#      self.mogilefs.change_device_weight('colinux', 1, 80)
#      self.mogilefs.change_device_weight('colinux', 1, 100)
#  
#      try:
#          self.mogilefs.change_device_weight('colinux', 1, "SPAM")
#      except ValueError:
#          pass
#      else:
#          assert False, "ValueError expected for invalid weight"
  
#  def test_fsck_start(self):
#      self.mogilefs.fsck_start()
#      status = self.mogilefs.fsck_status()
#      assert status['running'] == '1'
#  
#      self.mogilefs.fsck_stop()
#      status = self.mogilefs.fsck_status()
#      assert status['running'] == '0'
#  
#  def test_fsck_reset(self):
#      self.mogilefs.fsck_reset(0, 0)
#  
#  def test_fsck_log_rows(self):
#      self.mogilefs.fsck_log_rows()
#  
#  def test_fsck_clearlog(self):
#      self.mogilefs.fsck_clearlog()
#  
#  def test_list_fids(self):
#      self.mogilefs.list_fids(1, 10)
#  
#  def test_get_stats(self):
#      self.mogilefs.get_stats()
#  
#  def test_slave_list(self):
#      assert False
#  
#  def test_slave_add(self):
#      assert False
#  
#  def test_slave_modify(self):
#      assert False
#  
#  def test_slave_delete(self):
#      assert False

if __name__ == "__main__":
  unittest.main()
