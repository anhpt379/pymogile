#! coding: utf-8
# pylint: disable-msg=W0311
import unittest
from pymogile.backend import Backend
from pymogile.exceptions import MogileFSError


class TestBackend(unittest.TestCase):
  def setUp(self):
    self.backend = Backend(['127.0.0.1:7001'])
  
  def test_do_request_trackers_not_exist(self):
    backend = Backend(["127.0.0.1:7011", "127.0.0.1:7012"])
    try:
      backend.do_request("get_domains")
    except MogileFSError:
      pass
    else:
      assert False
      
  def test_do_request_one_tracker_down(self):
    backend = Backend(["127.0.0.1:7001", "127.0.0.1:7012"])
    try:
      backend.do_request("get_domains")
    except MogileFSError:
      assert False

  def test_do_request(self):
      res = self.backend.do_request("get_domains")
      assert res
  
  def test_do_request_cmd_not_exist(self):
    try:
      self.backend.do_request("asdfkljweioav")
    except MogileFSError:
      pass
    else:
      assert False
  
  def test_do_request_with_no_cmd(self):
    try:
      self.backend.do_request()   # pylint: disable-msg=E1120
    except TypeError:
      pass
    except Exception, e:
      assert False, "TypeError expected, actual %r" % e
    else:
      assert False
          
          
if __name__ == "__main__":
  unittest.main()

