# -*- coding: utf-8 -*-
from pymogile.backend import Backend
from pymogile.exceptions import MogileFSError

def get_backend():
    return Backend(["127.0.0.1:7001"])

def test_do_request_host_not_exist():
    backend = Backend(["127.0.0.1:7011", "127.0.0.1:7012"])
    try:
        backend.do_request("get_domains")
    except MogileFSError:
        pass
    else:
        assert False

def test_do_request():
    backend = get_backend()
    res = backend.do_request("get_domains")

def test_do_request_cmd_not_exist():
    backend = get_backend()
    try:
        res = backend.do_request("spameggham")
    except MogileFSError:
        pass

def test_do_request_with_no_cmd():
    backend = get_backend()
    try:
        backend.do_request()
    except TypeError:
        pass
    except Exception, e:
        assert False, "TypeError expected, actual %r" % e
    else:
        assert False

