# -*- coding: utf-8 -*-
from pymogile import Admin, MogileFSError

DOMAIN   = "testdomain"
CLASS    = "testclass"
TRACKERS = ["127.0.0.1:7001"]

def test_create_domain():
    moga = Admin(TRACKERS)

    moga.delete_domain(DOMAIN)
    moga.create_domain(DOMAIN)

    try:
        moga.create_domain(DOMAIN)
    except MogileFSError:
        # domain exists
        pass
    else:
        assert False, "the domain %s should exist" % DOMAIN
    assert moga.delete_domain(DOMAIN)

def test_delete_domain():
    moga = Admin(TRACKERS)
    try:
        moga.delete_domain(DOMAIN)
    except MogileFSError:
        # domain does not exist
        pass
    else:
        assert False, "the domain %s should not exist" % DOMAIN
    assert moga.create_domain(DOMAIN)
    assert moga.delete_domain(DOMAIN)

def test_get_domains():
    moga = Admin(TRACKERS)
    try:
        moga.create_domain(DOMAIN)
        moga.create_class(DOMAIN, CLASS, 2)
        ret = moga.get_domains()
        assert DOMAIN in ret
        assert CLASS in ret[DOMAIN], ret
        assert ret[DOMAIN][CLASS] == 2, ret
    finally:
        moga.delete_domain(DOMAIN)

def test_create_class():
    moga = Admin(TRACKERS)
    moga.create_domain(DOMAIN)
    moga.create_class(DOMAIN, CLASS, 2)
    moga.delete_class(DOMAIN, CLASS)

def test_get_server_settings():
    moga = Admin(TRACKERS)
    res = moga.server_settings()
    assert res is not None
    assert 'schema_version' in res
    try:
        version = int(res['schema_version'])
    except:
        assert False, "schema version must be an integer"

def test_set_server_settings():
    moga = Admin(TRACKERS)
    moga.set_server_settings("memcache_servers", "127.0.0.1:11211")
    res = moga.server_settings()
    assert res['memcache_servers'] == '127.0.0.1:11211'

def test_get_devices():
    moga = Admin(TRACKERS)
    devices = moga.get_devices()
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

def test_create_host():
    moga = Admin(TRACKERS)
    try:
        moga.create_host('testhost', '192.168.0.1', 7500)
    finally:
        moga.delete_host('testhost')

def test_create_host():
    moga = Admin(TRACKERS)
    try:
        moga.create_host('testhost', '192.168.0.1', 7500)
        moga.update_host('testhost', port=7501, status='alive')
    finally:
        moga.delete_host('testhost')


def test_update_device():
    moga = Admin(TRACKERS)
    ## TODO
    moga.update_device('colinux', 1, status='down', weight=80)
    moga.update_device('colinux', 1, status='alive', weight=100)

def test_change_device_state():
    moga = Admin(TRACKERS)
    ## TODO
    moga.change_device_state('colinux', 1, 'down')
    moga.change_device_state('colinux', 1, 'alive')

def test_change_device_weight():
    moga = Admin(TRACKERS)
    ## TODO
    moga.change_device_weight('colinux', 1, 80)
    moga.change_device_weight('colinux', 1, 100)

    try:
        moga.change_device_weight('colinux', 1, "SPAM")
    except ValueError:
        pass
    else:
        assert False, "ValueError expected for invalid weight"

def test_fsck_start():
    moga = Admin(TRACKERS)

    moga.fsck_start()
    status = moga.fsck_status()
    assert status['running'] == '1'

    moga.fsck_stop()
    status = moga.fsck_status()
    assert status['running'] == '0'

def test_fsck_reset():
    moga = Admin(TRACKERS)
    moga.fsck_reset(0, 0)

def test_fsck_log_rows():
    moga = Admin(TRACKERS)
    moga.fsck_log_rows()

def test_fsck_clearlog():
    moga = Admin(TRACKERS)
    moga.fsck_clearlog()

def test_list_fids():
    moga = Admin(TRACKERS)
    moga.list_fids(1, 10)

def test_get_stats():
    moga = Admin(TRACKERS)
    moga.get_stats()

def test_slave_list():
    assert False

def test_slave_add():
    assert False

def test_slave_modify():
    assert False

def test_slave_delete():
    assert False

