#! coding: utf-8
# pylint: disable-msg=W0311
from pymogile.backend import Backend
from pymogile.exceptions import MogileFSError


class Admin(object):
  def __init__(self, trackers, readonly=False, timeout=None):
    self.readonly = bool(readonly)
    self.backend = Backend(trackers, timeout)
    
  def replicate_now(self):
    return self.backend.do_request("replicate_now")

  def get_hosts(self, hostid=None):
    if hostid:
      params = {'hostid': hostid}
    else:
      params = None
    res = self.backend.do_request("get_hosts", params)
    results = []
    fields = ["hostid", 
              "status", 
              "hostname", 
              "hostip", 
              "http_port", 
              "http_get_port", 
              "altip altmask"]
    hosts = int(res['hosts']) + 1
    for ct in xrange(1, hosts):
      results.append(dict([(f, res['host%d_%s' % (ct, f)]) for f in fields]))
    return results

  def get_devices(self, devid=None):
    if devid:
      params = {'devid': devid}
    else:
      params = None
    res = self.backend.do_request("get_devices", params)
    ret = []
    for x in xrange(1, int(res['devices']) + 1):
      device = {}
      for k in ('devid', 'hostid', 'status', 'observed_state', 'utilization'):
        device[k] = res.get('dev%d_%s' % (x, k))

      for k in ('mb_total', 'mb_used', 'weight'):
        value = res.get('dev%d_%s' % (x, k))
        if value:
          device[k] = int(value)
        else:
          device[k] = None
      ret.append(device)
    return ret

  def list_fids(self, from_fid, to_fid):
    """
    get raw information about fids, for enumerating the dataset
       ( from_fid, to_fid )
    returns:
       {fid => {dict with keys: domain, class, devcount, length, key}}
    """
    res = self.backend.do_request('list_fids', {'from': from_fid, 'to': to_fid})
    results = {}
    for x in xrange(1, int(res['fid_count']) + 1):
      key = 'fid_%d_fid' % x
      results[key] = dict([(k, res['fid_%d_%s' % (x, k)]) \
                           for k in ('key', 'length', 'class', 'domain', 'devcount')])
    return results

  def clear_cache(self):
    return self.backend.do_request('clear_cache')

  def get_domains(self):
    """
    get a dict of the domains we know about in the format of
       {
         "domain_name": {
           "class_name": mindevcount, 
           "class_name": mindevcount, 
           ... 
                        }, 
         ... 
      }
    """
    res = self.backend.do_request('get_domains')
    domain_length = int(res['domains'])
    ret = {}
    for x in xrange(1, domain_length + 1):
      domain_name = res['domain%d' % x]
      ret.setdefault(domain_name, {})
      class_length = int(res['domain%dclasses' % x])
      for y in xrange(1, class_length + 1):
        k = 'domain%dclass%dname' % (x, y)
        v = 'domain%dclass%dmindevcount' % (x, y)
        ret[domain_name][res[k]] = int(res[v])
    return ret

  def create_domain(self, domain):
    """
    create a new domain
    """
    if self.readonly:
      return False
    res = self.backend.do_request('create_domain', {'domain': domain})
    if res['domain'] == domain:
      return True
    else:
      return False

  def delete_domain(self, domain):
    """
    delete a domain
    """
    if self.readonly:
      return False
    try:
      res = self.backend.do_request('delete_domain', {'domain': domain})
    except MogileFSError:
      return False
    if res['domain'] == domain:
      return True
    else:
      return False

  def create_class(self, domain, cls, mindevcount):
    """
    create a class within a domain
    """
    return self._modify_class('create', domain, cls, mindevcount)

  def update_class(self, domain, cls, mindevcount):
    """
    update a class's mindevcount within a domain
    """
    return self._modify_class('update', domain, cls, mindevcount)

  def delete_class(self, domain, cls):
    """
    delete a class
    """
    if self.readonly:
      return False
    res = self.backend.do_request("delete_class", 
                                  {'domain': domain, 'class': cls})
    if res['class'] == cls:
      return True
    else:
      return False

  def create_host(self, host, ip, port, getport=None, status=None):
    params = {'host': host, 'ip': ip, 'port': port, 'getport': getport}
    if status:
      params['status'] = status
    return self._modify_host('create', params)

  def update_host(self, host, ip=None, port=None, getport=None, status=None):
    params = {'host': host}
    if ip:
      params['ip'] = ip
    if port:
      params['port'] = port
    if getport:
      params['getport'] = True
    if status:
      params['status'] = status
    return self._modify_host('update', params)

  def delete_host(self, host):
    self.backend.do_request("delete_host", {'host': host})

  def create_device(self, hostname, devid, hostip=None, state=None):
    params = {'hostname': hostname, 'devid': devid}
    if hostip:
      params['hostip'] = hostip
    if state:
      params['state'] = state
    return self.backend.do_request('create_device', params)

  def update_device(self, host, device, status=None, weight=None):
    if status:
      self.change_device_state(host, device, status)

    if weight:
      self.change_device_weight(host, device, weight)
    return True

  def change_device_state(self, host, device, state):
    """
    change the state of a device; pass in the hostname of the host the
    device is located on, the device id number, and the state you want
    the host to be set to.
    """
    params = {'host': host, 'device': device, 'state': state}
    return self.backend.do_request('set_state', params)

  def change_device_weight(self, host, device, weight):
    """
    change the weight of a device by passing in the hostname and
    the device id
    """
    if not isinstance(weight, (int, long)):
      raise ValueError('argument weight muse be an integer')
    params = {'host': host, 'device': device, 'weight': weight}
    return self.backend.do_request('set_weight', params)
  
  def _get_slave_keys(self):
    res = self.backend.do_request("server_setting", {"key": "slave_keys"})
    if not res:
      return {}

    value = res['value']
    slave_keys = {}
    for slave in value.split(','):
      key, weight = (slave.split("=", 1) + [None])[:2]
      # Weight can be zero, 
      # so don't default to 1 if it's defined and longer than 0 characters.
      try:
        weight = int(weight)
        if not weight:
          weight = 1
      except (TypeError, ValueError):
        weight = 1

      slave_keys[key] = weight
    return slave_keys

  def _set_slave_keys(self, keys):
    buf = []
    for key, weight in keys.items():
      try:
        weight = int(weight)
        if weight != 1:
          key = "%s=%d" % (key, weight)
      except (TypeError, ValueError):
        pass

      buf.append(key)

    self.backend.do_request("set_server_setting",
                            {'key': 'slave_keys', 'value': ''.join(buf)})

  def slave_list(self):
    keys = self._get_slave_keys()
    ret = {}
    for key in keys:
      res = self.backend.do_request('server_setting',
                                    {'key': 'slave_%s' % key})
      if not res:
        continue

      value = res['value']
      dsn, username, password = (value.split('|') + ['', ''])[:3]
      ret[key] = (dsn, username, password)

    return ret

  def slave_add(self, key, dsn, username, password):
    keys = self._get_slave_keys()
    if key in keys:
      return

    value = '|'.join([dsn, username, password])
    self.backend.do_request("set_server_setting",
                            {'key': key, 'value': value})
    keys[key] = None
    self._set_slave_keys(keys)

  def slave_modify(self, key, **opts):
    keys = self._get_slave_keys()
    if key not in keys:
      # slave not fuond
      return
    res = self.backend.do_request("server_setting", {"key": "slave_%s" % key})
    value = res['value']
    dsn, username, password = (value.split('|') + ['', ''])[:3]

    dsn = opts.get('dsn') or dsn
    username = opts.get('username') or username
    password = opts.get('password') or password

    value = '|'.join([dsn, username, password])
    res = self.backend.do_request('set_server_setting', 
                                  {'key': 'slave_%s' % key, 'value': value})

  def slave_delete(self, key):
    slave_keys = self._get_slave_keys()
    if not slave_keys:
      return None

    if key not in slave_keys:
      return False

    self.backend.do_request('set_server_setting',
                            {key: "slave_%s" % key})
    del slave_keys[key]
    self._set_slave_keys(slave_keys)
    
  def fsck_start(self):
    return self.backend.do_request("fsck_start")

  def fsck_stop(self):
    return self.backend.do_request("fsck_stop")

  def fsck_reset(self, policy_only, startpos):
    return self.backend.do_request("fsck_reset",
                            {'policy_only': policy_only, 'startpos': startpos})

  def fsck_clearlog(self):
    return self.backend.do_request("fsck_clearlog")

  def fsck_status(self):
    return self.backend.do_request("fsck_status")

  def fsck_log_rows(self, after_logid=None):
    params = {}
    if after_logid:
      params['after_logid'] = after_logid
    res = self.backend.do_request("fsck_getlog", params)

    row_count = int(res['row_count'])
    ret = []
    for x in xrange(1, row_count + 1):
      rec = {}
      for k in ("logid", "utime", "fid", "evcode", "devid"):
        rec[k] = res.get("row_%d_%s" % (x, k))
      ret.append(rec)
    return ret

  def set_server_setting(self, key, value):
    params = {'key': key, 'value': value}
    return self.backend.do_request("set_server_setting", params)

  def server_settings(self):
    res = self.backend.do_request("server_settings")
    if not res:
      return
    ret = {}
    for x in xrange(1, int(res["key_count"]) + 1):
      key = res.get("key_%d" % x, '')
      value = res.get("value_%d" % x, '')
      ret[key] = value
    return ret

  def _modify_class(self, verb, domain, cls, mindevcount, replpolicy=None):
    if self.readonly:
      return False
    params = {'domain': domain, 
              'class': cls, 
              'mindevcount': mindevcount}
    if replpolicy:
      params['replpolicy'] = replpolicy
    res = self.backend.do_request("%s_class" % verb, params)
    if res['class'] == cls:
      return True
    else:
      return False

  def _modify_host(self, verb, params):
    if self.readonly:
      return False
    try:
      return self.backend.do_request("%s_host" % verb, params)
    except MogileFSError:
      return False
  
  ## Extra 
  #
  def get_freespace(self, devid=None):
    """Get the free space for the entire cluster, or a specific node"""
    return sum([x['mb_free'] for x in  self.get_devices(devid)])

  def get_stats(self):
    params = {'all': 1}
    res = self.backend.do_request('stats', params)

    ret = {}
    # get replication statistics
    if 'replicationcount' in res:
      replication = ret.setdefault('replication', {})
      for x in xrange(1, int(res['replicationcount']) + 1):
        domain = res.get('replication%ddomain' % x, '')
        cls = res.get('replication%dclass' % x, '')
        devcount = res.get('replication%ddevcount' % x, '')
        fields = res.get('replication%dfields' % x)
        (replication.setdefault(domain, {}).setdefault(cls, {}))[devcount] = fields

    # get file statistics
    if 'filescount' in res:
      files = ret.setdefault('files', {})
      for x in xrange(1, int(res['filescount']) + 1):
        domain = res.get('files%ddomain' % x, '')
        cls = res.get('files%dclass' % x, '')
        (files.setdefault(domain, {}))[cls] = res.get('files%dfiles' % x)

    # get device statistics
    if 'devicescount' in res:
      devices = ret.setdefault('devices', {})
      for x in xrange(1, int(res['devicescount']) + 1):
        key = res.get('devices%did' % x, '')
        devices[key] = {'host': res.get('devices%dhost' % x),
                        'status': res.get('devices%dstatus' % x),
                        'files': res.get('devices%dfiles' % x),
                }

    if 'fidmax' in res:
      ret['fids'] = {'max': res['fidmax']}

    # return the created response
    return ret
