#! coding: utf-8
# pylint: disable-msg=W0311
"""
implement from description of MogileFS-Client version 1.13
http://search.cpan.org/~dormando/MogileFS-Client/lib/MogileFS/Client.pm

This module is a client library for the MogileFS distributed file system
"""
from pymogile.backend import Backend
from pymogile.exceptions import MogileFSError
from pymogile.file import NormalHTTPFile, LargeHTTPFile


class Client(object):
  def __init__(self, domain, trackers, readonly=False):
    """Create new Client object with the given list of trackers."""
    self.readonly = bool(readonly)
    self.domain = domain
    self.backend = Backend(trackers, timeout=3)

  def run_hook(self, hookname, *args):
    pass

  def add_hook(self, hookname, *args):
    pass

  def add_backend_hook(self):
    raise NotImplementedError()

  def errstr(self):
    raise NotImplementedError()

  def errcode(self):
    raise NotImplementedError()


  @property
  def last_tracker(self):
    """
    Returns a tuple of (ip, port), representing the last mogilefsd
    'tracker' server which was talked to.
    """
    return self.backend.last_host_connected

  def new_file(self, key, cls=None, largefile=False, content_length=0,
               create_open_arg=None, create_close_arg=None, opts=None):
    """
    Start creating a new filehandle with the given key,
    and option given class and options.

    Returns a filehandle you should then print to,
    and later close to complete the operation.

    NOTE: check the return value from close!
    If your close didn't succeed, the file didn't get saved!
    """
    self.run_hook('new_file_start', key, cls, opts)

    create_open_arg = create_open_arg or {}
    create_close_arg = create_close_arg or {}

    # fid should be specified, or pass 0 meaning to auto-generate one
    fid = 0
    params = {'domain': self.domain,
              'key': key,
              'fid': fid,
              'multi_dest': 1}
    if cls is not None:
      params['class'] = cls
    res = self.backend.do_request('create_open', params)
    if not res:
      return None

    # [(devid, path), (devid, path),... ]
    dests = []
    # determine old vs. new format to populate destinations
    if 'dev_count' not in res:
      dests.append((res['devid'], res['path']))
    else:
      for x in xrange(1, int(res['dev_count']) + 1):
        devid_key = 'devid_%d' % x
        path_key = 'path_%s' % x
        dests.append((res[devid_key], res[path_key]))

    main_dest = dests[0]
    main_devid, main_path = main_dest

    self.run_hook("new_file_end", key, cls, opts)

    # TODO
    if largefile:
      file_class = LargeHTTPFile
    else:
      file_class = NormalHTTPFile

    return file_class(mg=self,
                      fid=res['fid'],
                      path=main_path,
                      devid=main_devid,
                      backup_dests=dests,
                      cls=cls,
                      key=key,
                      content_length=content_length,
                      create_close_arg=create_close_arg,
                      overwrite=1)

  def edit_file(self, key, overwrite=False):
    """Edit the file with the the given key.

    NOTE: edit_file is currently EXPERIMENTAL and not recommended for production use.
    MogileFS is primarily designed for storing files for later retrieval, rather than editing.
    Use of this function may lead to poor performance and, until it has been proven mature, should be considered to also potentially cause data loss.

    NOTE: use of this function requires support for the DAV 'MOVE' verb and partial PUT (i.e. Content-Range in PUT) on the back-end storage servers (e.g. apache with mod_dav).

    Returns a seekable filehandle you can read/write to.
    Calling this function may invalidate some or all URLs you currently have for this key, so you should call ->get_paths again afterwards if you need them.

    On close of the filehandle, the new file contents will replace the previous contents (and again invalidate any existing URLs).

    By default, the file contents are preserved on open, but you may specify the overwrite option to zero the file first.
    The seek position is at the beginning of the file, but you may seek to the end to append.
    """
    raise NotImplementedError()

  def read_file(self, key):
    """
    Read the file with the the given key.
    Returns a seekable filehandle you can read() from.
    Note that you cannot read line by line using <$fh> notation.

    Takes the same options as get_paths
    (which is called internally to get the URIs to read from).
    """
    paths = self.get_paths(key)
    if not paths:
      return None
    path = paths[0]
    backup_dests = [(None, p) for p in paths[1:]]
    return LargeHTTPFile(path=path, backup_dests=backup_dests, readonly=1)


  def store_file(self, key, fp, cls=None, chunk_size=8192):
    """
    Wrapper around new_file, print, and close.

    Given a key, class, and a filehandle or filename, stores the file
    contents in MogileFS.  Returns the number of bytes stored on success,
    undef on failure.
    """
    if self.readonly:
      return False

    params = {}
    if chunk_size:
      params['chunk_size'] = chunk_size
    if cls:
      params['class'] = cls
    params[key] = key

    self.run_hook('store_file_start', params)

    try:
      new_file = self.new_file(key, cls)
      _bytes = 0
      while True:
        buf = fp.read(chunk_size)
        if not buf:
          break
        _bytes += len(buf)
        new_file.write(buf)

      self.run_hook('store_file_end', params)
    finally:
      fp.close()
      new_file.close()

    return _bytes

  def store_content(self, key, content, cls=None, **opts):
    """
    Wrapper around new_file, print, and close.  Given a key, class, and
    file contents (scalar or scalarref), stores the file contents in
    MogileFS. Returns the number of bytes stored on success, undef on
    failure.
    """
    if self.readonly:
      return False

    self.run_hook('store_content_start', key, cls, opts)

    output = self.new_file(key, cls, None, **opts)
    output.write(content)
    output.close()

    self.run_hook('store_content_end', key, cls, opts)

    return len(content)

  def get_paths(self, key, noverify=1, zone='alt', pathcount=2):
    """
    Given a key, returns an array of all the locations (HTTP URLs) that the file
    has been replicated to.
    """
    self.run_hook('get_paths_start', key)

    params = {'domain': self.domain,
              'key': key,
              'noverify': noverify and 1 or 0,
              'zone': zone,
              'pathcount': pathcount}
    try:
      res = self.backend.do_request('get_paths', params)
      paths = [res["path%d" % x] for x in xrange(1, int(res["paths"]) + 1)]
    except (MogileFSError, MogileFSError):
      paths = []

    self.run_hook('get_paths_end', key)
    return paths

  def get_file_data(self, key, timeout=10):
    """
    Returns scalarref of file contents in a scalarref.
    Don't use for large data, as it all comes back to you in one string.
    """
    fp = self.read_file(key)
    if not fp:
      return None
    try:
      content = fp.read()
      return content
    finally:
      fp.close()

  def delete(self, key):
    """
    Delete a file from MogileFS
    """
    try:
      if self.readonly:
        return False
      self.backend.do_request('delete', {'domain': self.domain, 'key': key})
      return True
    except MogileFSError:
      return False

  def rename(self, from_key, to_key):
    """
    Rename file (key) in MogileFS from oldkey to newkey.
    Returns true on success, failure otherwise
    """
    try:
      if self.readonly:
        return False
      params =  {'domain': self.domain,
                 'from_key': from_key,
                 'to_key': to_key}
      self.backend.do_request('rename', params)
      return True
    except MogileFSError:
      return False

  def list_keys(self, prefix=None, after=None, limit=None):
    """
    Used to get a list of keys matching a certain prefix.

    $prefix specifies what you want to get a list of.

    $after is the item specified as a return value from this function last time
          you called it.

    $limit is optional and defaults to 1000 keys returned.

    In list context, returns ($after, $keys).
    In scalar context, returns arrayref of keys.
    The value $after is to be used as $after when you call this function again.

    When there are no more keys in the list,
    you will get back undef or an empty list
    """
    params = {'domain': self.domain}
    if prefix:
      params['prefix'] = prefix
    if after:
      params['after'] = after
    if limit:
      params['limit'] = limit

    res = self.backend.do_request('list_keys', params)
    results = []
    for x in xrange(1, int(res['key_count']) + 1):
      results.append(res['key_%d' % x])
    return results

  def keys(self, prefix=None):
    """
    Get all keys matching a certain prefix
    """
    params = {'domain': self.domain}
    if prefix:
      params['prefix'] = prefix
    results = []
    while True:
      res = self.backend.do_request('list_keys', params)
      for x in res.keys():
        if x not in ['key_count', 'next_after']:
          results.append(res[x])
      if len(res) < 1002:
        break
      params['after'] = res['next_after']
    return list(set(results))


  def foreach_key(self, *args, **kwds):
    """
    Functional interface/wrapper around list_keys.

    Given some %OPTIONS (currently only one, "prefix"),
    calls your callback for each key matching the provided prefix.
    """
    raise NotImplementedError()

  def update_class(self, key, new_class):
    """
    Update the replication class of a pre-existing file,
    causing the file to become more or less replicated.
    """
    try:
      if self.readonly:
        return False
      params = {"domain": self.domain,
                "key": key,
                "class": new_class}
      res = self.backend.do_request("updateclass", params)
      return res
    except MogileFSError:
      return False

  def sleep(self, duration):
    """
    just makes some sleeping happen.  first and only argument is number of
    seconds to instruct backend thread to sleep for.
    """
    try:
      self.backend.do_request("sleep", {'duration': duration})
      return True
    except MogileFSError:
      return False

  def set_pref_ip(self, *ips):
    """
    Weird option for old, weird network architecture.
    Sets a mapping table of preferred alternate IPs, if reachable.
    For instance, if trying to connect to 10.0.0.2 in the above example,
    the module would instead try to connect to 10.2.0.2 quickly first,
    then then fall back to 10.0.0.2 if 10.2.0.2 wasn't reachable.
    expects as argument a tuple of ("standard-ip", "preferred-ip")
    """
    self.backend.set_pref_ip(*ips)

