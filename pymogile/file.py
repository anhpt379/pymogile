#! coding: utf-8
import put
import logging
import httplib
import urlparse
from cStringIO import StringIO

from pymogile.exceptions import MogileFSError, HTTPError


def is_success(response):
    return response.status >= 200 and response.status < 300


def get_content_length(response):
    try:
        return long(response.getheaders('content-length'))
    except (TypeError, ValueError):
        return 0


class HTTPFile(object):
    def __init__(self, mg, fid, key, cls, create_close_arg=None):
        self.mg = mg
        self.fid = fid
        self.key = key
        self.cls = cls
        self.create_close_arg = create_close_arg or {}
        self._is_closed = False

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        if not self._is_closed:
            self.close()

    def __del__(self):
        if not self._is_closed:
            try:
                self.close()
                self._is_closed = True
            except Exception, e:
                logging.debug("got an exception in __del__: %s" % str(e))

    def _makedirs(self, path):
        url = urlparse.urlsplit(path)
        if url.scheme == 'http':
            connection = httplib.HTTPConnection
        elif url.scheme == 'https':
            connection = httplib.HTTPSConnection
        else:
            raise ValueError("unsupported url scheme")

        # MogileFS file path usually looks like
        # /dev1/0/000/000/0000000900.fid
        # or ["", "dev1", "0", "000", "000", "0000000900.fid"]
        # when splitted.
        if not path.endswith(".fid"):
            raise ValueError(
                "Invalid path '%s'. Maybe the file isn't MogileFS FID"
            )

        # first remove fid
        elements = url.path.split("/")[:-1]
        length = len(elements)
        if length != 5:
            raise ValueError(
                "Invalid path '%s'. Maybe the file isn't MogileFS FID" %
                url.path
            )

        created = False
        for idx in xrange(3, length + 1):
            # recursively create directories
            # /dev1/0/
            # /dev1/0/000/
            # /dev1/0/000/000/
            parent = "/".join(elements[:idx]) + "/"
            conn = connection(url.netloc)
            conn.request("MKCOL", parent)
            res = conn.getresponse()
            if res.status >= 200 and res.status < 300:
                created = idx == length
            elif res.status >= 400 and res.status < 500:
                # keep going
                continue
            else:
                # unexpected status code while making directories
                raise HTTPError(res.status, res.reason)

        return created

    def _request(self, path, method, *args, **kwds):
        url = urlparse.urlsplit(path)
        if url.scheme == 'http':
            connection = httplib.HTTPConnection
        elif url.scheme == 'https':
            connection = httplib.HTTPSConnection
        elif not url.scheme:
            raise ValueError("url scheme is empty")
        else:
            raise ValueError("unsupported url scheme '%s'" % url.scheme)

        conn = connection(url.netloc)
        target = urlparse.urlunsplit(
            (None, None, url.path, url.query, url.fragment)
        )
        conn.request(method, target, *args, **kwds)
        res = conn.getresponse()
        if is_success(res):
            return res

        if method == 'PUT' and res.status == 403:
            created = self._makedirs(path)
            if created:
                conn = connection(url.netloc)
                conn.request(method, target, *args, **kwds)
                res = conn.getresponse()
                if is_success(res):
                    return res

        raise HTTPError(res.status, res.reason)


class LargeHTTPFile(HTTPFile):
    def __init__(
        self,
        path,
        backup_dests=None,
        overwrite=False,
        mg=None,
        fid=None,
        devid=None,
        cls=None,
        key=None,
        readonly=False,
        create_close_arg=None,
        **kwds
    ):

        super(LargeHTTPFile, self
              ).__init__(mg, fid, key, cls, create_close_arg)

        if backup_dests is None:
            backup_dests = []

        for tried_devid, tried_path in [(devid, path)] + list(backup_dests):
            self._path = tried_path

            if overwrite:
                # Ensure file overwritten/created, even if they don't print anything
                res = self._request(
                    tried_path,
                    "PUT",
                    "", headers={'Content-Length': '0'}
                )
            else:
                res = self._request(tried_path, "HEAD")

            if is_success(res):
                if overwrite:
                    self.length = 0
                else:
                    self.length = get_content_length(res)

                self.devid = tried_devid
                self.path = tried_path
                break
        else:
            raise HTTPError("couldn't connect to any storage nodes")

        self.overwrite = overwrite
        self.readonly = readonly

        self._is_closed = 0
        self._pos = 0
        self._eof = 0

    def read(self, n=-1):
        if self._is_closed:
            return False

        if self._eof:
            return ''

        headers = {}
        if n == 0:
            return ''
        elif n > 0:
            headers['Content-Range'] = 'bytes=%d-%d' % (
                self._pos, self._pos + n - 1
            )
        else:
            # if n is negative, then read whole content
            pass

        try:
            res = self._request(self._path, "GET", headers=headers)
        except HTTPError, e:
            if e.code == httplib.REQUESTED_RANGE_NOT_SATISFIABLE:
                self._eof = 1
                return ''
            else:
                raise e

        content = res.read()
        self._pos += len(content)

        if n < 0:
            self._eof = 1

        return content

    def readline(self, length=None):
        raise NotImplementedError()

    def readlines(self, sizehint=0):
        raise NotImplementedError()

    def write(self, content):
        if self._is_closed:
            return False
        if self.readonly:
            return False

        length = len(content)
        start = self._pos
        end = self._pos + length - 1
        headers = {
            'Content-Range': "bytes %d-%d/*" % (start, end),
            'Content-Length': length
        }
        self._request(self._path, "PUT", content, headers=headers)

        if self._pos + length > self.length:
            self.length = self._pos + length

        self._pos += len(content)

    def close(self):
        if not self._is_closed:
            self._is_closed = 1
            if self.devid:
                params = {
                    'fid': self.fid,
                    'devid': self.devid,
                    'domain': self.mg.domain,
                    'size': self.length,
                    'key': self.key,
                    'path': self.path,
                }
                if self.create_close_arg:
                    params.update(self.create_close_arg)
                try:
                    self.mg.backend.do_request('create_close', params)
                except MogileFSError, e:
                    if e.err != 'empty_file':
                        raise

    def seek(self, pos, mode=0):
        if self._is_closed:
            return False
        if pos < 0:
            pos = 0
        self._pos = pos

    def tell(self):
        if self._is_closed:
            return False
        return self._pos


class NormalHTTPFile(HTTPFile):
    def __init__(
        self,
        path,
        devid,
        backup_dests=None,
        mg=None,
        fid=None,
        cls=None,
        key=None,
        create_close_arg=None,
        **kwds
    ):

        super(NormalHTTPFile, self
              ).__init__(mg, fid, key, cls, create_close_arg)

        if backup_dests is None:
            backup_dests = []
        self._fp = StringIO()
        self._paths = [(devid, path)] + list(backup_dests)
        self._is_closed = 0

    def paths(self):
        return self._paths

    def read(self, n=-1):
        return self._fp.read(n)

    def readline(self, *args, **kwds):
        return self._fp.readline(*args, **kwds)

    def readlines(self, *args, **kwds):
        return self._fp.readlines(*args, **kwds)

    def write(self, content):
        self._fp.write(content)

    def close(self):
        if not self._is_closed:
            self._is_closed = True

            #      content = self._fp.getvalue()
            #      self._fp.close()

            for tried_devid, tried_path in self._paths:
                try:
                    #          self._request(tried_path, "PUT", content)
                    self._fp.seek(0)
                    put.putfile(self._fp, tried_path)
                    devid = tried_devid
                    path = tried_path
                    break
                except HTTPError, e:
                    continue
            else:
                devid = None
                path = None

            self._fp.seek(0, 2)
            size = self._fp.tell()
            self._fp.close()
            if devid:
                params = {
                    'fid': self.fid,
                    'domain': self.mg.domain,
                    'key': self.key,
                    'path': path,
                    'devid': devid,
                    'size': size
                }
                if self.create_close_arg:
                    params.update(self.create_close_arg)
                try:
                    self.mg.backend.do_request('create_close', params)
                except MogileFSError, e:
                    if e.err != 'empty_file':
                        raise

    def seek(self, pos, mode=0):
        return self._fp.seek(pos, mode)

    def tell(self):
        return self._fp.tell()
