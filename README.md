# MogileFS client library for Python

## Installation

```bash
$ pip install pymogile
```

## Examples

### Connect to MogileFS

```python
>>> from pymogile import Client, MogileFSError
>>> datastore = Client(domain='test', trackers=['127.0.0.1:7001'])
```

### New file

```python
>>> fp = datastore.new_file('foobar.txt')
>>> fp.write('foo')
>>> fp.close()
```

### Get paths

```python
>>> datastore.get_paths('foobar.txt')
['http://127.0.0.1:7500/dev4/0/000/251/0000251237.fid', 'http://127.0.0.1:7500/dev6/0/000/251/0000251237.fid']
>>> print datastore.get_paths('404.txt')
[]
```

### Get file data

```python
>>> datastore.get_file_data('404.txt')
>>> datastore.get_file_data('foobar.txt')
'foo'
```

### File info

```python
>>> datastore.file_info('foobar.txt')
{'class': 'default',
 'devcount': '2',
 'domain': 'test',
 'fid': '0000251237',
 'key': 'foobar.txt',
 'length': 3}
>>> datastore.file_info('foobar.txt', devices=True)
{'class': 'default',
 'devcount': '2',
 'devids': ['4', '6'],
 'domain': 'test',
 'fid': '0000251237',
 'key': 'foobar.txt',
 'length': 3}
```

### Rename file

```python
>>> datastore.rename('404.txt', 'test.txt')
False
>>> datastore.rename('foobar.txt', 'foo.txt')
True
>>> datastore.get_file_data('foobar.txt')
>>> datastore.get_file_data('test.txt')
>>> datastore.get_file_data('foo.txt')
'foo'
```

### Remove file

```python
>>> datastore.delete('foobar.txt')
False
>>> datastore.delete('foo.txt')
True
>>> datastore.get_file_data('foo.txt')
```
