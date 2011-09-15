from setuptools import setup, find_packages

version = '2.0'

CLASSIFIERS = """\
Development Status :: 4 - Beta
License :: OSI Approved :: GPL
Programming Language :: Python
Topic :: Software Development :: Libraries :: Python Modules
"""

setup(name='pymogile',
      version=version,
      description="pymogile",
      long_description="""Python MogileFS Client""",
      classifiers=filter(None, map(str.strip, CLASSIFIERS.splitlines())),
      keywords='',
      author='AloneRoad',
      author_email='aloneroad@gmail.com',
      license='GPL',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False
      )
