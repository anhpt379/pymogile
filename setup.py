from setuptools import setup, find_packages
import sys, os

version = '0.1.0'

CLASSIFIERS = """\
Development Status :: 3 - Alpha
License :: OSI Approved :: GPL
Programming Language :: Python
Topic :: Software Development :: Libraries :: Python Modules
"""

setup(name='pymogile',
      version=version,
      description="pymongile",
      long_description="""Python MogileFS Client""",
      classifiers=filter(None, map(str.strip, CLASSIFIERS.splitlines())),
      keywords='',
      author='Chihio Sakatoku',
      author_email='csakatoku@gmail.com',
      #url='',
      license='GPL',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          # -*- Extra requirements: -*-
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      test_suite='nose.collector'
      )
