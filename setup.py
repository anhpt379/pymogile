from setuptools import setup, find_packages

setup(
    name='pymogile',
    version='2.1.0',
    description="pymogile",
    long_description="""Python MogileFS Client""",
    classifiers=(
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ),
    author='AloneRoad',
    author_email='aloneroad@gmail.com',
    maintainer='stefanfoulis',
    maintainer_email='stefan.foulis@gmail.com',
    license='GPL',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    include_package_data=True,
    zip_safe=False
)
