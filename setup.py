"""
Memc
"""

from setuptools import setup

setup(
    name = 'memc',
    version = '1.0',
    url = '',
    license="It's be thinking. Maybe It'll be MIT license, Rakuten, Inc.",
    description="client library for memcache-compatible products",
    long_description = __doc__,
    zip_safe = False,
    platforms = 'any',
    install_requires = [],
    packages = [
        'memc',
        ],
)

