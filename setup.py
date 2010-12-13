#!/usr/bin/env python

"""
Setup the MRedis
"""

from setuptools import setup

setup(name='MRedis',
      version="0.2",
      description="MRedis is a multi-server wrapper for the excellent Python Redis client.",
      author="Gavin M. Roy",
      author_email="gmr@myyearbook.com",
      license="BSD",
      url="http://github.com/gmr/mredis",
      packages=['mredis'],
      install_requires = ['redis'],
      zip_safe=True)
