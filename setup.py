#!/usr/bin/env python

from distutils.core import setup

setup(name='1up-coding-challenge',
      version='0.0',
      description='Simple data processor for FHIR data',
      author='Michael Burton',
      author_email='mjburton11@gmail.com',
      packages=['1up-coding-challenge'],
      install_requires=['pandas', 'numpy']
      )
