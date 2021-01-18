#!/usr/bin/env python

from distutils.core import setup

setup(name='oneup_coding_challenge',
      version='0.0',
      description='Simple data processor for FHIR data',
      author='Michael Burton',
      author_email='mjburton11@gmail.com',
      packages=['oneup_coding_challenge'],
      install_requires=['pandas', 'numpy', 'tabulate']
      )
