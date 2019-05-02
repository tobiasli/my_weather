#!/usr/bin/env python

from distutils.core import setup

setup(name='my_weather',
      version='1.0',
      description='Tools for managing netatmo data.',
      author='Tobias Litherland',
      author_email='tobiaslland@gmail.com',
      url='https://github.com/tobiasli/my_weather/projects/1',
      packages=['tregex-tobiasli', 'shyft', 'bokeh', 'lnetatmo', 'numpy'],
     )