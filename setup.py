#!/usr/bin/env python
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(name='weather-tobiasli',
                 version='1.1.4',
                 description='Tools for managing weather data using shyft.',
                 author='Tobias Litherland',
                 author_email='tobiaslland@gmail.com',
                 url='https://github.com/tobiasli/weather',
                 packages=setuptools.find_packages(),
                 long_description=long_description,
                 long_description_content_type="text/markdown",
                 classifiers=[
                     "Programming Language :: Python :: 3",
                     "License :: OSI Approved :: MIT License",
                     "Operating System :: OS Independent",
                 ],
                 install_requires=['tregex-tobiasli', 'lnetatmo', 'pytest', 'numpy', 'cryptography', 'rdp', 'shyft.time_series', 'shyft']
                 )