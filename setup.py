#!/usr/bin/env python

from setuptools import find_packages
from setuptools import setup

setup_requires = [
    'ray[serve]',
    'fair_async_rlock',
    'pydantic',
    'numpy',
    'ujson'
]

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='stick_ray',
      version='1.0.0',
      description='Sticky connections for Ray',
      long_description=long_description,
      long_description_content_type="text/markdown",
      url="https://github.com/joshuaalbert/stick_ray",
      author='Joshua G. Albert',
      author_email='josh.albert@touchmedical.ca',
      setup_requires=setup_requires,
      tests_require=[
          'pytest>=2.8',
      ],
      package_dir={'': './'},
      packages=find_packages('./'),
      classifiers=[
          "Programming Language :: Python :: 3",
          "License :: OSI Approved :: Apache Software License",
          "Operating System :: OS Independent",
      ],
      python_requires='>=3.9',
      )
