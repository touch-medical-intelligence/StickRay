[![Python](https://img.shields.io/pypi/pyversions/stick_ray.svg)](https://badge.fury.io/py/stick_ray)
[![PyPI](https://badge.fury.io/py/stick_ray.svg)](https://badge.fury.io/py/stick_ray)

Main
Status: ![Workflow name](https://github.com/JoshuaAlbert/stick_ray/actions/workflows/unittests.yml/badge.svg?branch=main)

Develop
Status: ![Workflow name](https://github.com/JoshuaAlbert/stick_ray/actions/workflows/unittests.yml/badge.svg?branch=develop)

## _Our mission is to make scalable resilient stick connections for Ray._

# What is it?

StickRay is:

1) a well-tested sticky connections library for Ray.
2) designed to be as scalable as Ray Serve.
3) implemented with strategies to make it robust to failure and load-based attacks.
4) fully open source, as in, use it for whatever you want.

# Documentation

For examples, check out the [documentation](https://stickray.readthedocs.io/). Ray slides from Ray Summit 2023
presentation
are [here](https://docs.google.com/presentation/d/1CJvhrCTXmH1cwI1-df8JheQW9c9Rv69T2n-xrFhhcAM/edit?usp=sharing).

# Install

**Notes:**

1. StickRay requires >= Python 3.9.
2. It is always highly recommended to use a unique virtual environment for each project.
   To use `miniconda`, have it installed, and run

```bash
# To create a new env, if necessary
conda create -n stick_ray_py python=3.11
conda activate stick_ray_py
```

## For end users

Install directly from PyPi,

```bash
pip install stick_ray
```

## For development

Clone repo `git clone https://www.github.com/JoshuaAlbert/stick_ray.git`, and install:

```bash
cd stick_ray
pip install -r requirements.txt
pip install -r requirements-examples.txt
pip install -r requirements-tests.txt
pip install .
```

# Change Log

19 Sept, 2023 -- StickRay 1.0.0 released
