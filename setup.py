#!/usr/bin/python
from setuptools import setup, find_packages

setup(
    name='Coding-Challenge',
    version='1.0',
    install_requires=[
        'apache-beam[gcp]==2.66.0',
        'geopy==2.4.1',
    ],
    extras_require={
        'dev': [
            'pytest==8.4.1',
        ]
    },
    packages=find_packages(exclude=['notebooks', 'tests']),
    py_modules=['config'],
    include_package_data=True,
    description='Coding Challenge'
)