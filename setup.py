# Copyright (C) 2014 Stefan C. Mueller

import os.path
from setuptools import setup, find_packages

if os.path.exists('README.rst'):
    with open('README.rst') as f:
        long_description = f.read()
else:
    long_description = None
    
setup(
    name = 'sourblossom',
    version = '0.1.0',
    description='Asynchronous remote procedure call library',
    long_description=long_description,
    author='Stefan C. Mueller',
    author_email='stefan.mueller@fhnw.ch',
    url='https://github.com/smurn/sourblossom',
    packages = find_packages(),
    install_requires =   ['twisted>=15.0.0', 
                          'utwist>=0.1.3',
                          'twistit>=0.2.2',
                          'picklesize>=0.1.1'],
)
