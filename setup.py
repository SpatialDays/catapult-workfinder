#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

with open('requirements.txt') as requirements_file:
    requirements = [line.rstrip('\n') for line in requirements_file]

setup_requirements = [ ]

with open('requirements_dev.txt') as requirements_file:
    test_requirements = [line.rstrip('\n') for line in requirements_file]

setup(
    author="Emily Selwood",
    author_email='Emily.selwood@sa.catapult.org.uk',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    description="find work for the ard processing",
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme + '\n\n' + history,
    keywords='workfinder',
    name='workfinder',
    packages=find_packages(include=['workfinder'], exclude=['venv*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    include_package_data=True,
    url='https://github.com/emilyselwood/workfinder',
    version='0.2.31'
)
