from setuptools import setup, find_packages
from dss_simulator.__version__ import version

setup(
    name='ssbgp-dss-simulator',
    version=version,
    description='Simulator component for SS-BGP distributed simulation system',
    url='https://github.com/ssbgp/dss-simulator',
    license='MIT',
    author='David Fialho',
    author_email='fialho.david@protonmail.com',

    packages=find_packages(),

    install_requires=[],

    extras_require={
        'test': ['pytest'],
    },

    package_data={
        'dss_simulator': [
            'logs.ini'
        ],
    },

    entry_points={
        'console_scripts': [
            'dss-simulator=dss_simulator.main:main',
        ],
    }
)
