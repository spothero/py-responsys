from setuptools import setup

setup(
    name='responsys_client',
    packages=['responsys_client'],  # this must be the same as the name above
    version='0.2.0',
    install_requires=[
        'pytz==2016.7',
        'requests==2.18.4',
        'mock==2.0.0',
    ],
    description='This is an Oracle Responsys REST API client written in Python 2.7.',
    author='Nicholas Kincaid',
    author_email='nbkincaid@gmail.com',
    url='https://github.com/spothero/py-responsys',
    download_url='https://github.com/spothero/py-responsys/tarball/0.1.8',
    keywords=['testing'],
    classifiers=["Programming Language :: Python :: 2.7",
                 "Topic :: Software Development :: Libraries :: Python Modules"]
)
