from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='Simpycity',
    version='2.0.0',
    description='A database-respecting object-relational mapper for PostgreSQL.',
    long_description=long_description,
    author='Command Prompt, Inc.',
    author_email='support@commandprompt.com',
    url='https://github.com/commandprompt/Simpycity',
    download_url='https://github.com/commandprompt/Simpycity/releases/tag/2.0.0',
    install_requires=[
        "psycopg2>=2.5"
    ],
    packages=find_packages(),
    test_suite='nose.collector',
    license='LGPL',
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Natural Language :: English'
    ],
    keywords='orm postgresql',
)

