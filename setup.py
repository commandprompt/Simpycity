try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='Simpycity',
    version='2.0.0',
    description='A database-respecting object-relational mapper for PostgreSQL.',
    long_description="""Simpycity is an object-relational mapper. It seamlessly maps PostgreSQL query and function result sets to Python classes and class attributes. It allows for the easy and rapid development of query- and stored procedure-based data representations. Simpycity leverages PostgreSQL's powerful composite type system, and the advanced type handling of the psycopg2 database access library.""",
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

