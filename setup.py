try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='Simpycity',
    version='1.0',
    description='A simple functionally-oriented PostgreSQL DB access library.',
    author='Commandprompt, Inc.',
    author_email='support@commandprompt.com',
    url='https://github.com/commandprompt/Simpycity',
    #FIXME
    download_url='',
    install_requires=[
        "psycopg2>=2.5"
    ],
    packages=find_packages(),
    test_suite='nose.collector',
    license='LGPL',
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)",
        "Operating System :: MacOS X",
        "Operating System :: Unix",
        "Topic :: Software Development :: Databases",
    ],
)

