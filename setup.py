try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='Simpycity',
    version='0.3.1',
    description='A simple functionally-oriented PostgreSQL DB access library.',
    author='Aurynn Shaw, Commandprompt, Inc.',
    author_email='ashaw@commandprompt.com',
    url='https://public.commandprompt.com/projects/simpycity/wiki',
    download_url='https://projects.commandprompt.com/public/simpycity/repo/dist/Simpycity-0.3.1.tar.gz',
    install_requires=[
        "psycopg2>=2.0.8",
        "Exceptable>=0.1.0"
    ],
    packages=find_packages(),
    test_suite='nose.collector',
    license='LGPL',
#    packages=['simpycity','test'],
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

