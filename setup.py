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
    url='https://projects.commandprompt.com/public/simpycity/repo/dist',
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
)

