try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='Simpycity',
    version='0.1.4a5',
    description='A simple functionally-oriented PostgreSQL DB access pattern.',
    author='Aurynn Shaw, Commandprompt, Inc.',
    author_email='ashaw@commandprompt.com',
    url='https://projects.commandprompt.com/public/simpycity',
    install_requires=[
        "psycopg2>=2.0.8",
    ],
    license='LGPL',
    packages=['simpycity','test'],
    include_package_data=True,
    zip_safe=True,
)