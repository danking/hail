from setuptools import setup, find_packages

setup(
    name = 'dbuf',
    version = '0.0.1',
    url = 'https://github.com/hail-is/hail.git',
    author = 'Hail Team',
    author_email = 'hail@broadinstitute.org',
    description = 'distributed buffer',
    packages = find_packages(),
    include_package_data=True,
    install_requires=[
        'aiohttp>=3.6,<3.7'
    ]
)
