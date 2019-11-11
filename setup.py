from setuptools import setup

# read the contents of your README file
from os import path

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='pystalkd',
    version='1.3.0',
    packages=['pystalkd'],
    url='https://github.com/menezes-/pystalkd',
    download_url='https://github.com/menezes-/pystalkd/archive/v1.3.0.zip',
    keywords=['beanstalkd', 'python3', 'bindings'],
    license='MIT',
    author='Gabriel',
    author_email='gabrielmenezesvi@gmail.com',
    description='Beanstalkd bindings for python3',
    long_description=long_description,
    long_description_content_type='text/markdown',
    extras_require={'yaml': ["PyYAML"]}
)
