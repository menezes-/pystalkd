from distutils.core import setup
import os
long_description = 'See https://github.com/menezes-/pystalkd'
if os.path.exists('README.rst'):
    long_description = open('README.rst').read()

setup(
    name='pystalkd',
    version='1.1',
    packages=['pystalkd'],
    url='https://github.com/menezes-/pystalkd',
    download_url='https://github.com/menezes-/pystalkd/archive/master.zip',
    keywords=['beanstalkd', 'python3', 'bindings'],
    license='Apache-2.0',
    author='Gabriel',
    author_email='gabrielmenezesvi@gmail.com',
    description='Beanstalkd bindings for python3',
    long_description=long_description
)
