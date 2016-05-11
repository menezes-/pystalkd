from setuptools import setup
import os
long_description = 'See https://github.com/menezes-/pystalkd'

# used to create the description to the pypi page
if os.path.exists('README.md'):
    # pandoc --from=markdown --to=rst --output=README.rst README.md
    import subprocess

    try:
        process = subprocess.Popen(['pandoc', '--from=markdown', "--to=rst", "README.md"], stdout=subprocess.PIPE)
        out, err = process.communicate()
        long_description = str(out, 'utf8')
    except FileNotFoundError:
        # use default description
        pass

setup(
    name='pystalkd',
    version='1.2.3',
    packages=['pystalkd'],
    url='https://github.com/menezes-/pystalkd',
    download_url='https://github.com/menezes-/pystalkd/archive/v1.2.3.zip',
    keywords=['beanstalkd', 'python3', 'bindings'],
    license='Apache-2.0',
    author='Gabriel',
    author_email='gabrielmenezesvi@gmail.com',
    description='Beanstalkd bindings for python3',
    long_description=long_description,
    extras_require={'yaml': ["PyYAML"]}
)
