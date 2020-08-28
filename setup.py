import setuptools
import os.path

def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as fp:
        return fp.read()

def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(name='safegraph_py_functions',
	version=get_version("safegraph_py_functions/__init__.py"),
    description='SafeGraph Python Library',
    author='lynzt',
    url='https://github.com/SafeGraphInc/safegraph_py',
    packages = ['safegraph_py_functions'],
    python_requires='>=3.6',
      )

