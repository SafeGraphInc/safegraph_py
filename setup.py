import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(name='safegraph_py_functions',
	version='1.1.0',
    description='SafeGraph Python Library',
    author='lynzt',
    url='https://github.com/SafeGraphInc/safegraph_py',
    packages = ['safegraph_py_functions'],
    python_requires='>=3.6',
      )
