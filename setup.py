from setuptools import setup, Extension
from Cython.Build import cythonize

modules = []
modules.append(Extension('databaseManager', sources=['databaseManager.pyx']))
modules.append(Extension('Timer',sources=['Timer.pyx']))
modules.append(Extension('util', sources=['util.pyx']))

setup(
	name='SQLiteFileList',
	version='0.1',
	author='Colin Behunin',
	ext_modules=cythonize(modules, compiler_directives={'language_level': 3})
	# ext_modules=[module]
)
