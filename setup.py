from setuptools import setup, Extension
from Cython.Build import cythonize
modules=[]
modules.append(Extension ('databaseManager', sources=['databaseManager.py']))
modules.append(Extension ('util', sources=['util.py']))
modules.append(Extension ('main', sources=['main.py']))
setup(
	name='SQLiteFileList',
	version='0.1',
	author='Colin Behunin',
	ext_modules=cythonize(modules,compiler_directives={'language_level': 3})
	# ext_modules=[module]
)
