from setuptools import setup, Extension
from Cython.Build import cythonize
module = Extension ('sqliteDatabase', sources=['main.pyx', 'databaseManager.pyx', 'util.pyx'])
setup(
	name='SQLiteFileList',
	version='0.1',
	author='Colin Behunin',
	ext_modules=cythonize([module],compiler_directives={'language_level': 3})
	# ext_modules=[module]
)
