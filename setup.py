from setuptools import setup, Extension

module = Extension ('sqliteDatabase', sources=['main.pyx','databaseManager.pyx','Timer.pyx','util.pyx'])
setup(
	name='sqlite',
	version='0.1',
	packages=['sqliteDatabase'],
	author='Colin Behunin',
	ext_modules=[module]
)
