import sys

from setuptools import setup, Extension
from Cython.Build import cythonize

"""
ARGS:
	--noforce: do not recompile all modules
	--debug: sets gdb_debug=True
	--cythontrace: enables code coverage tracing
	--annotate: set annotate=True
	--annotatefull: set annotate-fullc=True
"""
# default values
force = True
debug = False
annotate = False
annotateFull = False
macros = []

# parse flags
if "--debug" in sys.argv:
	debug = True
	sys.argv.remove("--debug")
if "--noforce" in sys.argv:
	force = False
	sys.argv.remove("--noforce")
if "--annotate" in sys.argv:
	annotate = True
	sys.argv.remove("--annotate")
	# this is in prerelease, so this currently does nothing
	if "--annotatefull" in sys.argv:
		annotateFull = True
		sys.argv.remove("--annotate-full")
if "--cythontrace" in sys.argv:
	macros.append(("CYTHON_TRACE",1))
	print(macros)
	sys.argv.remove("--cythontrace")

modules = [Extension('databaseManager', sources=['databaseManager.pyx'], define_macros=macros),
					 Extension('Timer', sources=['Timer.pyx'], define_macros=macros),
					 Extension('util', sources=['util.pyx'], define_macros=macros)]

setup(
	name='SQLiteFileList',
	version='0.1',
	author='Colin Behunin',
	ext_modules=cythonize(modules,
												compiler_directives={'language_level': 3},
												gdb_debug=debug,
												force=force,
												annotate=annotate,
												)
	# ext_modules=[module]
)
# , "profile": True, "linetrace": True
