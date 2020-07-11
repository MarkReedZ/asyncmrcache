
from setuptools import setup, Extension, find_packages

m1 = Extension(
    'asyncmrcache.internals',
     sources = [
      './src/asyncmrcache/internals/city.c',
      #'./src/asyncmrcache/internals/plist.c',
      './src/asyncmrcache/internals/module.c',
      './src/asyncmrcache/internals/mrserver.c',
      './src/asyncmrcache/internals/mrclient.c',
      './src/asyncmrcache/internals/mrprotocol.c',
     ],
     include_dirs = ['./src/asyncmrcache/internals'],
     extra_compile_args = ['-msse4.2', '-mavx2', '-mbmi2', '-Wunused-variable','-std=c99','-Wno-discarded-qualifiers', '-Wno-unused-variable','-Wno-unused-function'],
     extra_link_args = [],
     define_macros = [('DEBUG_PRINT',1)]
)

setup(
    name='asyncmrcache',
    version="0.2",
    description='Python asyncio mrmrcache client',
    long_description='Asyncio based Python client for mrmrcache',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        ],
    url='https://github.com/MarkReedZ/asyncmrcache',
    author='Mark Reed',
    author_email='mark@untilfluent.com',
    license='MIT License',
    ext_modules = [m1],
    package_dir={'':'src'},
    packages=find_packages('src'),# + ['prof'],
    zip_safe=True,
)
