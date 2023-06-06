from setuptools import find_packages, setup

setup(
    name="asynciomultiplexer",
    version="1.1.0",
    author='John Rusnak',
    author_email='jrusnak69@gmail.com',
    # declare your packages
    packages=['asynciomultiplexer'],
    package_dir={"": "src"},
    # include data files
    # data_files=data_files,
    # entry_points=None,
)
