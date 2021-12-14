from setuptools import setup, find_packages

name = "pm-utilities"
version = "0.0a"
author = "Patrick McNeely"
author_email = "pmcneely2@gmail.com"
license = "MIT"
text_descr = "README.md"

packages = find_packages(where="src")
package_dir = {"": "src"}

# entry_points = {"console_scripts": ["pgmtest = launch_server.py"]}

setup(
    name=name,
    version=version,
    packages=packages,
    package_dir=package_dir,
    # entry_points=entry_points,
    # scripts=scripts,
    author=author,
    license=license,
    long_description=open(text_descr).read(),
)
