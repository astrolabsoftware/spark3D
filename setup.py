#!/usr/bin/env python
# Copyright 2018 Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -*- coding: utf-8 -*-

import os
import subprocess

from setuptools import setup
from distutils.command.build import build
from distutils.command.clean import clean
from distutils.command.sdist import sdist
from distutils.spawn import find_executable

with open("README.md", "r") as fh:
    long_description = fh.read()

requirements = ["numpy>=1.14", "pyspark", "scipy"]
setup_requirements = ["wheel"]
test_requirements = ["coverage>=4.2", "coveralls"]

# Read the versions
exec(compile(
    open("pyspark3d/version.py").read(),
    "pyspark3d/version.py",
    "exec"))

VERSION = __version__
SCALA_VERSION = __scala_version__
SCALA_VERSION_ALL = __scala_version_all__

ASSEMBLY_JAR = \
    "target/scala-2.11/spark3D-assembly-{}.jar".format(
        __version__)
# Move the JAR inside the package
ASSEMBLY_JAR_S = \
    "pyspark3d/spark3D-assembly-{}.jar".format(
        __version__)


class jar_build(build):
    """ Class to handle spark3D JAR while installing pyspark3d """
    def run(self):
        """
        Override distutils.command.build.
        Compile the companion library and produce a FAT jar.
        """
        if find_executable('sbt') is None:
            raise EnvironmentError("""
            The executable "sbt" cannot be found.
            Please install the "sbt" tool to build the companion jar file.
            """)

        build.run(self)
        subprocess.check_call(
            "sbt 'set test in assembly := {{}}' ++{} assembly".format(
                SCALA_VERSION_ALL), shell=True)

        subprocess.check_call(
            "cp {} {}".format(
                ASSEMBLY_JAR, ASSEMBLY_JAR_S), shell=True)


class jar_clean(clean):
    """ Extends distutils.command.clean """
    def run(self):
        """
        Cleans the scala targets from the system.
        """
        clean.run(self)
        subprocess.check_call('sbt clean', shell=True)


class my_sdist(sdist):
    """ Extends distutils.command.sdist """
    def initialize_options(self, *args, **kwargs):
        """
        During installation, open the MANIFEST file
        and insert the path to the spark3D JAR required
        to run pyspark3d.
        """
        here = os.path.dirname(os.path.abspath(__file__))
        filename = os.path.join(here, "MANIFEST.in")
        with open(filename, 'w') as f:
            incld = "include {}\n"
            f.write(incld.format(ASSEMBLY_JAR_S))
        return super().initialize_options(*args, **kwargs)


setup(
    name='pyspark3d',
    version=VERSION,
    description="Spark extension for processing large-scale 3D data sets",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="AstroLab Software",
    author_email='peloton@lal.in2p3.fr',
    url='https://github.com/astrolabsoftware/spark3d',
    packages=['pyspark3d'],
    install_requires=requirements,
    license="Apache License, Version 2.0",
    zip_safe=False,
    keywords=['spark', 'spark3d', 'scala', 'python', 'py4j'],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    tests_require=test_requirements,
    cmdclass={
        'build': jar_build,
        'clean': jar_clean,
        'sdist': my_sdist,
    },
    include_package_data=True,
    setup_requires=setup_requirements
)
