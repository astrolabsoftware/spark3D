#!/bin/bash
# Copyright 2018 AstroLab Software
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

# Clean and launch the test suite
# Must be launched using ./test_scala.sh <SCALA_BINARY_VERSION>

SCALA_BINARY_VERSION=$1
if [ -z $SCALA_BINARY_VERSION ]
then
    echo "You did not specify the scala version for the test!"
    echo "Syntax : ./test_scala.sh <SCALA_BINARY_VERSION>"
    echo "Example: ./test_scala.sh 2.11.8"
    echo " "
    SCALA_BINARY_VERSION=`python -c "from pyspark3d import version; print(version.__scala_version_all__)"`
    echo "Taking the default SCALA_BINARY_VERSION: $SCALA_BINARY_VERSION"
fi

sbt ++$SCALA_BINARY_VERSION clean
sbt ++$SCALA_BINARY_VERSION coverage test coverageReport
