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
import os
pwd = os.environ["PWD"]

# This is the place where paths and environment variables should be defined
# Note that this is only for the driver (loading JARS, conf, etc)

# spark3D version
version = "0.1.5"

# Scala version used to compile spark3D
scala_version = "2.11"

# External JARS to be added to both driver and executors
extra_jars = [
    os.path.join(pwd, "../target/scala-{}/spark3d_{}-{}.jar".format(
        scala_version, scala_version, version)),
    os.path.join(pwd, "../lib/jhealpix.jar")
]

# External packages specified using their Maven coordinates
extra_packages = [
    "com.github.astrolabsoftware:spark-fits_2.11:0.6.0",
]
