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
from pathlib import Path
from pyspark3d.version import __version__, __scala_version__

# For local tests
path_to_conf = Path().cwd().as_uri()

# This is the place where paths and environment variables should be defined
# Note that this is only for the driver (loading JARS, conf, etc)

# spark3D version
version = __version__

# Scala version used to compile spark3D
scala_version = __scala_version__

# Verbosity for Spark
log_level = "WARN"

# External JARS to be added to both driver and executors
# Should contain the FAT JAR of spark3D.
where_is_my_jar = os.path.abspath(os.path.dirname(__file__))
extra_jars = [
    os.path.join(
        where_is_my_jar, "spark3D-assembly-{}.jar".format(version))
]

# External packages specified using their Maven coordinates
extra_packages = [
    "com.github.astrolabsoftware:spark-fits_2.11:0.6.0",
]
