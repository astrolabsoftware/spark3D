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
from pyspark import SparkContext
from pyspark3d import load_from_jvm
from pyspark3d_conf import pyspark3d_conf

import os
import doctest
import numpy as np

def Point3D(x: float, y: float, z: float, isSpherical: bool):
    """
    Class to handle Point3D

    Parameters
    ----------
    x : float
        Input X coordinate in Euclidean space, and R in spherical space.
    y : float
        Input Y coordinate in Euclidean space, and THETA in spherical space.
    z : float
        Input Z coordinate in Euclidean space, and PHI in spherical space.
    isSpherical : bool
        If true, it assumes that the coordinates of the Point3D
        are (r, theta, phi). Otherwise, it assumes cartesian
        coordinates (x, y, z).

    Example
    ----------
    >>> p3d = Point3D(1.0, np.pi, 0.0, True)
    >>> print(type(p3d))
    <class 'py4j.java_gateway.JavaObject'>

    >>> p3d.getVolume()
    0.0

    >>> p3d.toHealpix(2048, True)
    50331644
    """
    scala_p3d = "com.astrolabsoftware.spark3d.geometryObjects.Point3D"
    p3d = load_from_jvm(scala_p3d)

    return p3d(x, y, z, isSpherical)


if __name__ == "__main__":
    """
    Run the doctest using

    python __init__.py

    If the tests are OK, the script should exit gracefuly, otherwise the
    failure(s) will be printed out.
    """
    # Activate the SparkContext for the test suite
    pwd = os.environ["PWD"]
    spark3d_jar = os.path.join(
        pwd, "../../target/scala-2.11/spark3d_2.11-0.1.5.jar")
    healpix_jar = os.path.join(
        pwd, "../../lib/jhealpix.jar")
    sparkfits_maven = "com.github.astrolabsoftware:spark-fits_2.11:0.6.0"

    dic = {
        "spark.jars": spark3d_jar+","+healpix_jar,
        "spark.jars.packages": sparkfits_maven
        }

    conf = pyspark3d_conf("local", "test", dic)
    sc = SparkContext.getOrCreate(conf=conf)

    # Numpy introduced non-backward compatible change from v1.14.
    if np.__version__ >= "1.14.0":
        np.set_printoptions(legacy="1.13")

    # Run the test suite
    doctest.testmod()
