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
from pyspark.sql import SparkSession

from py4j.java_gateway import JavaObject

from pyspark3d import load_from_jvm

import sys
import os
import doctest
import numpy as np

def windowQuery(rdd: JavaObject, envelope: JavaObject) -> JavaObject:
    """
    Binding around windowQuery (RangeQuery.scala). For full description,
    see `$spark3d/src/main/scala/com/spark3d/spatialOperator/RangeQuery.scala`

    Perform window query, that is match between RDD elements and
    a user-defined window (point, shell, box).

    Parameters
    ----------
    rdd : JavaObject (RDD[A<:Shape3D])
        RDD[A] coming from the Java side. `A` can be Point3D, ShellEnvelope,
        or BoxEnvelope.
    envelope : JavaObject (B<:Shape3D)
        Geometry object coming from the Java side. Can be Point3D,
        ShellEnvelope, or BoxEnvelope.

    Returns
    ----------
    match : JavaObject (RDD[A<:Shape3D])
        RDD[A] containing elements of `rdd` intersecting (within)
        the `envelope`.

    Examples
    ----------
    >>> from pyspark3d import get_spark_session
    >>> from pyspark3d import load_user_conf
    >>> from pyspark3d.geometryObjects import ShellEnvelope
    >>> from pyspark3d_conf import path_to_conf
    >>> from pyspark3d.spatial3DRDD import SphereRDD

    Load the user configuration, and initialise the spark session.
    >>> dic = load_user_conf()
    >>> spark = get_spark_session(dicconf=dic)

    Load the data
    >>> fn = os.path.join(path_to_conf,
    ...     "../src/test/resources/cartesian_spheres.fits")
    >>> rdd = SphereRDD(spark, fn, "x,y,z,radius",
    ...     False, "fits", {"hdu": "1"})

    Load the envelope (Sphere at the center, and radius 0.5)
    >>> sh = ShellEnvelope(0.0, 0.0, 0.0, False, 0.0, 0.5)

    Perform the query
    >>> match = windowQuery(rdd.rawRDD(), sh)
    >>> print("{}/{} objects found in the envelope".format(
    ...     len(match.collect()), rdd.rawRDD().count()))
    1435/20000 objects found in the envelope
    """
    spark3droot = "com.astrolabsoftware.spark3d."
    scalapath = spark3droot + "spatialOperator.RangeQuery"
    scalaclass = load_from_jvm(scalapath)

    classpath = spark3droot + "python.PythonClassTag.classTagFromObject"
    classtag = load_from_jvm(classpath)

    first_el = rdd.first()
    match = scalaclass.windowQuery(
        rdd,
        envelope,
        classtag(first_el),
        classtag(envelope))

    return match


if __name__ == "__main__":
    """
    Run the doctest using

    python spatialOperator.py

    If the tests are OK, the script should exit gracefuly, otherwise the
    failure(s) will be printed out.
    """
    # Numpy introduced non-backward compatible change from v1.14.
    if np.__version__ >= "1.14.0":
        np.set_printoptions(legacy="1.13")

    # Run the test suite
    failure_count, test_count = doctest.testmod()
    sys.exit(failure_count)
