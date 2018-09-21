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
from pyspark.sql import SparkSession

from py4j.java_gateway import JavaObject

from pyspark3d import load_from_jvm
from pyspark3d.pyspark3d_conf import path_to_conf

import os
from typing import Dict

def Point3DRDD(
        spark: SparkSession, filename: str, colnames: str,
        isSpherical: bool, format: str, options: Dict={"": ""}) -> JavaObject:
    """
    Binding around Point3DRDD.scala. For full description,
    see `$spark3d/src/main/scala/com/spark3d/spatial3DRDD/Point3DRDD.scala`

    Construct a Point3DRDD from a RDD[Point3D].
    The RDD[Point3D] is constructed from any available data source registered
    in Spark. For more information about available official connectors:
    `https://spark-packages.org/?q=tags%3A%22Data%20Sources%22`.
    That currently includes: CSV, JSON, TXT, FITS, ROOT, HDF5, Avro, Parquet...

    Note
    -----------
    The Scala version makes use of several constructors (i.e. with different
    kinds of argument). Here we only provide one way to instantiate a
    Point3DRDD Scala class, through the full list of arguments.

    Note that pyspark works with Python wrappers around the *Java* version
    of Spark objects, not around the *Scala* version of Spark objects.
    Therefore on the Scala side, we trigger the method
    `Point3DRDDFromV2PythonHelper` which is a modified version of
    `Point3DRDDFromV2`. The main change is that `options` on the Scala side
    is a java.util.HashMap in order to smoothly connect to `dictionary` in
    the Python side.

    Also for convenience (for the developper!), the StorageLevel is no more an
    argument in the constructor but is set to `StorageLevel.MEMORY_ONLY`.
    This is because I couldn't find how to pass that information from
    Python to Java... TBD!

    Parameters
    ----------
    spark : SparkSession
        The current Spark session.
    filename : str
        File name where the data is stored.
    colnames : str
        Comma-separated names of (x,y,z) columns. Example: "Z_COSMO,RA,Dec".
    isSpherical : bool
        If true, it assumes that the coordinates of the Point3D are
        (r, theta, phi). Otherwise, it assumes cartesian coordinates (x, y, z).
    format : str
        The name of the data source as registered in Spark. For example:
            - text
            - csv
            - json
            - com.astrolabsoftware.sparkfits or fits
            - org.dianahep.sparkroot
            - gov.llnl.spark.hdf or hdf5
    options : dic of (str: str), optional
        Options to pass to the DataFrameReader. Default is no options.

    Returns
    ----------
    p3drdd : Point3DRDD instance
        Instance of the Scala Point3DRDD class.

    Examples
    ----------
    Load data
    >>> fn = os.path.join(path_to_conf, "../src/test/resources/astro_obs.fits")
    >>> rdd = Point3DRDD(spark, fn, "Z_COSMO,RA,DEC",
    ...     True, "fits", {"hdu": "1"})

    Check we have a spatial3DRDD
    >>> assert("com.astrolabsoftware.spark3d.spatial3DRDD" in rdd.toString())

    Count the number of elements
    >>> print(rdd.rawRDD().count())
    20000

    Repartition the data of the RDD (ONION)
    >>> gridtype = "LINEARONIONGRID"
    >>> rdd_part = rdd.spatialPartitioningPython(gridtype,
    ...     rdd.rawRDD().getNumPartitions())

    Repartition the data of the RDD (OCTREE)
    >>> gridtype = "OCTREE"
    >>> rdd_part = rdd.spatialPartitioningPython(gridtype,
    ...     rdd.rawRDD().getNumPartitions())

    Get a RDD with the coordinates of the Point3D
    centers (e.g. useful for plot)
    >>> rdd_centers = rdd.toCenterCoordinateRDDPython(rdd.rawRDD())
    >>> print(round(list(rdd_centers.first())[0], 2))
    0.55

    To see all the available methods:
    >>> print(sorted(rdd.__dir__())) # doctest: +NORMALIZE_WHITESPACE
    ['$lessinit$greater$default$6', '$lessinit$greater$default$7', 'apply',
    'boundary', 'boundary_$eq',
    'com$astrolabsoftware$spark3d$spatial3DRDD$Shape3DRDD$$mapElements$1',
    'equals', 'getClass', 'getDataEnvelope', 'hashCode', 'isSpherical',
    'notify', 'notifyAll', 'partition', 'rawRDD', 'spatialPartitioning',
    'spatialPartitioning$default$2', 'spatialPartitioningPython',
    'spatialPartitioningPython$default$2', 'toCenterCoordinateRDD',
    'toCenterCoordinateRDDPython', 'toString', 'wait']

    """
    scalapath = "com.astrolabsoftware.spark3d.spatial3DRDD.Point3DRDD"
    scalaclass = load_from_jvm(scalapath)

    p3drdd = scalaclass(
        spark._jwrapped.sparkSession(), filename, colnames,
        isSpherical, format, options)

    return p3drdd

def SphereRDD(
        spark: SparkSession, filename: str, colnames: str,
        isSpherical: bool, format: str, options: Dict={"": ""}) -> JavaObject:
    """
    Binding around SphereRDD.scala. For full description,
    see `$spark3d/src/main/scala/com/spark3d/spatial3DRDD/SphereRDD.scala`

    Construct a SphereRDD from a RDD[ShellEnvelope].
    The RDD[Point3D] is constructed from any available data source registered
    in Spark. For more information about available official connectors:
    `https://spark-packages.org/?q=tags%3A%22Data%20Sources%22`.
    That currently includes: CSV, JSON, TXT, FITS, ROOT, HDF5, Avro, Parquet...

    Note
    -----------
    The Scala version makes use of several constructors (i.e. with different
    kinds of argument). Here we only provide one way to instantiate a
    SphereRDD Scala class, through the full list of arguments.

    Note that pyspark works with Python wrappers around the *Java* version
    of Spark objects, not around the *Scala* version of Spark objects.
    Therefore on the Scala side, we trigger the method
    `SphereRDDFromV2PythonHelper` which is a modified version of
    `SphereRDDFromV2`. The main change is that `options` on the Scala side
    is a java.util.HashMap in order to smoothly connect to `dictionary` in
    the Python side.

    Also for convenience (for the developper!), the StorageLevel is no more an
    argument in the constructor but is set to `StorageLevel.MEMORY_ONLY`.
    This is because I couldn't find how to pass that information from
    Python to Java... TBD!

    Parameters
    ----------
    spark : SparkSession
        The current Spark session.
    filename : str
        File name where the data is stored.
    colnames : str
        Comma-separated names of (x,y,z,radius) columns.
        Example: "Z_COSMO,RA,Dec,Radius".
    isSpherical : bool
        If true, it assumes that the coordinates of the Point3D are
        (r, theta, phi). Otherwise, it assumes cartesian coordinates (x, y, z).
    format : str
        The name of the data source as registered in Spark. For example:
            - text
            - csv
            - json
            - com.astrolabsoftware.sparkfits or fits
            - org.dianahep.sparkroot
            - gov.llnl.spark.hdf or hdf5
    options : dic of (str: str), optional
        Options to pass to the DataFrameReader. Default is no options.

    Returns
    ----------
    sphererdd : SphereRDD instance
        Instance of the Scala SphereRDD class.

    Examples
    ----------
    Load the data
    >>> fn = os.path.join(path_to_conf,
    ...     "../src/test/resources/cartesian_spheres.fits")
    >>> rdd = SphereRDD(spark, fn, "x,y,z,radius",
    ...     False, "fits", {"hdu": "1"})

    Check we have a SphereRDD
    >>> assert("spark3d.spatial3DRDD.SphereRDD" in rdd.toString())

    Count the number of elements
    >>> print(rdd.rawRDD().count())
    20000

    Repartition the data of the RDD (ONION)
    >>> gridtype = "LINEARONIONGRID"
    >>> rdd_part = rdd.spatialPartitioningPython(gridtype,
    ...     rdd.rawRDD().getNumPartitions())

    Repartition the data of the RDD (OCTREE)
    >>> gridtype = "OCTREE"
    >>> rdd_part = rdd.spatialPartitioningPython(gridtype,
    ...     rdd.rawRDD().getNumPartitions())

    Get a RDD with the coordinates of the Point3D
    centers (e.g. useful for plot)
    >>> rdd_centers = rdd.toCenterCoordinateRDDPython(rdd.rawRDD())
    >>> print(round(list(rdd_centers.first())[0], 2))
    0.77

    To see all the available methods:
    >>> print(sorted(rdd.__dir__())) # doctest: +NORMALIZE_WHITESPACE
    ['$lessinit$greater$default$6', '$lessinit$greater$default$7', 'apply',
    'boundary', 'boundary_$eq',
    'com$astrolabsoftware$spark3d$spatial3DRDD$Shape3DRDD$$mapElements$1',
    'equals', 'getClass', 'getDataEnvelope', 'hashCode', 'isSpherical',
    'notify', 'notifyAll', 'partition', 'rawRDD', 'spatialPartitioning',
    'spatialPartitioning$default$2', 'spatialPartitioningPython',
    'spatialPartitioningPython$default$2', 'toCenterCoordinateRDD',
    'toCenterCoordinateRDDPython', 'toString', 'wait']

    """
    scalapath = "com.astrolabsoftware.spark3d.spatial3DRDD.SphereRDD"
    scalaclass = load_from_jvm(scalapath)

    sphererdd = scalaclass(
        spark._jwrapped.sparkSession(), filename, colnames,
        isSpherical, format, options)

    return sphererdd


if __name__ == "__main__":
    """
    Run the doctest using

    python spatial3DRDD.py

    If the tests are OK, the script should exit gracefuly, otherwise the
    failure(s) will be printed out.
    """
    import sys
    import doctest
    import numpy as np

    # Numpy introduced non-backward compatible change from v1.14.
    if np.__version__ >= "1.14.0":
        np.set_printoptions(legacy="1.13")

    from pyspark3d import get_spark_session
    from pyspark3d import load_user_conf

    # Load the spark3D JAR+deps, and initialise the spark session.
    # In a pyspark shell, you do not need this.
    dic = load_user_conf()
    spark = get_spark_session(dicconf=dic)

    # Run the test suite
    failure_count, test_count = doctest.testmod(extraglobs={"spark": spark})
    sys.exit(failure_count)
