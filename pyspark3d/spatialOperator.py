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

from py4j.java_gateway import JavaObject

from pyspark3d import load_from_jvm
from pyspark3d.converters import scala2python

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
    >>> from pyspark3d.geometryObjects import ShellEnvelope
    >>> from pyspark3d_conf import path_to_conf
    >>> from pyspark3d.spatial3DRDD import SphereRDD

    Load the data
    >>> fn = os.path.join(path_to_conf,
    ...     "../src/test/resources/cartesian_spheres.fits")
    >>> rdd = SphereRDD(spark, fn, "x,y,z,radius",
    ...     False, "fits", {"hdu": "1"})

    Load the envelope (Sphere at the center, and radius 0.5)
    >>> sh = ShellEnvelope(0.0, 0.0, 0.0, False, 0.0, 0.5)

    Perform the query
    >>> matchRDD = windowQuery(rdd.rawRDD(), sh)
    >>> print("{}/{} objects found in the envelope".format(
    ...     len(matchRDD.collect()), rdd.rawRDD().count()))
    1435/20000 objects found in the envelope
    """
    spark3droot = "com.astrolabsoftware.spark3d."
    scalapath = spark3droot + "spatialOperator.RangeQuery"
    scalaclass = load_from_jvm(scalapath)

    classpath = spark3droot + "python.PythonClassTag.classTagFromObject"
    classtag = load_from_jvm(classpath)

    first_el = rdd.first()
    matchRDD = scalaclass.windowQuery(
        rdd,
        envelope,
        classtag(first_el),
        classtag(envelope))

    return matchRDD

def KNN(
        rdd: JavaObject, queryObject: JavaObject,
        k: int, unique: bool = False) -> list:
    """
    Binding around KNN (SpatialQuery.scala). For full description, see
    `$spark3d/src/main/scala/com/spark3d/spatialOperator/SpatialQuery.scala`

    Finds the K nearest neighbours of the query object within `rdd`.
    The naive implementation here searches through all the the objects in the
    RDD to get the KNN. The nearness of the objects here is decided on the
    basis of the distance between their centers.
    Note that `queryObject` and elements of `rdd` must have the same type
    (either both Point3D, or both ShellEnvelope, or both BoxEnvelope).

    Parameters
    ----------
    rdd : JavaObject (RDD[A<:Shape3D])
        RDD[A] coming from the Java side. `A` can be Point3D, ShellEnvelope,
        or BoxEnvelope.
    envelope : JavaObject (A<:Shape3D)
        Object to which the KNN are to be found. Can be Point3D,
        ShellEnvelope, or BoxEnvelope, but should have the same type as the
        elements of `rdd`.
    k : int
        Number of nearest neighbors requested
    unique : bool, optional
        If True, returns only distinct objects. Default is False.

    Returns
    ----------
    match : List[JavaObject] (List[A<:Shape3D])
        List of the KNN (of type A<:Shape3D).

    Examples
    ----------
    >>> from pyspark3d.geometryObjects import Point3D
    >>> from pyspark3d_conf import path_to_conf
    >>> from pyspark3d.spatial3DRDD import Point3DRDD

    Load the data (spherical coordinates)
    >>> fn = os.path.join(path_to_conf, "../src/test/resources/astro_obs.fits")
    >>> rdd = Point3DRDD(spark, fn, "Z_COSMO,RA,DEC",
    ...     True, "fits", {"hdu": "1"})

    Define your query point
    >>> pt = Point3D(0.0, 0.0, 0.0, True)

    Perform the query: look for the 5 closest neighbours from `pt`.
    >>> match = KNN(rdd.rawRDD(), pt, 5, True)

    `match` is a list of Point3D. Take the coordinates and convert them
    into cartesian coordinates:
    >>> mod = "com.astrolabsoftware.spark3d.utils.Utils.sphericalToCartesian"
    >>> converter = load_from_jvm(mod)
    >>> match_coord = [converter(m).getCoordinatePython() for m in match]

    Print the distance to the query point (here 0, 0, 0)
    >>> def normit(l: list) -> float: return np.sqrt(sum([e**2 for e in l]))
    >>> print([int(normit(l)*1e6) for l in match_coord])
    [72, 73, 150, 166, 206]
    """
    spark3droot = "com.astrolabsoftware.spark3d."
    scalapath = spark3droot + "spatialOperator.SpatialQuery"
    scalaclass = load_from_jvm(scalapath)

    classpath = spark3droot + "python.PythonClassTag.classTagFromObject"
    classtag = load_from_jvm(classpath)

    match = scalaclass.KNN(rdd, queryObject, k, unique, classtag(queryObject))

    return scala2python(match)

def KNNEfficient(rdd: JavaObject, queryObject: JavaObject, k: int) -> list:
    """
    Binding around KNNEfficient (SpatialQuery.scala). For full description, see
    `$spark3d/src/main/scala/com/spark3d/spatialOperator/SpatialQuery.scala`

    More efficient implementation of the KNN query above.
    First we seek the partitions in which the query object belongs and we
    will look for the knn only in those partitions. After this if the limit k
    is not satisfied, we keep looking similarly in the neighbors of the
    containing partitions.

    Note 1: elements of `rdd` and `queryObject` can have different types
        among Shape3D (Point3D or ShellEnvelope or BoxEnvelope)

    Note 2: KNNEfficient only works on repartitioned RDD (python version).
        See example below.

    Parameters
    ----------
    rdd : JavaObject (RDD[A<:Shape3D])
        Repartitioned RDD[A] coming from the Java side.
        `A` can be Point3D, ShellEnvelope, or BoxEnvelope.
    envelope : JavaObject (B<:Shape3D)
        Object to which the KNN are to be found. `B` can be Point3D,
        ShellEnvelope, or BoxEnvelope, not necessarily the same type as the
        elements of `rdd`.
    k : int
        Number of nearest neighbors requested

    Returns
    ----------
    match : List[JavaObject] (List[A<:Shape3D])
        List of the KNN (of type A<:Shape3D).

    Examples
    ----------
    >>> from pyspark3d.geometryObjects import Point3D
    >>> from pyspark3d_conf import path_to_conf
    >>> from pyspark3d.spatial3DRDD import Point3DRDD

    Load the raw data (spherical coordinates)
    >>> fn = os.path.join(path_to_conf, "../src/test/resources/astro_obs.fits")
    >>> rdd = Point3DRDD(spark, fn, "Z_COSMO,RA,DEC",
    ...     True, "fits", {"hdu": "1"})

    Repartition the RDD
    >>> rdd_part = rdd.spatialPartitioningPython("LINEARONIONGRID", 5)

    Note that using rdd.spatialPartitioning("LINEARONIONGRID", 5, <classTag>)
    would work as well.

    Define your query point
    >>> pt = Point3D(0.0, 0.0, 0.0, True)

    Perform the query: look for the 5 closest neighbours from `pt`.
    >>> match = KNNEfficient(rdd_part, pt, 5)

    `match` is a list of Point3D. Take the coordinates and convert them
    into cartesian coordinates:
    >>> mod = "com.astrolabsoftware.spark3d.utils.Utils.sphericalToCartesian"
    >>> converter = load_from_jvm(mod)
    >>> match_coord = [converter(m).getCoordinatePython() for m in match]

    Print the distance to the query point (here 0, 0, 0)
    >>> def normit(l: list) -> float: return np.sqrt(sum([e**2 for e in l]))
    >>> print([int(normit(l)*1e6) for l in match_coord])
    [72, 73, 150, 166, 206]
    """
    spark3droot = "com.astrolabsoftware.spark3d."
    scalapath = spark3droot + "spatialOperator.SpatialQuery"
    scalaclass = load_from_jvm(scalapath)

    classpath = spark3droot + "python.PythonClassTag.classTagFromObject"
    classtag = load_from_jvm(classpath)

    el = rdd.first()
    match = scalaclass.KNNEfficient(
        rdd, queryObject, k,
        classtag(el), classtag(queryObject))

    return scala2python(match)

def CrossMatchCenter(
        rddA: JavaObject, rddB: JavaObject,
        epsilon: float, returnType: str) -> JavaObject:
    """
    Binding around CrossMatchCenter (CenterCrossMatch.scala).
    For full description, see
    `$spark3d/src/main/scala/com/spark3d/spatialOperator/CenterCrossMatch.scala`

    Cross match 2 RDD based on the object centers.
    *The two input RDD must have been partitioned the same way!*

    You have to choice to return:
      (1) Elements of (A, B) matching (returnType="AB")
      (2) Elements of A matching B (returnType="A")
      (3) Elements of B matching A (returnType="B")

    Which one you should choose? That depends on what you need:
    (1) gives you all elements but is slow.
    (2) & (3) give you all elements only in one side but is faster.

    Parameters
    ----------
    rddA : JavaObject (RDD[A<:Shape3D])
        RDD whose elements are Shape3D or any extension (Point3D, ...)
    rddB : JavaObject (RDD[B<:Shape3D])
        RDD whose elements are Shape3D or any extension (Point3D, ...)
    epsilon : float
        Tolerance for the distance between 2 centers. Should have the same
        units as the center coordinates.
    returnType : str
        Kind of crossmatch to perform:
            - Elements of (A, B) matching (returnType="AB")
            - Elements of A matching B (returnType="A")
            - Elements of B matching A (returnType="B")

    Returns
    ----------
    rdd : JavaObject
        RDD whose elements depends on `returnType`:
            - Elements of (A, B) matching (returnType="AB")
            - Elements of A matching B (returnType="A")
            - Elements of B matching A (returnType="B")

    Examples
    ----------
    >>> from pyspark3d_conf import path_to_conf
    >>> from pyspark3d.spatial3DRDD import Point3DRDD

    Load the raw data (spherical coordinates)
    >>> fn = os.path.join(path_to_conf,
    ...     "../src/test/resources/astro_obs_{}_light.fits")
    >>> rddA = Point3DRDD(spark, fn.format("A"), "Z_COSMO,RA,DEC",
    ...     True, "fits", {"hdu": "1"})
    >>> rddB = Point3DRDD(spark, fn.format("B"), "Z_COSMO,RA,DEC",
    ...     True, "fits", {"hdu": "1"})

    Apply the same partitioning to both RDD
    >>> rddA_part = rddA.spatialPartitioningPython("LINEARONIONGRID", 100)
    >>> rddB_part = rddB.spatialPartitioningPython(
    ...     rddA_part.partitioner().get())

    Cross match A and B centers and return elements of A matching with B
    >>> matchRDDB = CrossMatchCenter(rddA_part, rddB_part, 0.004, "A")
    >>> print("{}/{} elements in A match with elements of B!".format(
    ...     matchRDDB.count(), rddB_part.count()))
    19/1000 elements in A match with elements of B!
    """
    spark3droot = "com.astrolabsoftware.spark3d."
    scalapath = spark3droot + "spatialOperator.CenterCrossMatch"
    scalaclass = load_from_jvm(scalapath)

    classpath = spark3droot + "python.PythonClassTag.classTagFromObject"
    classtag = load_from_jvm(classpath)

    elA = rddA.first()
    elB = rddB.first()
    matchRDD = scalaclass.CrossMatchCenter(
        rddA,
        rddB,
        epsilon,
        returnType,
        classtag(elA),
        classtag(elB))

    return matchRDD

def CrossMatchHealpixIndex(
        rddA: JavaObject, rddB: JavaObject,
        nside: int, returnType: str) -> JavaObject:
    """
    Binding around CrossMatchHealpixIndex (PixelCrossMatch.scala).
    For full description, see
    `$spark3d/src/main/scala/com/spark3d/spatialOperator/PixelCrossMatch.scala`

    Cross match 2 RDD based on the healpix index of geometry center.
    The cross-match is done partition-by-partition, which means the two
    RDD must have been partitioned by the same partitioner.

    You have to choice to return:
      (1) Elements of (A, B) matching (returnType="AB")
      (2) Elements of A matching B (returnType="A")
      (3) Elements of B matching A (returnType="B")
      (4) Healpix pixel indices matching (returnType="healpix")

    Which one you should choose? That depends on what you need:
    (1) gives you all elements but is slow.
    (2) & (3) give you all elements only in one side but is faster.
    (4) gives you only healpix center but is even faster.

    Parameters
    ----------
    rddA : JavaObject (RDD[A<:Shape3D])
        RDD whose elements are Shape3D or any extension (Point3D, ...)
    rddB : JavaObject (RDD[B<:Shape3D])
        RDD whose elements are Shape3D or any extension (Point3D, ...)
    nside : int
        Resolution of the underlying healpix map used to convert angle
        coordinates to healpix index.
    returnType : str
        Kind of crossmatch to perform:
            - Elements of (A, B) matching (returnType="AB")
            - Elements of A matching B (returnType="A")
            - Elements of B matching A (returnType="B")

    Returns
    ----------
    rdd : JavaObject
        RDD whose elements depends on `returnType`:
            - Elements of (A, B) matching (returnType="AB")
            - Elements of A matching B (returnType="A")
            - Elements of B matching A (returnType="B")
            - Healpix pixel indices matching (returnType="healpix")

    Examples
    ----------
    >>> from pyspark3d_conf import path_to_conf
    >>> from pyspark3d.spatial3DRDD import Point3DRDD

    Load the raw data (spherical coordinates)
    >>> fn = os.path.join(path_to_conf,
    ...     "../src/test/resources/astro_obs_{}_light.fits")
    >>> rddA = Point3DRDD(spark, fn.format("A"), "Z_COSMO,RA,DEC",
    ...     True, "fits", {"hdu": "1"})
    >>> rddB = Point3DRDD(spark, fn.format("B"), "Z_COSMO,RA,DEC",
    ...     True, "fits", {"hdu": "1"})

    Apply the same partitioning to both RDD
    >>> rddA_part = rddA.spatialPartitioningPython("LINEARONIONGRID", 100)
    >>> rddB_part = rddB.spatialPartitioningPython(
    ...     rddA_part.partitioner().get())

    Cross match A and B centers and return elements of A matching with B
    >>> matchRDDB = CrossMatchHealpixIndex(rddA_part, rddB_part, 512, "A")
    >>> print("{}/{} elements in A match with elements of B!".format(
    ...     matchRDDB.count(), rddB_part.count()))
    15/1000 elements in A match with elements of B!
    """
    spark3droot = "com.astrolabsoftware.spark3d."
    scalapath = spark3droot + "spatialOperator.PixelCrossMatch"
    scalaclass = load_from_jvm(scalapath)

    classpath = spark3droot + "python.PythonClassTag.classTagFromObject"
    classtag = load_from_jvm(classpath)

    elA = rddA.first()
    elB = rddB.first()
    matchRDD = scalaclass.CrossMatchHealpixIndex(
        rddA,
        rddB,
        nside,
        returnType,
        classtag(elA),
        classtag(elB))

    return matchRDD


if __name__ == "__main__":
    """
    Run the doctest using

    python spatialOperator.py

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
