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
from pyspark.mllib.common import _java2py
from pyspark.sql import DataFrame

from pyspark3d import load_from_jvm
from pyspark3d import get_spark_context

def knn(df: DataFrame, p: list, k: int, coordSys: str, unique: bool):
    """ Finds the K nearest neighbors of the query object.

    The naive implementation here searches through all the the objects in
    the DataFrame to get the KNN. The nearness of the objects here
    is decided on the basis of the distance between their centers.

    Parameters
    ----------
    df : DataFrame
        Input Dataframe. Must have 3 columns corresponding to the
        coordinate (x, y, z) if cartesian or (r, theta, phi) f spherical.
    p : list of float
        Targeted point for which we want neighbors.
    k : int
        Number of neighbours
    coordSys : str
        Coordinate system: spherical or cartesian
    unique : bool
        Boolean. If true, returns only distinct objects. Default is false.

    Returns
    --------
    out : DataFrame
        DataFrame with the coordinates of the k neighbours found.

    Examples
    --------
    >>> df = spark.read.format("fits")\
        .option("hdu", 1)\
        .load("../src/test/resources/cartesian_points.fits")

    Get the 100 closest neighbours around the point [0.2, 0.2, 0.2]
    >>> K = 100
    >>> target = [0.2, 0.2, 0.2]
    >>> unique = False
    >>> neighbours = knn(df.select("x", "y", "z"), target, K, "spherical", unique)

    >>> print(neighbours.count())
    100

    You can add back the metadata
    >>> neighboursWithMeta = df.join(neighbours, ["x", "y", "z"], "left_semi")
    """
    prefix = "com.astrolabsoftware.spark3d"
    scalapath = "{}.Queries.KNN".format(prefix)
    scalaclass = load_from_jvm(scalapath)

    # # To convert python List to Scala Map
    convpath = "{}.python.PythonClassTag.javaListtoscalaList".format(prefix)
    conv = load_from_jvm(convpath)

    out = _java2py(get_spark_context() ,scalaclass(df._jdf, conv(p), k, coordSys, unique))

    return out

def windowquery(df: DataFrame, windowtype: str, windowcoord: int, coordSys: str):
    """ Perform window query, that is match between DF elements and
    a user-defined window (point, sphere, shell, box).

    If windowtype =
        - point: windowcoord = [x, y, z]
        - sphere: windowcoord = [x, y, z, R]
        - shell: windowcoord = [x, y, z, Rin, Rout]
        - box: windowcoord = [x1, y1, z1, x2, y2, z2, x3, y3, z3]
    Use [x, y, z] for cartesian or [r, theta, phi] for spherical.
    Note that box only accepts cartesian coordinates.

    Parameters
    ----------
    df : DataFrame
        Input Dataframe. Must have 3 columns corresponding to the
        coordinate (x, y, z) if cartesian or (r, theta, phi) f spherical.
    windowtype : str
        point, shell, sphere, or box.
    windowcoord : list of float
        List of Doubles. The coordinates of the window (see doc above).
    coordSys : str
        Coordinate system: spherical or cartesian

    Returns
    --------
    out : DataFrame
        DataFrame with the coordinates of the objects found in the window

    Examples
    --------
    >>> df = spark.read.format("csv")\
        .option("inferSchema", True)\
        .option("header", True)\
        .load("../src/test/resources/cartesian_spheres_manual.csv")

    Point-like window
    >>> windowtype = "point"
    >>> windowcoord = [1.0, 1.0, 1.0]
    >>> env = windowquery(df.select("x", "y", "z"), windowtype, windowcoord, "cartesian")

    >>> print(env.count())
    2

    You can add back the metadata
    >>> envWithMeta = df.join(env, ["x", "y", "z"], "left_semi")

    Sphere-like window
    >>> windowtype = "sphere"
    >>> windowcoord = [1.0, 1.0, 1.0, 2.0]
    >>> env = windowquery(df.select("x", "y", "z"), windowtype, windowcoord, "cartesian")

    >>> print(env.count())
    3

    Shell-like window
    >>> windowtype = "shell"
    >>> windowcoord = [1.0, 1.0, 1.0, 0.0, 2.0]
    >>> env = windowquery(df.select("x", "y", "z"), windowtype, windowcoord, "cartesian")

    >>> print(env.count())
    3

    Box-like window
    >>> windowtype = "box"
    >>> windowcoord = [2.0, 0.0, 0.0, 0.0, 2.0, 0.0, 0.0, 0.0, 2.0]
    >>> env = windowquery(df.select("x", "y", "z"), windowtype, windowcoord, "cartesian")

    >>> print(env.count())
    2
    """
    prefix = "com.astrolabsoftware.spark3d"
    scalapath = "{}.Queries.windowQuery".format(prefix)
    scalaclass = load_from_jvm(scalapath)

    # # To convert python List to Scala Map
    convpath = "{}.python.PythonClassTag.javaListtoscalaList".format(prefix)
    conv = load_from_jvm(convpath)

    out = _java2py(get_spark_context() ,scalaclass(df._jdf, windowtype, conv(windowcoord), coordSys))

    return out


if __name__ == "__main__":
    """
    Run the doctest using

    python queries.py

    If the tests are OK, the script should exit gracefuly, otherwise the
    failure(s) will be printed out.
    """
    import sys
    import doctest
    import numpy as np

    from pyspark import SparkContext
    from pyspark3d import pyspark3d_conf
    from pyspark3d import load_user_conf
    from pyspark3d import get_spark_session
    # Activate the SparkContext for the test suite
    dic = load_user_conf()
    conf = pyspark3d_conf("local", "test", dic)
    sc = SparkContext.getOrCreate(conf=conf)

    # Load the spark3D JAR+deps, and initialise the spark session.
    # In a pyspark shell, you do not need this.
    spark = get_spark_session(dicconf=dic)

    # Numpy introduced non-backward compatible change from v1.14.
    if np.__version__ >= "1.14.0":
        np.set_printoptions(legacy="1.13")

    # Run the test suite
    failure_count, test_count = doctest.testmod()
    sys.exit(failure_count)
