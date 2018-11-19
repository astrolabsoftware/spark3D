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
from pyspark.sql import DataFrame
from pyspark.mllib.common import _java2py

from pyspark3d import load_from_jvm
from pyspark3d import get_spark_context

from typing import Dict

def addSPartitioning(df: DataFrame, options: Dict={"": ""}, numPartitions: int=-1):
    """Add a DataFrame column describing the partitioning.

    This method allows to use a custom partitioner (SpatialPartitioner).
    Note that no data movement (shuffle) is performed yet here, as we just
    describe how the repartitioning should be done.
    Use `partitionBy` to trigger it.

    `options` must contain four entries:
       - gridtype: the type of repartitioning. Available: current (no repartitioning), onion, octree.
       - geometry: geometry of objects: points, spheres, or boxes
       - coordSys: coordinate system: spherical or cartesian
       - colnames: comma-separated names of the spatial coordinates. For points,
            must be "x,y,z" or "r,theta,phi". For spheres, must be "x,y,z,R" or
            "r,theta,phi,R".

    Parameters
    ----------
    df : DataFrame
        Input DataFrame
    options : Dictionary of Strings
        Dictionary containing metadata (see above).
    numPartitions : int
        (optional) The number of partitions wanted. -1 by default,
        i.e. the number of partitions of the input DataFrame.

    Returns
    ----------
    dfout : DataFrame
        Input DataFrame plus an additional column `partition_id`.

    Examples
    ----------
    Load data
    >>> df = spark.read.format("fits")\
        .option("hdu", 1)\
        .load("../src/test/resources/astro_obs.fits")

    Specify options
    >>> options = {
    ...     "geometry": "points",
    ...     "colnames": "Z_COSMO,RA,DEC",
    ...     "coordSys": "spherical",
    ...     "gridtype": "onion"}

    Add a column containing the partitioning (Onion)
    >>> df_colid = addSPartitioning(df, options, 10)
    >>> print(df_colid.select("partition_id").distinct().count())
    10

    Note that you can also return the current partitioning:
    >>> options = {
    ...     "geometry": "points",
    ...     "colnames": "Z_COSMO,RA,DEC",
    ...     "coordSys": "spherical",
    ...     "gridtype": "current"}
    >>> df_colid = addSPartitioning(df, options)
    >>> assert(df_colid.select("partition_id").distinct().count() == df.rdd.getNumPartitions())
    """
    prefix = "com.astrolabsoftware.spark3d"
    scalapath = "{}.Repartitioning.addSPartitioning".format(prefix)
    scalaclass = load_from_jvm(scalapath)

    # To convert python dic to Scala Map
    convpath = "{}.python.PythonClassTag.javaHashMaptoscalaMap".format(prefix)
    conv = load_from_jvm(convpath)

    dfout = _java2py(
        get_spark_context(),
        scalaclass(df._jdf, conv(options), numPartitions))

    return dfout

def repartitionByCol(df: DataFrame, colname: str, numPartitions: int=-1):
    """Repartition a DataFrame according to a column containing partition ID.

    Note this is not re-ordering elements, but making new partitions with
    objects having the same partition ID defined by one of the
    DataFrame column (i.e. shuffling).

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    colname : str
        Column name describing the repartitioning. Typically Ints.
    numPartitions : int
        (optional )Number of partitions. If not provided the code will
        guess the number of partitions by counting the number of distinct
        elements of the repartitioning column.
        As it can be costly, you can provide manually this information.

    Returns
    ---------
    dfout : DataFrame
        Repartitioned input DataFrame.

    Examples
    ---------
    Load data
    >>> df = spark.read.format("fits")\
        .option("hdu", 1)\
        .load("../src/test/resources/astro_obs.fits")

    Specify options
    >>> options = {
    ...     "geometry": "points",
    ...     "colnames": "Z_COSMO,RA,DEC",
    ...     "coordSys": "spherical",
    ...     "gridtype": "onion"}

    Add a column containing the partitioning (Onion)
    >>> df_colid = addSPartitioning(df, options, 10)
    >>> print(df_colid.select("partition_id").distinct().count())
    10

    Trigger the repartitioning
    >>> df_repart = repartitionByCol(df_colid, "partition_id", 10)
    >>> def mapLen(part): yield len([*part])
    >>> df_repart.rdd.mapPartitions(mapLen).take(1)[0]
    2104
    """
    prefix = "com.astrolabsoftware.spark3d"
    scalapath = "{}.Repartitioning.repartitionByCol".format(prefix)
    scalaclass = load_from_jvm(scalapath)

    dfout = _java2py(
        get_spark_context(),
        scalaclass(df._jdf, colname, numPartitions))

    return dfout


if __name__ == "__main__":
    """
    Run the doctest using

    python repartitioning.py

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
