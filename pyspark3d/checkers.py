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
from pyspark.sql import DataFrame
from pyspark.mllib.common import _java2py

from pyspark3d import load_from_jvm
from pyspark3d import get_spark_context

def checkLoadBalancing(df: DataFrame, kind: str="frac", numberOfElements: int=-1):
    """
    DataFrame containing the weight of each partition.
    You can choose between outputing the size (number of rows) of each partition
    or the fractional size (%) to the total number of rows.
    size of the dataset (in percent). This is useful to check whether the
    load is correctly balanced.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame
    kind : str
        print the load balancing in terms of fractional size (kind="frac")
        or number of rows per partition (kind="size"). Default is "frac".
    numberOfElements : int
        (optional). Total number of elements in the DataFrame.
        Only needed if you choose to output fractional sizes (kind="frac").
        If not provided (i.e. default value of -1) and kind="frac",
        it will be computed (count).

    Returns
    ----------
    dfout : DataFrame containing the weight of each partition.

    Examples
    ----------
    Load data
    >>> df = spark.read.format("fits")\
        .option("hdu", 1)\
        .load("../src/test/resources/astro_obs.fits")

    Fake repartitioning in 10 equal sized partitions
    >>> df = df.repartition(10)

    Compute the load balancing %
    >>> df_load = checkLoadBalancing(df, kind="frac")

    Note that this is a DataFrame, so you can use df.show()
    Here we will check that the total is indeed 100%
    >>> val = df_load.select("Load (%)").collect()
    >>> assert(int(sum([i[0] for i in val])) == 100)

    Same using number of rows instead of fractional contribution
    >>> df_load = checkLoadBalancing(df, kind="size")
    >>> val = df_load.select("Load (#Rows)").collect()

    >>> assert(int(sum([i[0] for i in val])) == df.count())
    """
    prefix = "com.astrolabsoftware.spark3d"
    scalapath = "{}.Checkers.checkLoadBalancing".format(prefix)
    scalaclass = load_from_jvm(scalapath)

    # To convert python dic to Scala Map
    convpath = "{}.python.PythonClassTag.javaHashMaptoscalaMap".format(prefix)
    conv = load_from_jvm(convpath)

    dfout = _java2py(
        get_spark_context(),
        scalaclass(df._jdf, kind, numberOfElements))

    return dfout


if __name__ == "__main__":
    """
    Run the doctest using

    python checkers.py

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
