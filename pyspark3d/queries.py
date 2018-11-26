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

    NOTE: `unique` is bugged...

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
    >>> neighbours = knn(df.select("x", "y", "z"), target, K, "spherical")

    >>> print(neighbours.count())
    100
    """
    prefix = "com.astrolabsoftware.spark3d"
    scalapath = "{}.Queries.KNN".format(prefix)
    scalaclass = load_from_jvm(scalapath)

    # # To convert python List to Scala Map
    convpath = "{}.python.PythonClassTag.javaListtoscalaList".format(prefix)
    conv = load_from_jvm(convpath)

    out = _java2py(get_spark_context() ,scalaclass(df._jdf, conv(p), k, coordSys, unique))

    return out
