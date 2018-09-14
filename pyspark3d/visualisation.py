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

from typing import Iterator, Any, Callable, Generator

from py4j.java_gateway import JavaObject

from pyspark3d import load_from_jvm
from pyspark3d.converters import scala2python

from pyspark.mllib.common import _java2py
from pyspark import RDD

class CollapseFunctions():
    """ Set of functions to create a representation of the data.
    The idea is to reduce the size of the dataset while keeping desired
    features (e.g. clustering).

    We give as an example two simple examples dealing with the clustering:
    - mean (1-means)
    - k-means

    The reduction is done at the level of the Spark partition (Iterator).

    """

    def mean(self, partition: Iterator) -> Generator:
        """Compute the centroid of the partition.
        It can be viewed as a k-means for k=1.

        Parameters
        ----------
        partition : Iterator
            Iterator over the elements of the Spark partition.

        Returns
        -------
        Generator of (list, int)
            Yield tuple with the centroid and the total number of points
            in the partition. If the partition is empty, return (None, 0).

        Examples
        -------
        List of coordinates (can be 2D, 3D, ..., nD)
        >>> mylist = [[1., 2.], [3., 4.], [5., 6.], [7., 8.], [9., 10.]]

        Considering only 1 partition
        >>> cf = CollapseFunctions()
        >>> myit = iter(mylist)
        >>> list(cf.mean(myit))
        [(array([ 5.,  6.]), 5)]

        Distribute over 2 partitions
        >>> rdd = sc.parallelize(mylist, 2)

        Compute the centroid for each partition
        >>> data = rdd.mapPartitions(
        ...     lambda partition: cf.mean(partition)).collect()
        >>> print(data)
        [(array([ 2.,  3.]), 2), (array([ 7.,  8.]), 3)]

        If there are empty partitions, it returns None for the empty ones.
        >>> rdd = sc.parallelize(mylist, 6)
        >>> cf = CollapseFunctions()
        >>> data = rdd.mapPartitions(
        ...     lambda partition: cf.mean(partition)).collect()
        >>> centroids = [c[0] for c in data]
        >>> print(centroids) # doctest: +NORMALIZE_WHITESPACE
        [None, array([ 1.,  2.]), array([ 3.,  4.]), array([ 5.,  6.]),
        array([ 7.,  8.]), array([  9.,  10.])]

        """
        import numpy as np

        # Unwrap the iterator
        xyz = [*partition]
        size = len(xyz)

        # Compute the centroid only if the partition is not empty
        if size > 0:
            mean = np.mean(xyz, axis=0)
        else:
            mean = None

        yield (mean, size)

    def kmeans(self, partition: Iterator, k: int=1) -> Generator:
        """Performs k-means on a the elements of the partition.
        The case k=1 corresponds to the function `mean` above.

        Parameters
        ----------
        partition : Iterator
            Iterator over the elements of the Spark partition.
        k : int, optional
            The number of centroids to generate. Default is 1 (mean).

        Returns
        -------
        Generator of (list, int)
            The list of centroids, and the total number of points in the
            partition. If the partition is empty, return ([None], 0).

        Examples
        -------
        List of coordinates (can be 2D, 3D, ..., nD)
        >>> mylist = [
        ...     [1., 2., 4.], [3., 4., 1.], [5., 6., 5.],
        ...     [9., 10., 7.], [1., 2., 7.], [3., 4., 6.],
        ...     [5., 6., 9.], [7., 8., 6.], [9., 10., 10.]]

        Considering only 1 partition
        >>> cf = CollapseFunctions()
        >>> myit = iter(mylist[0: 2])
        >>> list(cf.kmeans(myit, 1))
        [(array([[ 2. ,  3. ,  2.5]]), 2)]

        Distribute over 2 partitions
        >>> rdd = sc.parallelize(mylist, 2)

        Compute the centroid for each partition
        >>> data = rdd.mapPartitions(
        ...     lambda partition: cf.kmeans(partition, 1)).collect()
        >>> print(data) # doctest: +NORMALIZE_WHITESPACE
        [(array([[ 4.5 ,  5.5 ,  4.25]]), 4), (array([[ 5. ,  6. ,  7.6]]), 5)]

        If there are empty partitions, it returns None for the empty ones.
        >>> rdd = sc.parallelize(mylist, 10)
        >>> cf = CollapseFunctions()
        >>> data = rdd.mapPartitions(
        ...     lambda partition: cf.kmeans(partition, 1)).collect()
        >>> centroids = [c[0] for c in data]
        >>> print(centroids) # doctest: +NORMALIZE_WHITESPACE
        [[None], array([[ 1.,  2.,  4.]]), array([[ 3.,  4.,  1.]]),
        array([[ 5.,  6.,  5.]]), array([[  9.,  10.,   7.]]),
        array([[ 1.,  2.,  7.]]), array([[ 3.,  4.,  6.]]),
        array([[ 5.,  6.,  9.]]), array([[ 7.,  8.,  6.]]),
        array([[  9.,  10.,  10.]])]

        """
        from scipy.cluster.vq import kmeans

        # Unwrap the iterator
        xyz = [*partition]
        size = len(xyz)

        if len(xyz) > k - 1:
            centers, distortion = kmeans(xyz, k)
        else:
            centers = [None]

        yield (centers, size)


def collapse_rdd_data(
        rdd: RDD, collapse_function: Callable[[Iterator, Any], Generator],
        *args: Any) -> RDD:
    """Apply a collapse function to reduce the size of the data
    set. The function is applied for each partition (mapPartitions).

    Parameters
    ----------
    rdd : RDD[list[float]]
        RDD of list of float. Each list is the coordinate of a point (x, y, z).
    collapse_function : function
        collapse function to reduce the size of the data set. See
        `CollapseFunctions` for more information.
    args: Any
        Any arguments that have to be passed to `collapse_function`.
        Must be comma-separated.

    Returns
    -------
    RDD
        RDD whose elements are the result of the collapse function for
        each partition.

    Examples
    -------
    List of coordinates (can be 2D, 3D, ..., nD)
    >>> mylist = [
    ...     [1., 2., 4.], [3., 4., 1.], [5., 6., 5.],
    ...     [9., 10., 7.], [1., 2., 7.], [3., 4., 6.],
    ...     [5., 6., 9.], [7., 8., 6.], [9., 10., 10.]]

    Distribute over 2 partitions
    >>> rdd = sc.parallelize(mylist, 2)

    Compute the centroid for each partition
    >>> cf = CollapseFunctions()
    >>> data = collapse_rdd_data(rdd, cf.kmeans, 1).collect()
    >>> print(data) # doctest: +NORMALIZE_WHITESPACE
    [(array([[ 4.5 ,  5.5 ,  4.25]]), 4), (array([[ 5. ,  6. ,  7.6]]), 5)]

    """
    return rdd.mapPartitions(
        lambda partition: collapse_function(partition, *args))

def scatter3d_mpl(x: list, y: list, z: list, radius: list=None):
    """3D scatter plot from matplotlib.
    Invoke show() or save the figure to get the result.

    Parameters
    ----------
    x: list of float
        X coordinate
    y: list of float
        Y coordinate
    z: list of float
        Z coordinate
    radius: int/float or list of int/float, optional
        If given, the size of the markers. Can be a single number
        of a list of sizes (of the same length as the coordinates)

    """
    import pylab as pl
    from mpl_toolkits.mplot3d import Axes3D

    fig = pl.figure()
    ax = Axes3D(fig)

    # Size of the centroids
    if radius is None:
        rad = 10.
    else:
        assert len(radius) == len(x), "Wrong size!"
        rad = radius

    ax.scatter(x, y, z, c=z, s=rad)

    ax.set_xlabel("X")
    ax.set_ylabel("Y")
    ax.set_zlabel("Z")


if __name__ == "__main__":
    """
    Run the doctest using

    python visualisation.py

    If the tests are OK, the script should exit gracefuly, otherwise the
    failure(s) will be printed out.
    """
    import sys
    import doctest
    import numpy as np

    from pyspark import SparkContext
    from pyspark3d import pyspark3d_conf
    from pyspark3d import load_user_conf

    # Activate the SparkContext for the test suite
    dic = load_user_conf()
    conf = pyspark3d_conf("local", "test", dic)
    sc = SparkContext.getOrCreate(conf=conf)

    # Numpy introduced non-backward compatible change from v1.14.
    if np.__version__ >= "1.14.0":
        np.set_printoptions(legacy="1.13")

    # Run the test suite
    failure_count, test_count = doctest.testmod()
    sys.exit(failure_count)
