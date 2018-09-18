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

import numpy as np
import pylab as pl

from pyspark3d import set_spark_log_level
from pyspark3d import load_user_conf
from pyspark3d import get_spark_session
from pyspark3d.spatial3DRDD import Point3DRDD
from pyspark3d.visualisation import scatter3d_mpl
from pyspark3d.visualisation import CollapseFunctions
from pyspark3d.visualisation import collapse_rdd_data
from pyspark3d.converters import toCoordRDD

import argparse

def addargs(parser):
    """ Parse command line arguments for pyspark3d_part """

    ## Arguments
    parser.add_argument(
        '-inputpath', dest='inputpath',
        required=True,
        help='Path to a FITS file')

    ## Arguments
    parser.add_argument(
        '-hdu', dest='hdu',
        required=True,
        help='HDU index to load.')

    ## Arguments
    parser.add_argument(
        '-part', dest='part',
        default=None,
        help='Type of partitioning')

    ## Arguments
    parser.add_argument(
        '-npart', dest='npart',
        default=10,
        type=int,
        help='Number of partition')

    ## Arguments
    parser.add_argument(
        '--plot', dest='plot',
        action="store_true",
        help='Number of partition')


if __name__ == "__main__":
    """
    Re-partition RDD using OCTREE partitioning using pyspark3d
    """
    parser = argparse.ArgumentParser(
        description="""
        Re-partition RDD using OCTREE partitioning using pyspark3d
        """)
    addargs(parser)
    args = parser.parse_args(None)

    # Load user conf and Spark session
    dic = load_user_conf()
    spark = get_spark_session(dicconf=dic)

    # Set logs to be quiet
    set_spark_log_level()

    # Load raw data
    fn = args.inputpath
    p3d = Point3DRDD(
        spark, fn, "x,y,z", False, "fits", {"hdu": args.hdu})

    # Perform the re-partitioning, and convert to Python RDD
    npart = args.npart
    gridtype = args.part
    crdd = toCoordRDD(p3d, gridtype, npart).cache()

    # Collapse the data using a simple mean of each partition
    cf = CollapseFunctions()
    data = collapse_rdd_data(crdd, cf.mean).collect()

    x = [p[0][0] for p in data if p[0] is not None]
    y = [p[0][1] for p in data if p[0] is not None]
    z = [p[0][2] for p in data if p[0] is not None]
    rad = np.array([p[1] for p in data if p[0] is not None])

    scatter3d_mpl(x, y, z, rad / np.max(rad) * 500)

    pl.savefig("test_collapse_function_mean.png")
    pl.show()
