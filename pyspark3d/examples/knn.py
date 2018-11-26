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
from pyspark.sql import SparkSession

import numpy as np
import pylab as pl

from pyspark3d import set_spark_log_level
from pyspark3d.visualisation import scatter3d_mpl, sph2cart
from pyspark3d.queries import knn
from pyspark3d.repartitioning import addSPartitioning, repartitionByCol

import argparse

def addargs(parser):
    """ Parse command line arguments for knn """

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
        '--cartesian', dest='cartesian',
        action="store_true",
        help='Coordinate system')


if __name__ == "__main__":
    """
    Perform distributed KNN search
    """
    parser = argparse.ArgumentParser(
        description="""
        Perform distributed KNN search
        """)
    addargs(parser)
    args = parser.parse_args(None)

    # Initialise Spark Session
    spark = SparkSession.builder\
        .appName("collapse")\
        .getOrCreate()

    # Set logs to be quiet
    set_spark_log_level()

    # Load raw data
    fn = args.inputpath
    df = spark.read.format("fits").option("hdu", 1).load(fn)

    # Perform the re-partitioning, and convert to Python RDD
    npart = args.npart
    gridtype = args.part
    if args.cartesian:
        coordSys = "cartesian"
    else:
        coordSys = "spherical"

    if gridtype is not None:

        options = {
    	   "geometry": "points",
           "colnames": "x,y,z",
           "coordSys": coordSys,
           "gridtype": gridtype}

        df_colid = addSPartitioning(df, options, npart)
        df = repartitionByCol(df_colid, "partition_id", True, npart)

    xyz = np.transpose(df.select("x", "y", "z").collect())

    k = 100
    target = [0.2, 0.2, 0.2]
    nei = knn(df.select("x", "y", "z"), target, k, coordSys, False)

    xyz = np.transpose(nei.collect())

    ax = scatter3d_mpl(xyz[0], xyz[1], xyz[2], **{"facecolors":"red"})
    ax3 = scatter3d_mpl(target[0], target[1], target[2], axIn=ax, **{"facecolors":"green"})

    pl.savefig("knn.png")
    pl.show()
