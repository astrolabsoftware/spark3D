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
from pyspark3d.repartitioning import prePartition, repartitionByCol

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
        '-K', dest='K',
        default=10,
        type=int,
        help='Number of neighbours')

    ## Arguments
    parser.add_argument(
        '-target', dest='target',
        default=[],
        nargs='+',
        type=float,
        help='Target point')

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
    if fn.endswith(".fits"):
        df = spark.read.format("fits").option("hdu", 1).load(fn)
    elif fn.endswith(".csv"):
        df = spark.read.format("csv").option("inferSchema", True).option("header", True).load(fn)
    else:
        print("Input format not understood - choose btw FITS or CSV.")

    # Perform the re-partitioning, and convert to Python RDD
    npart = args.npart
    gridtype = args.part
    if args.cartesian:
        coordSys = "cartesian"
    else:
        coordSys = "spherical"

    if gridtype is not None:
        fnout = "knn_with_{}_repartitioning.png".format(gridtype)
        title = "KNN with {} partitioning".format(gridtype)

        options = {
    	   "geometry": "points",
           "colnames": "x,y,z",
           "coordSys": coordSys,
           "gridtype": gridtype}

        df_colid = prePartition(df, options, npart)
        df = repartitionByCol(df_colid, "partition_id", True, npart)
    else:
        fnout = "knn_without_repartitioning.png"
        title = "KNN without partitioning"

    xyz = np.transpose(df.select("x", "y", "z").collect())

    if len(args.target) == 0:
        target = [0.2, 0.2, 0.2]
    else:
        target = args.target

    ## Plot Target
    radius = [30]
    ax = scatter3d_mpl(
        [target[0]], [target[1]], [target[2]],
        label="Target",
        radius=radius, **{"facecolors":"green"})

    ## Plot the data set
    all = np.transpose(df.select("x", "y", "z").collect())
    scatter3d_mpl(
        all[0], all[1], all[2],
        label="Data set",
        axIn=ax, **{"facecolors":"blue", "marker": "."})

    ## Plot KNN std
    nei = knn(df.select("x", "y", "z"), target, args.K, coordSys, False)
    xyz = np.transpose(nei.collect())
    radius = np.ones_like(xyz[0]) * 60
    scatter3d_mpl(
        xyz[0], xyz[1], xyz[2],
        label="KNN standard",
        radius=radius, axIn=ax, **{"facecolors":"red"})

    ## Plot KNN unique
    neiUnique = knn(df.select("x", "y", "z"), target, args.K, coordSys, True)
    xyz = np.transpose(neiUnique.collect())
    radius = np.ones_like(xyz[0]) * 30
    scatter3d_mpl(
        xyz[0], xyz[1], xyz[2],
        label="KNN Unique",
        radius=radius, axIn=ax, **{"facecolors":"orange", "marker": "D"})

    pl.legend()
    pl.title(title)
    pl.savefig(fnout)
    pl.show()
