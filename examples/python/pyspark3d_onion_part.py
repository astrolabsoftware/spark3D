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

from pyspark3d import set_spark_log_level
from pyspark3d import load_from_jvm
from pyspark3d.spatial3DRDD import Point3DRDD

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
    Re-partition RDD using ONION partitioning using pyspark3d
    """
    parser = argparse.ArgumentParser(
        description="""
        Re-partition RDD using ONION partitioning using pyspark3d
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
    rdd = Point3DRDD(
        spark, fn, "Z_COSMO,RA,DEC", True, "fits", {"hdu": args.hdu})

    # Perform the re-partitioning
    npart = args.npart
    gridtype = args.part

    if gridtype is not None:
        rdd_part = rdd.spatialPartitioningPython(gridtype, npart)
    else:
        rdd_part = rdd.rawRDD().toJavaRDD().repartition(npart)

    if not args.plot:
        count = rdd_part.count()
        print("{} elements".format(count))
    else:
        # Plot the result
        # Collect the data on driver -- just for visualisation purpose, do not
        # do that with full data set or you will destroy your driver!!
        import pylab as pl
        from mpl_toolkits.mplot3d import Axes3D

        fig = pl.figure()
        ax = Axes3D(fig)

        # Converter from spherical to cartesian coordinate system
        # it takes a Point3D and return a Point3D
        mod = "com.astrolabsoftware.spark3d.utils.Utils.sphericalToCartesian"
        converter = load_from_jvm(mod)

        # Convert data for plot -- List of List of Point3D
        # Maybe use toCoordRDD instead...
        data_glom = rdd_part.glom().collect()

        # Take only a few points (400 per partition) to speed-up
        # For each Point3D (el), grab the coordinates, convert it from
        # spherical to cartesian coordinate system (for the plot) and
        # make it a python list (it is JavaList by default)
        data_all = [
            np.array(
                [list(
                    converter(el).getCoordinatePython())
                 for el in part[0:400]]).T
            for part in data_glom]

        for i in range(len(data_all)):
            ax.scatter(data_all[i][0], data_all[i][1], data_all[i][2])

        ax.set_xlabel("X")
        ax.set_ylabel("Y")
        ax.set_zlabel("Z")

        if gridtype is not None:
            pl.savefig("onion_part_python.png")
        else:
            pl.savefig("onion_nopart_python.png")
        pl.show()
