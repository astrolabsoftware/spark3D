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
from pyspark3d import load_user_conf
from pyspark3d import get_spark_session
from pyspark3d.spatial3DRDD import SphereRDD

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
    rdd = SphereRDD(
        spark, fn, "x,y,z,radius", False, "fits", {"hdu": args.hdu})

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

        # Convert data for plot
        # List[all partitions] of List[all Point3D per partition]
        data_glom = rdd_part.glom().collect()

        # Take only a few points (400 per partition) to speed-up
        # For each Sphere (el), takes the center and grab its coordinates and
        # make it a python list (it is JavaList by default)
        data_all = [
            np.array(
                [list(
                    el.center().getCoordinatePython())
                 for el in part[0:400]]).T
            for part in data_glom]

        # Collect the radius sizes
        radius = [
            np.array(
                [el.outerRadius()
                 for el in part[0:400]])
            for part in data_glom]

        # Plot partition-by-partition
        for i in range(len(data_all)):
            s = radius[i] * 3000
            ax.scatter(data_all[i][0], data_all[i][1], data_all[i][2], s=s)

        ax.set_xlabel("X")
        ax.set_ylabel("Y")
        ax.set_zlabel("Z")

        # Save the result on disk
        if gridtype is not None:
            pl.savefig("octree_part_python.png")
        else:
            pl.savefig("octree_nopart_python.png")
        pl.show()
