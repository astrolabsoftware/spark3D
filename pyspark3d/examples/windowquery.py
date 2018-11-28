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
from pyspark3d.visualisation import scatter3d_mpl
from pyspark3d.queries import windowquery

import argparse

def drawSphere(xc, yc, zc, r):
    """ Draw a 3D sphere

    Parameters
    ----------
    xc : double
        X coordinate for the center in data units.
    yc : double
        Y coordinate for the center in data units.
    zc : double
        Z coordinate for the center in data units.
    r : double
        Radius of the sphere in data units.

    Returns
    ----------
    (x, y, z) : tuple of 1D array
        x, y, z 1D coordinates of the 3D surface.
    """
    u, v = np.mgrid[0: 2 * np.pi: 20j, 0: np.pi: 10j]
    x = np.cos(u) * np.sin(v)
    y = np.sin(u) * np.sin(v)
    z = np.cos(v)

    # shift to the center and scale the sphere
    x = r * x + xc
    y = r * y + yc
    z = r * z + zc
    return (x,y,z)

def addargs(parser):
    """ Parse command line arguments for windowquery """

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
        '-windowtype', dest='windowtype',
        required=True,
        type=str,
        help='')

    ## Arguments
    parser.add_argument(
        '-windowcoord', dest='windowcoord',
        default=[],
        nargs='+',
        type=float,
        help='')

    ## Arguments
    parser.add_argument(
        '--cartesian', dest='cartesian',
        action="store_true",
        help='Coordinate system')


if __name__ == "__main__":
    """
    Perform distributed window query
    """
    parser = argparse.ArgumentParser(
        description="""
        Perform distributed window query
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
    windowtype = args.windowtype
    if args.cartesian:
        coordSys = "cartesian"
    else:
        coordSys = "spherical"

    fnout = "{}_window.png".format(windowtype)
    title = "{} window".format(windowtype)

    xyz = np.transpose(df.select("x", "y", "z").collect())

    windowcoord = args.windowcoord

    ## Plot the data set
    all = np.transpose(df.select("x", "y", "z").collect())
    ax = scatter3d_mpl(
        all[0], all[1], all[2],
        radius=np.ones_like(all[0]) * 30,
        label="Data set", **{"facecolors":"blue", "marker": "+"})

    ## Plot Target
    if windowtype == "point":
        assert(len(windowcoord) == 3)
        radius = [30]
        scatter3d_mpl(
            [windowcoord[0]], [windowcoord[1]], [windowcoord[2]],
            radius=radius, axIn=ax, **{"facecolors":"red", "alpha": 0.3})
    elif windowtype == "sphere":
        assert(len(windowcoord) == 4)
        (xs,ys,zs) = drawSphere(windowcoord[0],windowcoord[1],windowcoord[2],windowcoord[3])
        ax.plot_surface(xs, ys, zs, color="r", alpha=0.3)
    elif windowtype == "shell":
        assert(len(windowcoord) == 5)
    elif windowtype == "box":
        assert(len(windowcoord) == 9)

    ## Plot window query
    env = windowquery(df.select("x", "y", "z"), windowtype, windowcoord, coordSys)
    xyz = np.transpose(env.collect())
    radius = np.ones_like(xyz[0]) * 60
    scatter3d_mpl(
        xyz[0], xyz[1], xyz[2],
        label="Objects found",
        radius=radius, axIn=ax, **{"facecolors":"black"})

    pl.legend()
    pl.title(title)
    pl.savefig(fnout)
    pl.show()
