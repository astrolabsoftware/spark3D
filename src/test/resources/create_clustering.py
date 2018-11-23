#
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
from __future__ import division, absolute_import, print_function
from astropy.io import fits
import numpy as np
import sys
from sklearn.datasets import make_blobs

import argparse

def addargs(parser):
    """ Parse command line arguments """

    ## Number of row
    parser.add_argument(
        '-nrow', dest='nrow',
        required=True,
        type=int,
        help='Number of rows.')

    parser.add_argument(
        '-ncluster', dest='ncluster',
        required=True,
        type=int,
        help='Number of clusters.')

    parser.add_argument(
        '-stdcluster', dest='stdcluster',
        required=True,
        type=int,
        help='Std for clusters.')

    parser.add_argument(
        '-seed', dest='seed',
        type=int,
        default=0,
        help='Seed for the random number generator.')

    ## Output file name
    parser.add_argument(
        '-filename', dest='filename',
        default='test_file.fits',
        help='Name of the output file with .fits extension')


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="""
    Create dummy FITS file for test purpose.
    To create a FITS file just run
        `python create_clustering.py -nrow <> -ncluster <> -filename <> \
            -seed <> -coordinates <>`
    or
        `python create_clustering.py -h`
    to get help.
            """)
    addargs(parser)
    args = parser.parse_args(None)

    ## Grab the number of clusters desired
    ncluster = args.ncluster
    stdcluster = args.stdcluster
    nrow = args.nrow

    ## Initialise the seed for random number generator
    state = np.random.RandomState(args.seed)

    ## Primary HDU - just a header
    hdr = fits.Header()
    hdr['OBSERVER'] = "Toto l'asticot"
    hdr['COMMENT'] = "Here's some commentary about this FITS file."
    primary_hdu = fits.PrimaryHDU(header=hdr)

    ## Depends on the kind of coordinate system
    data = make_blobs(
        n_samples=nrow, n_features=3, centers=ncluster,
        cluster_std=stdcluster, center_box=(-10.0, 10.0),
        shuffle=True, random_state=state)
    a1 = np.array([p[0] for p in data[0]], dtype=np.float64)
    a2 = np.array([p[1] for p in data[0]], dtype=np.float64)
    a3 = np.array([p[2] for p in data[0]], dtype=np.float64)
    names = ["x", "y", "z"]

    ## Create each column
    col1 = fits.Column(name=names[0], format='E', array=a1)
    col2 = fits.Column(name=names[1], format='E', array=a2)
    col3 = fits.Column(name=names[2], format='E', array=a3)

    ## Format into columns
    cols = fits.ColDefs([col1, col2, col3])

    ## Make the first HDU.
    hdu1 = fits.BinTableHDU.from_columns(cols)

    ## Concatenate all HDU
    hdul = fits.HDUList([primary_hdu, hdu1])

    ## Save on disk
    hdul.writeto(args.filename)
