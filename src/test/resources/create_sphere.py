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

import argparse

def addargs(parser):
    """ Parse command line arguments """

    ## Number of row
    parser.add_argument(
        '-nrow', dest='nrow',
        required=True,
        type=int,
        help='Number of row.')

    parser.add_argument(
        '-coordinates', dest='coordinates',
        required=True,
        type=str,
        help='cartesian or spherical')

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
    To create a FITS file with spheres just run
      `python create_sphere.py -nrow <> -filename <> -seed <> -coordinates <>`
    or
      `python create_sphere.py -h`
    to get help.
            """)
    addargs(parser)
    args = parser.parse_args(None)

    ## Grab the number of row desired
    n = args.nrow

    ## Initialise the seed for random number generator
    state = np.random.RandomState(args.seed)

    ## Primary HDU - just a header
    hdr = fits.Header()
    hdr['OBSERVER'] = "Toto l'asticot"
    hdr['COMMENT'] = "Here's some commentary about this FITS file."
    primary_hdu = fits.PrimaryHDU(header=hdr)

    ## Depends on the kind of coordinate system
    if args.coordinates == "spherical":
        ## First extension: (r, theta, phi)
        a1 = np.array(state.rand(n), dtype=np.float64)
        a2 = np.array(state.rand(n) * np.pi, dtype=np.float64)
        a3 = np.array(state.rand(n) * 2 * np.pi, dtype=np.float64)
        a4 = np.array(state.rand(n) * 0.01, dtype=np.float64)
        names = ["Z_COSMO", "RA", "Dec", "radius"]
    elif args.coordinates == "cartesian":
        a1 = np.array(state.rand(n), dtype=np.float64)
        a2 = np.array(state.rand(n), dtype=np.float64)
        a3 = np.array(state.rand(n), dtype=np.float64)
        a4 = np.array(state.rand(n) * 0.01, dtype=np.float64)
        names = ["x", "y", "z", "radius"]
    else:
        print("You need to choose cartesian or spherical coordinates!")
        sys.exit()

    ## Create each column
    col1 = fits.Column(name=names[0], format='E', array=a1)
    col2 = fits.Column(name=names[1], format='E', array=a2)
    col3 = fits.Column(name=names[2], format='E', array=a3)
    col4 = fits.Column(name=names[3], format='E', array=a4)

    ## Format into columns
    cols = fits.ColDefs([col1, col2, col3, col4])

    ## Make the first HDU.
    hdu1 = fits.BinTableHDU.from_columns(cols)

    ## Concatenate all HDU
    hdul = fits.HDUList([primary_hdu, hdu1])

    ## Save on disk
    hdul.writeto(args.filename)
