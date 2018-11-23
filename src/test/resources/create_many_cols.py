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
        help='Number of rows.')

    parser.add_argument(
        '-ncols', dest='ncols',
        required=True,
        type=int,
        help='Number of columns')

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
        `python create_point.py -nrow <> -filename <> -seed <> -coordinates <>`
    or
        `python create_point.py -h`
    to get help.
            """)
    addargs(parser)
    args = parser.parse_args(None)

    ## Grab the number of row desired
    nrow = args.nrow
    ncols = args.ncols

    ## Initialise the seed for random number generator
    state = np.random.RandomState(args.seed)

    ## Primary HDU - just a header
    hdr = fits.Header()
    hdr['OBSERVER'] = "Toto l'asticot"
    hdr['COMMENT'] = "Here's some commentary about this FITS file."
    primary_hdu = fits.PrimaryHDU(header=hdr)

    cols = []
    for index in range(ncols):
        if index == 0:
            arr = np.array(state.rand(nrow), dtype=np.float64)
        elif index == 1:
            arr = np.array(state.rand(nrow) * np.pi, dtype=np.float64)
        elif index == 2:
            arr = np.array(state.rand(nrow) * 2 * np.pi, dtype=np.float64)
        else:
            arr = np.array(state.rand(nrow), dtype=np.float64)

        ## Create each column
        cols.append(fits.Column(name="col{}".format(index), format='E', array=arr))

    ## Format into columns
    allCols = fits.ColDefs(cols)

    ## Make the first HDU.
    hdu1 = fits.BinTableHDU.from_columns(allCols)

    ## Concatenate all HDU
    hdul = fits.HDUList([primary_hdu, hdu1])

    ## Save on disk
    hdul.writeto(args.filename)
