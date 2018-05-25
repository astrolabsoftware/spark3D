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
from astropy.io import fits
import pandas as pd

import argparse

def addargs(parser):
    """ Parse command line arguments for fits2csv """

    ## Arguments
    parser.add_argument(
        '-inputfits', dest='inputfits',
        required=True,
        help='Path to a FITS file')

    ## Arguments
    parser.add_argument(
        '-hdu', dest='hdu',
        required=True,
        type=int,
        help='HDU index to load.')

def load_fits_file(fn, hdu):
    """
    Load the data of a HDU.

    Parameters
    ----------
    fn : String
        File name
    hdu : Int
        Index of the HDU (must be bintable)

    Returns
    ----------
    dataAsList : List of List
        List of HDU columns
    """
    ## Return the data of the HDU only
    dataHdu = fits.open(fn)[hdu]

    ## Grab column names
    num_cols = len(dataHdu.data[0])
    columnNames = [
        dataHdu.header["TTYPE{}".format(i + 1)] for i in range(num_cols)]

    return columnNames, dataHdu.data


if __name__ == "__main__":
    """
    Convert a FITS file to a CSV file.
    """
    parser = argparse.ArgumentParser(
        description="""
        Convert a FITS file to a CSV file.
        """)
    addargs(parser)
    args = parser.parse_args(None)

    ## Grab column names and data
    columnNames, dataFits = load_fits_file(args.inputfits, args.hdu)

    ## Transform the data as DataFrame
    dataFrame = pd.DataFrame(dataFits)

    ## Switch the extension of the input file.
    outname = args.inputfits.split(".")[0] + ".csv"

    ## Save data as CSV
    dataFrame.to_csv(outname, index=False)
