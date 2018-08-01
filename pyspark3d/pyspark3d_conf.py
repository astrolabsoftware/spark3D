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
from pyspark import SparkConf
from typing import Dict

import doctest
import numpy as np

def pyspark3d_conf(
        master: str, AppName: str, confdic: Dict={}) -> SparkConf:
    """
    Set the configuration for running pyspark3d.
    In case you have a doubt about a missing package, just run:
    `conf.toDebugString().split("\n")`
    to see what is registered in the conf.

    Parameters
    ----------
    master : str
        The master URL to connect to, such as "local" to run
        locally with one thread, "local[4]" to run locally with 4 cores, or
        "spark://master:7077" to run on a Spark standalone cluster, or
        "yarn" to run on a YARN cluster.
    AppName : str
        The name for the application.
    confdic : Dict, optional
        Additional key/value to be passed to the configuration.
        Typically, this is the place where you will set the path to
        external JARS.

    Returns
    ----------
    conf : SparkConf instance

    Examples
    ----------
    >>> dic = {"spark.jars": "path/to/my/jar1,path/to/my/jar2"}
    >>> conf = pyspark3d_conf("local[*]", "myTest", dic)
    >>> conf.get("spark.master")
    'local[*]'
    """
    conf = SparkConf()
    conf.setMaster(master)
    conf.setAppName(AppName)
    for k, v in confdic.items():
        conf.set(key=k, value=v)

    return conf


if __name__ == "__main__":
    """
    Run the doctest using

    python pyspark3d_conf.py

    If the tests are OK, the script should exit gracefuly, otherwise the
    failure(s) will be printed out.
    """
    # Numpy introduced non-backward compatible change from v1.14.
    if np.__version__ >= "1.14.0":
        np.set_printoptions(legacy="1.13")

    # Run the test suite
    doctest.testmod()
