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
from pyspark import SparkContext
from pyspark.sql import SparkSession

import inspect
from typing import Any, List, Dict

from pyspark3d.pyspark3d_conf import extra_jars
from pyspark3d.pyspark3d_conf import extra_packages
from pyspark3d.pyspark3d_conf import log_level

from pyspark3d.version import __version__

ROOT_JVM = "_gateway.jvm"
MSG = """WARNING: You should not use {} outside the test suite or in a regular
pyspark session, or in batch mode.
"""
global TEST
TEST = False

def get_spark_context() -> SparkContext:
    """
    Return the current SparkContext.
    Raise a RuntimeError if spark hasn't been initialized.

    Returns
    ---------
    sparkContext : SparkContext instance
        The active sparkContext

    Examples
    ---------
    >>> pysc = get_spark_context()
    >>> print(type(pysc))
    <class 'pyspark.context.SparkContext'>
    """
    if SparkContext._active_spark_context:
        return SparkContext._active_spark_context
    else:
        raise RuntimeError("SparkContext must be initialized")

def rec_load(obj: Any, mod: List[str], count: int=0) -> Any:
    """
    Load recursively JavaPackages and JavaClasses residing inside the JVM:
    python world -> gateway -> JVM -> my_scala_packages

    There is no guarantee your package exist though!
    See the example below for the syntax.

    Parameters
    ----------
    obj : SparkContext instance or Any
        Initial call must take a SparkContext (`pyspark.context.SparkContext`).
        Then obj will represent subsequently:
            - `py4j.java_gateway.JavaGateway`
            - `py4j.java_gateway.JVMView`
            - `py4j.java_gateway.JavaPackage`
            - `py4j.java_gateway.JavaPackage`
            - ...
    mod : str
        List of packages from the SparkContext to your class in the JVM
    count : int, optional
        Counter used for the recursion. Must be 0 at the initial call.

    Returns
    ----------
    obj : Any
        obj is an instance of a JVM object and will represent subsequently:
            - `py4j.java_gateway.JavaGateway`
            - `py4j.java_gateway.JVMView`
            - `py4j.java_gateway.JavaPackage`
            - `py4j.java_gateway.JavaPackage`
            - ...

    Example
    ----------
    >>> pysc = get_spark_context()
    >>> mod = "_gateway.jvm.com.astrolabsoftware.spark3d.spatialOperator"
    >>> jvm_obj = rec_load(pysc, mod.split("."))
    >>> print(type(jvm_obj))
    <class 'py4j.java_gateway.JavaPackage'>
    """
    if count == len(mod):
        return obj
    else:
        return rec_load(getattr(obj, mod[count]), mod, count+1)

def load_from_jvm(scala_package: str) -> Any:
    """
    Load a Scala package (instance of a JVM object) from Python.

    Note: If the object returned by `load_from_jvm` is a JavaPackage while you
    were expecting a JavaClass, that means it has not been found in the JVM.
    Most likely you forgot to include the spark3D JAR in the classpath.

    Parameters
    ----------
    scala_package : str
        Scala package path as if you were importing it in Scala.

    Returns
    ----------
    jvm_obj : Any
        instance of a JVM object: `py4j.java_gateway.JavaPackage` or
        `py4j.java_gateway.JavaClass`.

    Example
    ----------
    >>> pysc = get_spark_context()
    >>> sp = "com.astrolabsoftware.spark3d.spatialOperator.CenterCrossMatch"
    >>> jvm_obj = load_from_jvm(sp)
    >>> print(type(jvm_obj))
    <class 'py4j.java_gateway.JavaClass'>
    """
    path = ROOT_JVM + "." + scala_package
    packages = path.split(".")

    # Get the SparkContext
    pysc = get_spark_context()

    # Load from Scala to Python
    jvm_obj = rec_load(pysc, packages)

    return jvm_obj

def get_spark_session(
        master: str="local[*]",
        appname: str="test",
        dicconf={}) -> SparkSession:
    """
    Return a SparkSession with extra configuration parameters.

    Note
    ----------
    this routine is mainly used in 2 contexts:
      - Running the test suite
      - Using spark3D within a standard ipython or notebook session
    In those two cases, you need to load the JAR+deps within the session.

    In a regular pyspark session, or in batch mode, you should not use it.
    Instead, link spark3D when starting the session:

    pyspark --jars /path/to/spar3djar --master ...

    spark-submit --jars /path/to/spar3djar --master ...

    If you installed pyspark3d via pip, the JAR is released with the
    python source files. To find its location:
    `python -c "import pyspark3d; print(pyspark3d.__file__)"`

    Parameters
    ----------
    master : str
        Execution mode: local[*], spark://..., yarn, etc
        If you are already in a Spark Session (pyspark shell), this won't have
        any effect.
    appname : str
        Name for the application
    dicconf : dictionary
        Key/value to pass to the SparkConf. Typically location of JARS,
        Maven coordinates, etc..

    Returns
    ----------
    spark : SparkSession
        The spark session

    Examples
    ----------
    >>> dicconf = load_user_conf()
    >>> spark = get_spark_session("local[*]", "test", dicconf)
    >>> print(type(spark))
    <class 'pyspark.sql.session.SparkSession'>
    """
    if not TEST:
        print(MSG.format(inspect.currentframe().f_code.co_name))

    # Grab the user conf
    conf = pyspark3d_conf(master, appname, dicconf)

    # Instantiate a spark session
    spark = SparkSession\
        .builder\
        .appName("test")\
        .config(conf=conf)\
        .getOrCreate()

    return spark

def load_user_conf(sparkSession: SparkSession=None) -> Dict:
    """
    Load pre-defined user Spark configuration stored in pyspark3d_conf.py
    to be passed to the SparkConf.
    If a spark Session is already running, you can pass it to transfer the
    conf parameters.

    Note
    ----------
    this routine is mainly used in 2 contexts:
      - Running the test suite
      - Using spark3D within a standard ipython or notebook session
    In those two cases, you need to load the JAR+deps within the session.

    In a regular pyspark session, or in batch mode, you should not use it.
    Instead, link spark3D when starting the session:

    pyspark --jars /path/to/spar3djar --master ...

    spark-submit --jars /path/to/spar3djar --master ...

    If you installed pyspark3d via pip, the JAR is released with the
    python source files. To find its location:
    `python -c "import pyspark3d; print(pyspark3d.__file__)"`

    Parameters
    ----------
    sparkSession : SparkSession, optional
        If set, the returned dictionary contains the
        parameters from this spark Session. Default is None.

    Returns
    ---------
    dic : dictionary
        Dictionary with extra arguments to be passed to the SparkConf

    Examples
    ---------
    From scratch
    >>> dic = load_user_conf()
    >>> assert("spark.jars" in dic)

    From a previous session
    >>> spark = SparkSession.builder.appName("toto").getOrCreate()
    >>> dic = load_user_conf(spark)
    >>> print(dic["spark.app.name"])
    toto
    >>> assert("spark.jars" in dic)
    """
    if not TEST:
        print(MSG.format(inspect.currentframe().f_code.co_name))

    extra_jars_with_commas = ",".join(extra_jars)
    extra_packages_with_commas = ",".join(extra_packages)

    if sparkSession is not None:
        oldconf = sparkSession.sparkContext.getConf().getAll()
        dic = {}
        for item in oldconf:
            k, v = item
            if k == "spark.jars":
                v += "," + extra_jars_with_commas
            if k == "spark.jars.packages":
                v += "," + extra_packages_with_commas
            dic[k] = v
    else:
        dic = {
            "spark.jars": extra_jars_with_commas,
            "spark.jars.packages": extra_packages_with_commas
            }

    return dic

def pyspark3d_conf(
        master: str, AppName: str, confdic: Dict={}) -> SparkConf:
    """
    Set the configuration for running pyspark3d.
    In case you have a doubt about a missing package, just run:
    `conf.toDebugString().split("\n")`
    to see what is registered in the conf.

    Note
    ----------
    this routine is mainly used in 2 contexts:
      - Running the test suite
      - Using spark3D within a standard ipython or notebook session
    In those two cases, you need to load the JAR+deps within the session.

    In a regular pyspark session, or in batch mode, you should not use it.
    Instead, link spark3D when starting the session:

    pyspark --jars /path/to/spar3djar --master ...

    spark-submit --jars /path/to/spar3djar --master ...

    If you installed pyspark3d via pip, the JAR is released with the
    python source files. To find its location:
    `python -c "import pyspark3d; print(pyspark3d.__file__)"`

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
    >>> dic = load_user_conf()
    >>> conf = pyspark3d_conf("local[*]", "myTest", dic)
    >>> conf.get("spark.master")
    'local[*]'
    """
    if not TEST:
        print(MSG.format(inspect.currentframe().f_code.co_name))

    conf = SparkConf()
    conf.setMaster(master)
    conf.setAppName(AppName)
    for k, v in confdic.items():
        conf.set(key=k, value=v)

    return conf

def set_spark_log_level(log_level_manual=None):
    """
    Set the level of log in Spark.

    Parameters
    ----------
    log_level_manual : String, optional
        Level of log wanted: INFO, WARN, ERROR, OFF, etc.
        By default, this is read from pyspark3d_conf.py

    Example
    ----------
    >>> pysc = get_spark_context()

    # should be verbose or whatever Spark default has been set
    >>> rdd_verb = pysc.parallelize([1, 2, 3, 4]).collect()

    # Default is OFF
    >>> set_spark_log_level()
    >>> rdd_silent = pysc.parallelize([1, 2, 3, 4]).collect()

    # Explicitly force to be silent
    >>> set_spark_log_level("OFF")
    >>> rdd_silent = pysc.parallelize([1, 2, 3, 4]).collect()
    """
    ## Get the logger
    pysc = get_spark_context()
    logger = pysc._jvm.org.apache.log4j

    ## Set the level
    if log_level_manual is None:
        level = getattr(logger.Level, log_level, "OFF")
    else:
        level = getattr(logger.Level, log_level_manual, "INFO")

    logger.LogManager.getLogger("org").setLevel(level)
    logger.LogManager.getLogger("akka").setLevel(level)


if __name__ == "__main__":
    """
    Run the doctest using

    python __init__.py

    If the tests are OK, the script should exit gracefuly, otherwise the
    failure(s) will be printed out.
    """
    import sys
    import doctest
    import numpy as np

    # Switch to test mode
    TEST = True

    # Activate the SparkContext for the test suite
    dic = load_user_conf()
    conf = pyspark3d_conf("local", "test", dic)
    sc = SparkContext.getOrCreate(conf=conf)

    # Numpy introduced non-backward compatible change from v1.14.
    if np.__version__ >= "1.14.0":
        np.set_printoptions(legacy="1.13")

    # Run the test suite
    failure_count, test_count = doctest.testmod()
    sys.exit(failure_count)
