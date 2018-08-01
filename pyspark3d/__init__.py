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
from pyspark import SparkContext
from typing import Any, List

import os
import doctest
import numpy as np

from pyspark3d_conf import pyspark3d_conf

ROOT_JVM = "_gateway.jvm"

def get_spark_context() -> SparkContext:
    """
    Return the current SparkContext.
    Raise a RuntimeError if spark hasn't been initialized.

    Returns
    ---------
    sparkContext : SparkContext instance
        The active sparkContext
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


if __name__ == "__main__":
    """
    Run the doctest using

    python __init__.py

    If the tests are OK, the script should exit gracefuly, otherwise the
    failure(s) will be printed out.
    """
    # Activate the SparkContext for the test suite
    pwd = os.environ["PWD"]
    spark3d_jar = os.path.join(
        pwd, "../target/scala-2.11/spark3d_2.11-0.1.5.jar")
    dic = {"spark.jars": spark3d_jar}
    conf = pyspark3d_conf("local", "test", dic)
    sc = SparkContext.getOrCreate(conf=conf)

    # Numpy introduced non-backward compatible change from v1.14.
    if np.__version__ >= "1.14.0":
        np.set_printoptions(legacy="1.13")

    # Run the test suite
    doctest.testmod()
