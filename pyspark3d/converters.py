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
from py4j.java_gateway import JavaObject
from py4j.java_collections import JavaList

from pyspark3d import load_from_jvm
from pyspark3d import get_spark_context

from pyspark import RDD
from pyspark.mllib.common import _java2py

def scala2java(scala_list: JavaObject) -> JavaList:
    """
    Convert a Scala list coming from the JVM into a JavaList:
    scala.collection.immutable.List -> java.util.ArrayList

    Parameters
    ----------
    scala_list : JavaObject
        Scala list (scala.collection.immutable.List)

    Returns
    ----------
    java_list : JavaList
        Java list (java.util.ArrayList)

    Examples
    ----------
    Instantiate a Point3D
    >>> from pyspark3d.geometryObjects import Point3D
    >>> p = Point3D(0.0, 0.0, 0.0, True)

    Grab the coordinates
    >>> coord_scala = p.getCoordinate()
    >>> assert(type(coord_scala) == JavaObject)

    `coord_scala` is a Scala List. Convert it to Java List
    >>> coord_java = scala2java(coord_scala)
    >>> assert(type(coord_java) == JavaList)
    """
    # Not very powerful check... As many things other than Scala list
    # are JavaObject...
    msg = """
    converters.scala2java expects a JavaObject not {}.
    """.format(type(scala_list))
    assert(type(scala_list) == JavaObject), msg

    convpath = "scala.collection.JavaConverters.seqAsJavaListConverter"
    converter = load_from_jvm(convpath)
    return converter(scala_list).asJava()

def java2python(java_list: JavaList) -> list:
    """
    Convert a Java list into a python list:
    java.util.ArrayList -> list

    Parameters
    ----------
    java_list : JavaList
        Java list (java.util.ArrayList)

    Returns
    ----------
    python_list : list
        Python list

    Examples
    ----------
    Instantiate a Point3D
    >>> from pyspark3d.geometryObjects import Point3D
    >>> p = Point3D(0.0, 0.0, 0.0, True)

    Grab the coordinates
    >>> coord_scala = p.getCoordinate()
    >>> assert(type(coord_scala) == JavaObject)

    `coord_scala` is a Scala List. Convert it to Java List
    >>> coord_java = scala2java(coord_scala)
    >>> assert(type(coord_java) == JavaList)

    Convert then the Java List into a python list
    >>> coord_python = java2python(coord_java)
    >>> assert(type(coord_python) == list)

    Other example. Instantiate directly a JavaList in python
    >>> java_list = sc._jvm.java.util.ArrayList()
    >>> java_list.add(0.0) # return a boolean
    True

    >>> python_list = java2python(java_list)
    >>> assert(python_list[0] == 0.0)
    """
    msg = """
    converters.java2python expects a JavaList not {}.
    """.format(type(java_list))
    assert(type(java_list) == JavaList), msg

    return list(java_list)

def scala2python(scala_list: JavaObject) -> list:
    """
    Convert a Scala list coming from the JVM into a python list:
    scala.collection.immutable.List -> list

    Parameters
    ----------
    scala_list : JavaObject
        Scala list (scala.collection.immutable.List)

    Returns
    ----------
    python_list : list
        Python list

    Examples
    ----------
    Instantiate a Point3D
    >>> from pyspark3d.geometryObjects import Point3D
    >>> p = Point3D(0.0, 0.0, 0.0, True)

    Grab the coordinates
    >>> coord_scala = p.getCoordinate()
    >>> assert(type(coord_scala) == JavaObject)

    `coord_scala` is a Scala List. Convert it to python List
    >>> coord_python = scala2python(coord_scala)
    >>> assert(type(coord_python) == list)
    """
    # Not very powerful check... As many things other than Scala list
    # are JavaObject...
    msg = """
    converters.scala2python expects a JavaObject not {}.
    """.format(type(scala_list))
    assert(type(scala_list) == JavaObject), msg

    return java2python(scala2java(scala_list))


if __name__ == "__main__":
    """
    Run the doctest using

    python converters.py

    If the tests are OK, the script should exit gracefuly, otherwise the
    failure(s) will be printed out.
    """
    import os
    import sys
    import doctest
    import numpy as np

    from pyspark import SparkContext
    from pyspark3d import pyspark3d_conf
    from pyspark3d import load_user_conf
    from pyspark3d import get_spark_session

    # Activate the SparkContext for the test suite
    dic = load_user_conf()
    conf = pyspark3d_conf("local[*]", "test", dic)
    sc = SparkContext.getOrCreate(conf=conf)

    # Load the spark3D JAR+deps, and initialise the spark session.
    # In a pyspark shell, you do not need this.
    spark = get_spark_session(dicconf=dic)

    # Numpy introduced non-backward compatible change from v1.14.
    if np.__version__ >= "1.14.0":
        np.set_printoptions(legacy="1.13")

    # Run the test suite
    failure_count, test_count = doctest.testmod(extraglobs={"spark": spark})
    sys.exit(failure_count)
