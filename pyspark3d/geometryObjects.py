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
from pyspark3d import load_from_jvm

from py4j.java_gateway import JavaObject

def Point3D(x: float, y: float, z: float, isSpherical: bool) -> JavaObject:
    """
    Binding around Point3D.scala. For full description,
    see `$spark3d/src/main/scala/com/spark3d/geometryObjects/Point3D.scala`.

    By default, the input coordinates are supposed euclidean,
    that is (x, y, z). The user can also work with spherical input coordinates
    (x=r, y=theta, z=phi) by setting the argument isSpherical=true.

    Parameters
    ----------
    x : float
        Input X coordinate in Euclidean space, and R in spherical space.
    y : float
        Input Y coordinate in Euclidean space, and THETA in spherical space.
    z : float
        Input Z coordinate in Euclidean space, and PHI in spherical space.
    isSpherical : bool
        If true, it assumes that the coordinates of the Point3D
        are (r, theta, phi). Otherwise, it assumes cartesian
        coordinates (x, y, z).

    Returns
    ----------
    p3d : Point3D instance
        An instance of the class Point3D.

    Example
    ----------
    Instantiate a point with spherical coordinates (r, theta, phi)
    >>> p3d = Point3D(1.0, np.pi, 0.0, True)

    The returned type is JavaObject (Point3D instance)
    >>> print(type(p3d))
    <class 'py4j.java_gateway.JavaObject'>

    You can then call the method associated, for example
    >>> p3d.getVolume()
    0.0

    Return the point coordinates
    >>> p3d = Point3D(1.0, 1.0, 0.0, False)
    >>> p3d.getCoordinatePython()
    [1.0, 1.0, 0.0]

    It will be a JavaList by default
    >>> coord = p3d.getCoordinatePython()
    >>> print(type(coord))
    <class 'py4j.java_collections.JavaList'>

    Make it a python list
    >>> coord_python = list(coord)
    >>> print(type(coord_python))
    <class 'list'>

    [Astro] Convert the (theta, phi) in Healpix pixel index:
    >>> p3d = Point3D(1.0, np.pi, 0.0, True) # (z, theta, phi)
    >>> p3d.toHealpix(2048, True)
    50331644

    To see all the available methods:
    >>> print(sorted(p3d.__dir__())) # doctest: +NORMALIZE_WHITESPACE
    ['center', 'distanceTo', 'equals', 'getClass', 'getCoordinate',
    'getCoordinatePython', 'getEnvelope', 'getHash', 'getVolume',
    'hasCenterCloseTo', 'hashCode', 'intersects', 'isEqual', 'isSpherical',
    'notify', 'notifyAll', 'toHealpix', 'toHealpix$default$2', 'toString',
    'wait', 'x', 'y', 'z']
    """
    scalapath = "com.astrolabsoftware.spark3d.geometryObjects.Point3D"
    p3d = load_from_jvm(scalapath)

    return p3d(x, y, z, isSpherical)

def ShellEnvelope(*args) -> JavaObject:
    """
    Binding around ShellEnvelope.scala. For full description, see
    `$spark3d/src/main/scala/com/spark3d/geometryObjects/ShellEnvelope.scala`

    The Scala version makes use of several constructors (i.e. with different
    kinds of argument). In order to mimick this within a single routine, we
    abstract the arguments of the constructor using the iterable `*args`.
    There are then 5 possibilities to instantiate a `ShellEnvelope`:

    Case 1: Defined with a center coordinates, inner and outer radius.
        args = [x: Double, y: Double, z: Double,
            isSpherical: Boolean, innerRadius: Double, outerRadius: Double]
    Case 2: Defined with a center coordinates, and a radius (= a sphere).
        args = [x: Double, y: Double, z: Double,
            isSpherical: Boolean, radius: Double]
    Case 3: Defined with a Point3D, and a radius (= a sphere).
        args = [p: Point3D(...), radius: Double]
    Case 4: from another ShellEnvelope
        args = [shell: ShellEnvelope(...)]
    Case 5: Null envelope
        args = []

    Returns
    ----------
    shell : ShellEnvelope instance
        An instance of the class ShellEnvelope. Throw an error if
        the iterable in the constructor is not understood.

    Example
    ----------
    >>> from pyspark3d.geometryObjects import Point3D

    Case 1: Defined with a center coordinates (cart), inner and outer radius.
    >>> shell_case1 = ShellEnvelope(0.0, 1.0, 1.0, False, 0.5, 1.0)
    >>> assert("ShellEnvelope" in shell_case1.__str__())

    Case 2: Defined with a center coordinates, and a radius (= a sphere).
    >>> shell_case2 = ShellEnvelope(0.0, 0.0, 0.0, False, 1.0)
    >>> print(round(shell_case2.getArea(), 1))
    12.6

    Case 3: Defined with a Point3D, and a radius (= a sphere).
    >>> origin = Point3D(0.0, 0.0, 0.0, False)
    >>> shell_case3 = ShellEnvelope(origin, 1.0)
    >>> print(shell_case3.intersects(origin))
    True

    Case 4: From another ShellEnvelope
    >>> shell_case4 = ShellEnvelope(shell_case3)
    >>> print(shell_case4.isEqual(shell_case3))
    True

    Case 5: The null shell
    >>> shell_case5 = ShellEnvelope()
    >>> print(shell_case5.isNull())
    True

    To see all the available methods:
    >>> print(sorted(shell_case1.__dir__())) # doctest: +NORMALIZE_WHITESPACE
    ['center', 'contains', 'equals', 'expandBy', 'expandInnerRadius',
    'expandOuterRadius', 'expandToInclude', 'getArea', 'getClass',
    'getEnvelope', 'getHash', 'hasCenterCloseTo', 'hashCode', 'innerRadius',
    'innerRadius_$eq', 'intersects', 'intersectsShell', 'isEqual', 'isNull',
    'isPointInShell', 'notify', 'notifyAll', 'outerRadius', 'outerRadius_$eq',
    'setToNull', 'toHealpix', 'toHealpix$default$2', 'toString', 'wait']

    """
    warning = """
        There are 5 possibilities to instantiate a `ShellEnvelope`:

        Case 1: Defined with a center coordinates, inner and outer radius.
            args = [x: Double, y: Double, z: Double,
                isSpherical: Boolean, innerRadius: Double, outerRadius: Double]
        Case 2: Defined with a center coordinates, and a radius (= a sphere).
            args = [x: Double, y: Double, z: Double,
                isSpherical: Boolean, radius: Double]
        Case 3: Defined with a Point3D, and a radius (= a sphere).
            args = [p: Point3D(...), radius: Double]
        Case 4: from another ShellEnvelope
            args = [shell: ShellEnvelope(...)]
        Case 5: Null envelope
            args = []
    """
    scalapath = "com.astrolabsoftware.spark3d.geometryObjects.ShellEnvelope"
    shell = load_from_jvm(scalapath)

    # case 6
    if len(args) == 0:
        return shell()

    # Case 5
    elif len(args) == 1:
        cond_shell = "ShellEnvelope" in args[0].__str__()

        msg = """
        You are trying to instantiate a ShellEnvelope with 1 argument which is
        not a ShellEnvelope.

        {}
        """.format(warning)

        assert(cond_shell), msg

        return shell(args[0])

    # Case 3
    elif len(args) == 2:
        msg = """
        You are trying to instantiate a ShellEnvelope with 2 arguments
        which are not a Point3D (center) and a float (radius).

        {}
        """.format(warning)

        assert("Point3D" in args[0].__str__()), msg
        assert(type(args[1]) is float or type(args[1]) is int), msg

        return shell(args[0], args[1])

    # Case 2
    elif len(args) == 5:
        msg = """
        You are trying to instantiate a ShellEnvelope with 5 arguments
        but there is one or several type mismatch.

        {}
        """.format(warning)

        assert(type(args[0]) == int or type(args[0]) == float), msg
        assert(type(args[1]) == int or type(args[1]) == float), msg
        assert(type(args[2]) == int or type(args[2]) == float), msg
        assert(type(args[3]) == bool), msg
        assert(type(args[4]) == int or type(args[4]) == float), msg

        return shell(args[0], args[1], args[2], args[3], args[4])
    # Case 1
    elif len(args) == 6:
        msg = """
        You are trying to instantiate a ShellEnvelope with 6 arguments
        but there is one or several type mismatch.

        {}
        """.format(warning)

        assert(type(args[0]) == int or type(args[0]) == float), msg
        assert(type(args[1]) == int or type(args[1]) == float), msg
        assert(type(args[2]) == int or type(args[2]) == float), msg
        assert(type(args[3]) == bool), msg
        assert(type(args[4]) == int or type(args[4]) == float), msg
        assert(type(args[5]) == int or type(args[5]) == float), msg

        return shell(args[0], args[1], args[2], args[3], args[4], args[5])
    else:
        msg = """
        Constructor not understood.

        {}
        """.format(warning)
        assert(False), msg

def BoxEnvelope(*args) -> JavaObject:
    """
    Binding around BoxEnvelope.scala. For full description,
    see `$spark3d/src/main/scala/com/spark3d/geometryObjects/BoxEnvelope.scala`

    The Scala version makes use of several constructors (i.e. with different
    kinds of argument). In order to mimick this within a single routine, we
    abstract the arguments of the constructor using the iterable `*args`.
    There are then 5 possibilities to instantiate a `BoxEnvelope`:

    Case 1: from coordinates
        args = [x1: float, x2: float, y1: float, y2: float,
            z1: float, z2: float]
    Case 2: from a single Point3D (i.e. the box is a Point3D)
        args = [p: Point3D(...)]
    Case 3: from three Point3D
        args = [p1: Point3D(...), p2: Point3D(...), p3: Point3D(...)]
    Case 4: from another BoxEnvelope
        args = [b: BoxEnvelope(...)]
    Case 5: Null envelope
        args = []

    Coordinates of input Point3D MUST be cartesian.

    Returns
    ----------
    box : BoxEnvelope instance
        An instance of the class BoxEnvelope. Throw an error if the
        iterable in the constructor is not understood.

    Example
    ----------
    >>> from pyspark3d.geometryObjects import Point3D

    Case 1: Cube from coordinates
    >>> box_case1 = BoxEnvelope(0.0, 1.0, 0.0, 1.0, 0.0, 1.0)
    >>> print(box_case1.__str__())
    Env[0.0 : 1.0, 0.0 : 1.0, 0.0 : 1.0, ]

    Case 2: Zero volume
    >>> p3d = Point3D(0.0, 0.0, 0.0, False)
    >>> box_case2 = BoxEnvelope(p3d)
    >>> print(box_case2.getVolume())
    0.0

    Case 3: Cube from 3 Point3D
    >>> p3d_1 = Point3D(0.0, 1.0, 0.0, False)
    >>> p3d_2 = Point3D(0.1, 1.0, 0.0, False)
    >>> p3d_3 = Point3D(1.0, -1.0, 1.0, False)
    >>> origin = Point3D(0.0, 0.0, 0.0, False)
    >>> box_case3 = BoxEnvelope(p3d_1, p3d_2, p3d_3)
    >>> print(box_case3.contains(origin))
    True

    Case 4: From another envelope
    >>> box_case4 = BoxEnvelope(box_case3)
    >>> print(box_case4.isEqual(box_case3))
    True

    Case 5: The null cube
    >>> box_case5 = BoxEnvelope()
    >>> print(box_case5.isNull())
    True

    To see all the available methods:
    >>> print(sorted(box_case1.__dir__())) # doctest: +NORMALIZE_WHITESPACE
    ['apply', 'center', 'contains', 'covers', 'distance', 'equals', 'expandBy',
    'expandOutwards', 'expandToInclude', 'getClass', 'getEnvelope', 'getHash',
    'getVolume', 'getXLength', 'getYLength', 'getZLength', 'hasCenterCloseTo',
    'hashCode', 'indexID', 'indexID_$eq', 'intersection', 'intersects',
    'intersectsBox', 'intersectsRegion', 'isEqual', 'isNull', 'maxExtent',
    'maxX', 'maxX_$eq', 'maxY', 'maxY_$eq', 'maxZ', 'maxZ_$eq', 'minExtent',
    'minX', 'minX_$eq', 'minY', 'minY_$eq', 'minZ', 'minZ_$eq', 'notify',
    'notifyAll', 'setToNull', 'toHealpix', 'toHealpix$default$2', 'toString',
    'translate', 'wait']

    """
    warning = """
        There are 5 possibilities to instantiate a `BoxEnvelope`:

        Case 1: from coordinates
            args = [x1: float, x2: float, y1: float, y2: float,
                z1: float, z2: float]
        Case 2: from a single Point3D (i.e. the box is a Point3D)
            args = [p: Point3D(...)]
        Case 3: from three Point3D
            args = [p1: Point3D(...), p2: Point3D(...), p3: Point3D(...)]
        Case 4: from another BoxEnvelope
            args = [b: BoxEnvelope(...)]
        Case 5: Null envelope
            args = []
    """
    scalapath = "com.astrolabsoftware.spark3d.geometryObjects.BoxEnvelope"
    box = load_from_jvm(scalapath)

    # case 6
    if len(args) == 0:
        return box()

    # Case 2 or 4
    elif len(args) == 1:
        cond_p3d = "Point3D" in args[0].__str__()
        cond_box = "Env" in args[0].__str__()

        msg = """
        You are trying to instantiate a BoxEnvelope with 1 argument which is
        neither a Point3D nor a BoxEnvelope.

        {}
        """.format(warning)

        assert(cond_p3d or cond_box), msg

        return box(args[0])

    # Case 3
    elif len(args) == 3:
        msg = """
        You are trying to instantiate a BoxEnvelope with 3 arguments and one
        at least is not a Point3D.

        {}
        """.format(warning)

        for arg in args:
            assert("Point3D" in arg.__str__()), msg

        return box(args[0], args[1], args[2])

    # Case 3
    elif len(args) == 6:
        msg = """
        You are trying to instantiate a BoxEnvelope with 3 arguments and one
        at least is not a Point3D.

        {}
        """.format(warning)

        for arg in args:
            assert(type(arg) == int or type(arg) == float), msg

        return box(args[0], args[1], args[2], args[3], args[4], args[5])
    else:
        msg = """
        Constructor not understood.

        {}
        """.format(warning)
        assert(False), msg


if __name__ == "__main__":
    """
    Run the doctest using

    python geometryObjects.py

    If the tests are OK, the script should exit gracefuly, otherwise the
    failure(s) will be printed out.
    """
    import sys
    import doctest
    import numpy as np

    from pyspark import SparkContext
    from pyspark3d import pyspark3d_conf
    from pyspark3d import load_user_conf
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
