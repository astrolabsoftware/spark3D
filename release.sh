#!/bin/bash

## TODO before bumping version:
##  - run sbt clean
##  - update build.sbt
##  - update website (01_installation.md, picture of the package, _pages/home.md, )
##  - update README.md
##  - update pyspark3d/version.py
##  - update the CHANGELOG.md
##  - update the runners
##  - update the notebook, examples
## THEN:

## GitHub
git tag <name>
git push origin --tags

## Maven - Scala 2.11
sbt ++2.11.8 publishSigned
sbt ++2.11.8 sonatypeRelease

## pypi
python setup.py sdist bdist_wheel
twine upload dist/*

## Note: to be automated! And clean the repo after this.
