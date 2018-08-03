#!/bin/bash

## Script to measure the coverage of the test suite (via doctest).
## Launch it using ./coverage
## and open the html files under the folder htmlcov/
## Skip xpure.py as it is not really part of the pipeline
cd pyspark3d
for i in *.py
do
    coverage run -a $i
done

coverage report

echo " " >> cov.txt
echo $(date) >> cov.txt
coverage report >> cov.txt

coverage html
cd ../
