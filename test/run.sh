#!/bin/sh
P="`pwd`"

for file in `find . -name *.py`
do
   echo "Executing $file"
   DIR=$(dirname ${file})
   cd $P/$DIR
   PIFILE=${file##*/}
   python $PIFILE
done
