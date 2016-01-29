#!/bin/sh

function join { local IFS="$1"; shift; echo "$*"; }

patterns=$(head -n 1 object.csv | tr ',' '\n' | uniq | tail -n +2)

csvcut -c 1 -x object.csv | tail -n +2 > test/data/patterns/images.csv

csvcut -c 2 -x object.csv | tail -n +2 > test/data/patterns/objects.csv

csvjoin patterns/images.csv patterns/objects.csv  > test/data/patterns/images_objects.csv

for pattern in ${patterns}; do
  indices=$(csvcut -n object.csv | grep -i $pattern | cut -d ':' -f1)

  joined=$(join , ${indices[@]}) 

  csvcut -c ${joined} -C 2 -x object.csv | tail -n +2 > test/data/patterns/${pattern}.csv

  csvjoin test/data/patterns/images_objects.csv test/data/patterns/${pattern}.csv > test/data/${pattern}.csv
done
