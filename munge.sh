#!/bin/sh
DATADIR=test/data

function join { local IFS="$1"; shift; echo "$*"; }

patterns=$(head -n 1 ${DATADIR}/object.csv | tr ',' '\n' | uniq | tail -n +2)

csvcut -c 1 -x ${DATADIR}/object.csv | tail -n +2 > ${DATADIR}/patterns/images.csv

csvcut -c 2 -x ${DATADIR}/object.csv | tail -n +2 > ${DATADIR}/patterns/objects.csv

csvjoin ${DATADIR}/patterns/images.csv ${DATADIR}/patterns/objects.csv  > ${DATADIR}/patterns/images_objects.csv

for pattern in ${patterns}; do
  indices=$(csvcut -n ${DATADIR}/object.csv | grep -i $pattern | cut -d ':' -f1)

  joined=$(join , ${indices[@]})

  csvcut -c ${joined} -C 2 -x ${DATADIR}/object.csv | tail -n +2 > ${DATADIR}/patterns/${pattern}.csv

  csvjoin ${DATADIR}/patterns/images_objects.csv ${DATADIR}/patterns/${pattern}.csv > ${DATADIR}/${pattern}.csv
done
