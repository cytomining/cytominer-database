#!/bin/bash

stdin=$1

join() {
    local IFS="$1";

    shift;

    echo "$*";
}

task() {

    directory=${stdin}/${1}/

    patterns_directory=${directory}/patterns

    object_csv=${directory}/object.csv

    images_csv=${patterns_directory}/images.csv

    objects_csv=${patterns_directory}/objects.csv

    if [[ ! -e $object_csv ]];
    then
        #echo Skipping directory $directory because object.csv not found.
        return
    fi

    images_objects=${patterns_directory}/images_objects.csv

    mkdir -p ${patterns_directory}

    csvcut -c 1 -x ${object_csv} | tail -n +2 > ${images_csv}
    csvcut -c 2 -x ${object_csv} | tail -n +2 > ${objects_csv}

    csvjoin ${images_csv} ${objects_csv}  > ${images_objects}

    for pattern in $(head -n 1 ${directory}/object.csv | tr ',' '\n' | sort | tr -d '\r' | uniq | grep -v Image); do
        indices=$(csvcut -n ${object_csv} | grep -i ${pattern} | cut -d ':' -f1)

        joined=$(join , ${indices[@]})

        csvcut -c ${joined} -C 2 -x ${object_csv} | tail -n +2 > ${patterns_directory}/${pattern}.csv

        csvjoin ${images_objects} ${patterns_directory}/${pattern}.csv > ${directory}/${pattern}.csv
    done

    rm -rf ${patterns_directory}
}

for directory in $(ls ${stdin}); do
    if ${TRAVIS};
    then
        task ${directory}
    else
        task ${directory} &
    fi
done

wait