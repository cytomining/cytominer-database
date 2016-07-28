# perturbation

[![Build Status](https://travis-ci.org/0x00B1/persistence.svg?branch=master)](https://travis-ci.org/0x00B1/persistence)

A package for storing perturbation data

### Using PostGresQL as backend

Setup database

```
docker run --name testdb -p 3210:5432 -P -e POSTGRES_PASSWORD=password -d postgres
PGPASSWORD=password psql -h localhost -p 3210 -U postgres -c "DROP DATABASE IF EXISTS testdb"
PGPASSWORD=password psql -h localhost -p 3210 -U postgres -c "CREATE DATABASE testdb"
```

First pass of creating backend

```
perturbation $DATADIR -o  postgresql://postgres:password@localhost:3210/testdb -t images
```

where `DATADIR` is the top-level directory containing `image.csv` and `object.csv` files, e.g. `DATADIR=test/data/`

Second pass of creating backend

```
DIRS=`find $DATADIR -maxdepth 1 -mindepth 1`
parallel  --eta --joblog processing.log --max-procs -1  perturbation {1} -o postgresql://postgres:password@localhost:3210/testdb -t objects ::: `echo $DIRS`
```

Shutdown

```
docker stop testdb
docker rm testdb
```

