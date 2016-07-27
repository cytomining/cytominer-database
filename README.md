# perturbation

[![Build Status](https://travis-ci.org/0x00B1/persistence.svg?branch=master)](https://travis-ci.org/0x00B1/persistence)

A package for storing perturbation data

## Notes

- Using PostGresQL as backend
 - `docker run --name testdb -p 3210:5432 -P -e POSTGRES_PASSWORD=password -d postgres`
 - `PGPASSWORD=password psql -h localhost -p 3210 -U postgres -c "DROP DATABASE IF EXISTS testdb"`
 - `PGPASSWORD=password psql -h localhost -p 3210 -U postgres -c "CREATE DATABASE testdb"`
 - connection string is `postgresql://postgres:password@localhost:3210/testdb`

