#!/bin/bash

#python3 -m virtualenv .env
#source .env/bin/activate
#pip install -r requirements.txt


docker pull postgres
docker pull mariadb
#docker pull dpage/pgadmin4

#clean out stale containers
docker stop $(docker ps -aq)
docker rm $(docker ps -a -q)
#docker system prune -a -f
docker run -d --name docker-postgres -p 55432:5432 -e POSTGRES_PASSWORD=docker -d postgres

docker run -d --name docker-mariadb -p 33306:3306 -e MYSQL_ROOT_PASSWORD=docker -d mariadb
docker run -d --name mssql-express -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=Docker1234' -e 'MSSQL_PID=Express' -p 11433:1433 mcr.microsoft.com/mssql/server:2017-latest-ubuntu

export PGDATABASE=postgres
export PGUSER=postgres
export PGPASSWORD=docker
export PGPORT=5432
export PGHOST=localhost
