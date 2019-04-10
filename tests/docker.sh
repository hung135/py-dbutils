#!/bin/bash

#python3 -m virtualenv .env
#source .env/bin/activate
#pip install -r requirements.txt


docker pull postgres
docker pull nginx
docker pull dpage/pgadmin4

#clean out stale containers
docker stop $(docker ps -aq)
docker rm $(docker ps -a -q)
#docker system prune -a -f
docker run --restart "unless-stopped" --name docker-postgres -p 5432:5432 -e POSTGRES_PASSWORD=docker -d postgres
#sleep 2 && docker run -it --rm -e "PGPASSWORD=docker" --link docker-postgres:postgres postgres psql -h postgres -U postgres


export PGDATABASE=postgres
export PGUSER=postgres
export PGPASSWORD=docker
export PGPORT=5432
export PGHOST=localhost
