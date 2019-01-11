#!/usr/bin/env bash
set -e

docker run --name pgtest --rm -itd -p 5432:5432 \
    --health-cmd "psql -c 'SELECT 1' -U test -d test" --health-interval 1s \
    -v "$PWD/driver/src/test/resources/certdir/server/pg_hba.conf":/var/lib/postgresql/pg_hba.conf \
    -v "$PWD/driver/src/test/resources/certdir/server/root.crt":/var/lib/postgresql/root.crt \
    -v "$PWD/driver/src/test/resources/certdir/server/server.crt":/var/lib/postgresql/server.crt \
    -v "$PWD/driver/src/test/resources/certdir/server/server.key":/var/lib/postgresql/server.key \
    -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test -e POSTGRES_DB=test \
    postgres:${PGVERSION:-11}-alpine \
    --hba_file=/var/lib/postgresql/pg_hba.conf \
    --max-prepared-transactions=10 \
    --ssl=on \
    --ssl_ca_file=/var/lib/postgresql/root.crt \
    --ssl_cert_file=/var/lib/postgresql/server.crt \
    --ssl_key_file=/var/lib/postgresql/server.key

while [[ $ATTEMPT -le 19 && "`docker inspect -f {{.State.Health.Status}} pgtest`" != "healthy" ]];
    do sleep 1;
    ATTEMPT=$(( $ATTEMPT + 1 ))
done

docker exec pgtest psql -c "SHOW server_version;" -U test -d test
docker exec pgtest psql -c "CREATE EXTENSION hstore; CREATE EXTENSION citext;" -U test -d test
docker exec pgtest psql -c "CREATE DATABASE hostdb OWNER test;" -U test -d test
docker exec pgtest psql -c "CREATE EXTENSION sslinfo;" -U test -d hostdb
docker exec pgtest psql -c "CREATE DATABASE hostssldb OWNER test;" -U test -d test
docker exec pgtest psql -c "CREATE EXTENSION sslinfo;" -U test -d hostssldb
docker exec pgtest psql -c "CREATE DATABASE hostnossldb OWNER test;" -U test -d test
docker exec pgtest psql -c "CREATE EXTENSION sslinfo;" -U test -d hostnossldb
docker exec pgtest psql -c "CREATE DATABASE hostsslcertdb OWNER test;" -U test -d test
docker exec pgtest psql -c "CREATE EXTENSION sslinfo;" -U test -d hostsslcertdb
docker exec pgtest psql -c "CREATE DATABASE certdb OWNER test;" -U test -d test
docker exec pgtest psql -c "CREATE EXTENSION sslinfo;" -U test -d certdb
