docker run \
-e POSTGRES_USER="root" \
-e POSTGRES_PASSWORD="root" \
-e POSTGRES_DB="movielens" \
-v $(pwd)/postgres:/var/lib/postgresql/data:rw \
-p 5432:5432 \
postgres:13

pgcli --host localhost --user root --port 5432 --dbname movielens

docker run -it \
-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
-e PGADMIN_DEFAULT_PASSWORD="root" \
-p 8080:80 \
dpage/pgadmin4


###### Network ######

docker network create pgnetwork


docker run -it \
-e POSTGRES_USER="root" \
-e POSTGRES_PASSWORD="root" \
-e POSTGRES_DB="movielens" \
-v $(pwd)/postgres:/var/lib/postgresql/data:rw \
-p 5432:5432 \
--network pgnetwork \
--name pgdatabase \
postgres:13


docker run -it \
-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
-e PGADMIN_DEFAULT_PASSWORD="root" \
-p 8080:80 \
--network pgnetwork \
--name pgadmin \
dpage/pgadmin4

## postgresql

docker exec -e PGPASSWORD=admin -it recsysbigdata-pgdatabase-1 psql -U admin -d movielens

select movies.title, COUNT(ratings.item_id) FROM (ratings JOIN movies ON movies.item_id = ratings.item_id) GROUP BY movies.title;
select * from ratings join movies on ratings.item_id = movies.item_id;

SELECT *
FROM ratings
WHERE timestamp::timestamp > NOW() - INTERVAL '5 minutes'
ORDER BY timestamp::timestamp DESC
LIMIT 10;

DELETE FROM ratings
WHERE (item_id, user_id) IN (
    (327, 997206),
    (377, 997206),
    (587, 997206),
    (588, 997206),
    (596, 997206),
    (637, 997206),
    (1022, 997206),
    (1100, 997206),
    (1177, 997206),
    (1223, 997206),
    (1225, 997206),
    (1274, 997206),
    (1288, 997206),
    (1379, 997206),
    (1441, 997206),
    (116, 667138),
    (377, 667138),
    (383, 667138),
    (588, 667138),
    (637, 667138),
    (1022, 667138),
    (1288, 667138),
    (1441, 667138),
    (1175, 577039),
    (1225, 577039));

SELECT *
FROM ratings
WHERE (item_id, user_id) IN (
    (327, 997206),
    (377, 997206),
    (587, 997206),
    (588, 997206),
    (596, 997206),
    (637, 997206),
    (1022, 997206),
    (1100, 997206),
    (1177, 997206),
    (1223, 997206),
    (1225, 997206),
    (1274, 997206),
    (1288, 997206),
    (1379, 997206),
    (1441, 997206),
    (116, 667138),
    (377, 667138),
    (383, 667138),
    (588, 667138),
    (637, 667138),
    (1022, 667138),
    (1288, 667138),
    (1441, 667138),
    (1175, 577039),
    (1225, 577039));

## spark submit
docker exec -it recsysbigdata-spark-master-1 bash
spark-submit --master spark://localhost:7077 spark_stream.py
spark-submit --master spark://localhost:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
spark_stream.py

## kafka
docker exec -it broker bash
kafka-topics --bootstrap-server localhost:9092 --list
kafka-topics --bootstrap-server localhost:9092 --topic movies --describe
kafka-console-consumer --bootstrap-server localhost:9092 --topic movies --from-beginning --timeout-ms 5000 --max-messages 5
kafka-console-consumer --bootstrap-server localhost:9092 --topic ratings --from-beginning --timeout-ms 5000 --max-messages 5
kafka-console-consumer --bootstrap-server localhost:9092 --topic reviews --from-beginning --timeout-ms 5000 --max-messages 5
kafka-topics --bootstrap-server broker:9092 --delete --topic movies
kafka-topics --bootstrap-server broker:9092 --delete --topic ratings
kafka-topics --bootstrap-server broker:9092 --delete --topic reviews