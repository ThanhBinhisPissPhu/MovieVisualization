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

SELECT * FROM movies 
WHERE timestamp::TIMESTAMP > now() + interval '1 hour';

select * from movies where item_id = 4255;

SELECT *
FROM ratings
WHERE timestamp::timestamp > NOW() - INTERVAL '5 minutes'
LIMIT 10;

SELECT *
FROM reviews
WHERE timestamp::timestamp > NOW() - INTERVAL '5 minutes'
LIMIT 10;

movies: 84651
movies_genres: 141500
movies_actors: 379321
movies_directors: 87477


## spark submit
docker exec -it recsysbigdata-spark-master-1 bash

spark-submit --master spark://spark-master:7077 spark_stream.py

spark-submit --master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
spark_stream/spark_stream.py

## kafka
docker exec -it broker bash
kafka-topics --bootstrap-server localhost:9092 --list
kafka-topics --bootstrap-server broker:29092 --list

kafka-topics --bootstrap-server localhost:9092 --topic movies --describe
kafka-console-consumer --bootstrap-server localhost:9092 --topic movies --from-beginning --timeout-ms 5000 --max-messages 5
kafka-console-consumer --bootstrap-server localhost:9092 --topic ratings --from-beginning --timeout-ms 5000 --max-messages 5
kafka-console-consumer --bootstrap-server localhost:9092 --topic reviews --from-beginning --timeout-ms 5000 --max-messages 5
kafka-topics --bootstrap-server broker:9092 --delete --topic movies
kafka-topics --bootstrap-server broker:9092 --delete --topic ratings
kafka-topics --bootstrap-server broker:9092 --delete --topic reviews