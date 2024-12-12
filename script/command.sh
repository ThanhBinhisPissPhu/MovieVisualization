## postgresql
docker exec -e PGPASSWORD=admin -it recsysbigdata-pgdatabase-1 psql -U admin -d movielens

select movies.title, COUNT(ratings.item_id) FROM (ratings JOIN movies ON movies.item_id = ratings.item_id) GROUP BY movies.title;
select * from ratings join movies on ratings.item_id = movies.item_id;

SELECT * FROM movies 
WHERE timestamp::TIMESTAMP > now() - interval '5 minutes';

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
spark_stream/spark_stream_unify.py

## kafka
docker exec -it broker bash
kafka-topics --bootstrap-server localhost:9092 --list
kafka-topics --bootstrap-server broker:29092 --list

kafka-topics --create --topic movies --bootstrap-server localhost:9092
kafka-topics --create --topic ratings --bootstrap-server localhost:9092
kafka-topics --create --topic reviews --bootstrap-server localhost:9092

kafka-run-class kafka.tools.GetOffsetShell --broker-list broker:29092 --topic movies --time -1
kafka-run-class kafka.tools.GetOffsetShell --broker-list broker:29092 --topic ratings --time -1
kafka-run-class kafka.tools.GetOffsetShell --broker-list broker:29092 --topic reviews --time -1

kafka-console-consumer --bootstrap-server localhost:9092 --topic movies --from-beginning --timeout-ms 5000 --max-messages 5
kafka-console-consumer --bootstrap-server localhost:9092 --topic ratings --from-beginning --timeout-ms 5000 --max-messages 5
kafka-console-consumer --bootstrap-server localhost:9092 --topic reviews --from-beginning --timeout-ms 5000 --max-messages 5

kafka-topics --bootstrap-server broker:9092 --delete --topic movies
kafka-topics --bootstrap-server broker:9092 --delete --topic ratings
kafka-topics --bootstrap-server broker:9092 --delete --topic reviews