# MapReduce Exercise

## Requirements

- Java 1.7
- Maven 3.x
- Hadoop 2.4.1

## Building

    $ mvn clean package
    $ cp target/lib/mongo-hadoop-core-1.3.0.jar /usr/local/hadoop/share/hadoop/common/lib/
    $ cp target/lib/mongo-java-driver-2.12.3.jar /usr/local/hadoop/share/hadoop/common/lib/

## Running

    $ hadoop jar target/mapreduce-1.0-SNAPSHOT.jar \
    com.mongodb.workshop.MapReduceExercise \
    mongodb://127.0.0.1:27017/movielens.ratings \
    mongodb://127.0.0.1:27017/movielens.movies \
    update=true