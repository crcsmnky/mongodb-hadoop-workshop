# Spark Exercise

## Requirements

- [Java SE 1.7](http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html)
- [Maven](http://maven.apache.org) (latest)
- [Spark 1.0 for Hadoop 2](http://spark.apache.org/downloads.html)

## Building

    $ mvn clean package

## Running

    $ /path/to/spark/bin/spark-submit --master local \
    --driver-memory 2G --executor-memory 2G \
    --class com.mongodb.workshop.SparkExercise target/spark-1.0-SNAPSHOT.jar \
    --jars target/lib/mongo-hadoop-core-1.3.0.jar,target/lib/mongo-java-driver-2.12.3.jar \
    mongodb://127.0.0.1:27017/movielens predictions

