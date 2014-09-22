# Hive Exercise

## Requirements

- [Hive 0.10.0](http://hive.apache.org)
- [Hadoop 2.4.1](http://hadoop.apache.org)

## Building

    $ mvn clean package

## Setup

Copy the depedencies in `target/libs` to the Hive `lib` directory. For example (assuming Hive is located in `/usr/local/hive`):

    $ cp target/libs/mongo-*.jar /usr/local/hive/lib/

## Running

    $ hive HiveExercise.q
