# MongoDB-Hadoop Workshop Exercises

MongoDB powers applications as an operational database and Hadoop delivers intelligence as with powerful analytical infrastructure. In this workshop we'll start by learning about how these technologies fit together with the MongoDB Connector for Hadoop. Then we'll cover reading/writing MongoDB data using MapReduce, Pig, Hive, and Spark. Finally, we'll discuss the broader data ecosystem and operational considerations.

## Data

Prior to running any of the exercises, load the sample dataset into MongoDB.

- Download [MongoDB](http://www.mongodb.org/downloads)
- [Install MongoDB](http://docs.mongodb.org/manual/installation/)
- Download the [MovieLens 10M](http://grouplens.org/datasets/movielens/) archive and unzip

Finally, load the dataset:

    $ python dataset/movielens.py [/path/to/movies.dat] [/path/to/ratings.dat]

For more information refer to the [dataset README](https://github.com/crcsmnky/mongodb-hadoop-workshop/tree/master/dataset).

## Exercises

Refer to the individual READMEs for steps on building and deploying each exercise.

- [MapReduce](https://github.com/crcsmnky/mongodb-hadoop-workshop/tree/master/mapreduce)
- [Pig](https://github.com/crcsmnky/mongodb-hadoop-workshop/tree/master/pig)
- [Hive](https://github.com/crcsmnky/mongodb-hadoop-workshop/tree/master/hive)
- [Spark](https://github.com/crcsmnky/mongodb-hadoop-workshop/tree/master/spark)