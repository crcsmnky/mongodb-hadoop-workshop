REGISTER target/libs/mongo-hadoop-core-1.3.0.jar;
REGISTER target/libs/mongo-hadoop-pig-1.3.0.jar;
REGISTER target/libs/mongo-java-driver-2.12.3.jar;
REGISTER target/libs/datafu-1.2.0.jar;

ratings = LOAD 'mongodb://127.0.0.1:27017/mlsmall.ratings'
    USING com.mongodb.hadoop.pig.MongoLoader('movieid:int,rating:double');

grouped = GROUP ratings by movieid;

ratings_stats = FOREACH grouped {
    sum = SUM(ratings.rating);
    mean = AVG(ratings.rating);
    count = COUNT(ratings);
    median = datafu.pig.stats.StreamingMedian(ratings.rating);
    variance = datafu.pig.stats.VAR(ratings.rating);
    GENERATE group as movieid, mean as mean, sum as sum, count as count, median.quantile_0_5 as median, variance as variance;
}

STORE ratings_stats INTO 'mongodb://127.0.0.1:27017/mlsmall.pig_exercise3'
    USING com.mongodb.hadoop.pig.MongoStorage;
