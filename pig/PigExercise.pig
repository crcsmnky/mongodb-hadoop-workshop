-- register dependencies
REGISTER target/libs/mongo-hadoop-core-1.3.0.jar;
REGISTER target/libs/mongo-hadoop-pig-1.3.0.jar;
REGISTER target/libs/mongo-java-driver-2.12.3.jar;
REGISTER target/pig-1.0-SNAPSHOT.jar;

ratings = LOAD 'mongodb://127.0.0.1:27017/mlsmall.ratings'
    USING com.mongodb.hadoop.pig.MongoLoader('userid,movieid,rating');

DESCRIBE ratings;

ratings2 = FILTER ratings BY rating > 3;

ratings3 = FOREACH ratings2 GENERATE userid,movieid;

ratings4 = FOREACH ratings3 GENERATE *;

ratings_join_movieid = JOIN ratings3 BY movieid, ratings4 BY movieid;

ratings_join_movieid2 = FOREACH ratings_join_movieid
    GENERATE $0 AS userid, $1 AS movieid, $3 AS tmovieid;

ratings_join_userid = JOIN ratings_join_movieid2 BY userid, ratings3 BY userid;

ratings_join_userid2 = FILTER ratings_join_userid BY $1 != $4;

ratings_join_userid3 = FOREACH ratings_join_userid2
    GENERATE $1 AS movieid, $4 AS rmovieid;

ratings_count = GROUP ratings_join_userid3 BY movieid;

ratings_count_ordered = FOREACH ratings_count
    GENERATE group as movieid,
    com.mongodb.workshop.OrderByCountDesc($1) as ordered_movieids;

ratings_count_flat = FOREACH ratings_count_ordered
    GENERATE movieid, FLATTEN(ordered_movieids.right) AS rmovieid;

STORE ratings_count_flat INTO 'mongodb://127.0.0.1:27017/mlsmall.pigrecs'
    USING com.mongodb.hadoop.pig.MongoStorage();