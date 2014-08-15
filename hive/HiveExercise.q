-- Hive Exercise

-- Builds up a table of similar movie pairs ordered by count.

CREATE EXTERNAL TABLE ratings (userid INT, movieid INT, rating INT)
STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler'
TBLPROPERTIES('mongo.uri'='mongodb://127.0.0.1:27017/mlsmall.ratings');

CREATE TABLE ratings2 (userid INT, movieid INT);

INSERT OVERWRITE TABLE ratings2
  SELECT userid, movieid FROM ratings
  WHERE rating > 3;

CREATE TABLE movieid_pairs (movieid INT, rmovieid INT);

INSERT OVERWRITE TABLE movieid_pairs
  SELECT a.movieid, c.movieid
  FROM ratings2 a
  JOIN ratings2 b ON (a.movieid = b.movieid)
  JOIN ratings2 c ON (b.userid = c.userid);

CREATE TABLE movieid_pairs2 (movieid INT, rmovieid INT);

INSERT OVERWRITE TABLE movieid_pairs2
  SELECT movieid, rmovieid
  FROM movieid_pairs
  WHERE movieid != rmovieid;

CREATE EXTERNAL TABLE movieid_counts (movieid INT, rmovieid INT, count INT)
STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler'
TBLPROPERTIES('mongo.uri'='mongodb://127.0.0.1:27017/mlsmall.hive_exercise');

INSERT INTO TABLE movieid_counts
  SELECT movieid, rmovieid, COUNT(rmovieid)
  FROM movieid_pairs2
  GROUP BY movieid, rmovieid;

DROP TABLE ratings;
DROP TABLE ratings2;
DROP TABLE movied_pairs;
DROP TABLE movied_pairs2;
DROP TABLE movieid_counts;