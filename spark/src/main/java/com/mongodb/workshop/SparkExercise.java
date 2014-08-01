package com.mongodb.workshop;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.bson.BSONObject;
import org.slf4j.Logger;
import scala.Tuple2;

import java.util.Date;

/**
 * MongoDB-Hadoop Workshop
 *
 * Spark job that reads users, movies, and ratings from MongoDB and
 * computes predicted ratings for all possible (user,movie) pairs using
 * the Spark MLlib built-in collaborative filter. The predicted ratings
 * are written back out to MongoDB.
 *
 */
public class SparkExercise
{
    public static void main(String[] args) {
        if(args.length < 2) {
            System.err.println("Usage: SparkExercise [mongodb db uri] [output collection name]");
            System.err.println("Note: assumes existence of ratings, users, movies collections");
            System.err.println("Example: SparkExercise mongodb://127.0.0.1:27017/movielens predictions");
            System.exit(-1);
        }

        // create SparkContext
        SparkConf conf = new SparkConf().setAppName("SparkExercise");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Logger log = sc.sc().log();

        // create base Configuration object
        Configuration inputConfig = new Configuration();
        inputConfig.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");

        // load users
        inputConfig.set("mongo.input.uri", args[0] + ".users");
        JavaRDD<Object> users = sc.newAPIHadoopRDD(inputConfig,
            MongoInputFormat.class, Object.class, BSONObject.class).map(
            new Function<Tuple2<Object, BSONObject>, Object>() {
                @Override
                public Object call(Tuple2<Object, BSONObject> doc) throws Exception {
                    return doc._2.get("userid");
                }
            }
        );

        log.warn("users = " + users.count());

        // load movies
        inputConfig.set("mongo.input.uri", args[0] + ".movies");
        JavaRDD<Object> movies = sc.newAPIHadoopRDD(inputConfig,
            MongoInputFormat.class, Object.class, BSONObject.class).map(
            new Function<Tuple2<Object, BSONObject>, Object>() {
                @Override
                public Object call(Tuple2<Object, BSONObject> doc) throws Exception {
                    return doc._2.get("movieid");
                }
            }
        );

        log.warn("movies = " + movies.count());

        // load ratings
        inputConfig.set("mongo.input.uri", args[0] + ".ratings");
        JavaRDD<Rating> ratings = sc.newAPIHadoopRDD(inputConfig,
            MongoInputFormat.class, Object.class, BSONObject.class).map(
            new Function<Tuple2<Object, BSONObject>, Rating>() {
                @Override
                public Rating call(Tuple2<Object, BSONObject> doc) throws Exception {
                    Integer userid = (Integer) doc._2.get("userid");
                    Integer movieid = (Integer) doc._2.get("movieid");
                    Number rating = (Number)doc._2.get("rating");
                    return new Rating(userid, movieid, rating.doubleValue());
                }
            }
        );

        log.warn("ratings = " + ratings.count());

        // train a collaborative filter model from existing ratings
        MatrixFactorizationModel model = ALS.train(ratings.rdd(), 10, 10, 0.01);

        // generate all possible (user,movie) pairings
        JavaPairRDD<Object,Object> allUsersMovies = users.cartesian(movies);

        log.warn("allUsersMovies = " + allUsersMovies.count());

        // predict ratings
        JavaRDD<Rating> predictedRatings = model.predict(allUsersMovies.rdd()).toJavaRDD();

        log.warn("predictedRatings = " + predictedRatings.count());

        // create BSON output RDD from predictions
        JavaPairRDD<Object,BSONObject> predictions = predictedRatings.mapToPair(
            new PairFunction<Rating, Object, BSONObject>() {
                @Override
                public Tuple2<Object, BSONObject> call(Rating rating) throws Exception {
                    DBObject doc = BasicDBObjectBuilder.start()
                        .add("userid", rating.user())
                        .add("movieid", rating.product())
                        .add("rating", rating.rating())
                        .add("timestamp", new Date())
                        .get();
                    // null key means an ObjectId will be generated on insert
                    return new Tuple2<Object, BSONObject>(null, doc);
                }
            }
        );

        log.warn(args[0] + "." + args[1] + " = " + predictions.count());

        // create output configuration
        Configuration outputConfig = new Configuration();
        outputConfig.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
        outputConfig.set("mongo.output.uri", args[0] + "." + args[1]);

        predictions.saveAsNewAPIHadoopFile("file:///not-applicable",
            Object.class, Object.class, MongoOutputFormat.class, outputConfig);
    }
}
