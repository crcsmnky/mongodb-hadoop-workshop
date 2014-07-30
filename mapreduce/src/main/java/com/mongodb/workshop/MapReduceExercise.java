package com.mongodb.workshop;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.io.IOException;

/**
 * MongoDB-Hadoop Workshop
 *
 * MapReduce job that reads movie ratings from MongoDB and
 * computes mean, median and std dev for each movie. Output
 * is written back to MongoDB. Output can be written as a
 * new collection or as updates to the movies collection.
 *
 * $ hadoop jar target/mapreduce-1.0-SNAPSHOT.jar
 *      com.mongodb.workshop.MapReduceExercise
 *      mongodb://127.0.0.1:27017/movielens.ratings
 *      mongodb://127.0.0.1:27017/movielens.movies
 *      update=true
 *
 */

public class MapReduceExercise
{
    public static class Map extends Mapper<Object, BSONObject, IntWritable, DoubleWritable> {
        @Override
        public void map(final Object key, final BSONObject doc, final Context context)
          throws IOException, InterruptedException {
            final int movieId = (Integer)doc.get("movieid");
            final double movieRating = (Double)doc.get("rating");

            context.write(new IntWritable(movieId), new DoubleWritable(movieRating));
        }
    }

    public static class Reduce extends Reducer<IntWritable, DoubleWritable, NullWritable, BSONWritable> {
        @Override
        public void reduce(final IntWritable key, final Iterable<DoubleWritable> values, final Context context)
          throws IOException, InterruptedException {
            DescriptiveStatistics stats = new DescriptiveStatistics();
            for(DoubleWritable rating : values) {
                stats.addValue(rating.get());
            }

            DBObject builder = new BasicDBObjectBuilder().start()
                .add("movieid", key.get())
                .add("mean", stats.getMean())
                .add("median", stats.getPercentile(50))
                .add("std", stats.getStandardDeviation())
                .add("count", stats.getN())
                .add("total", stats.getSum())
                .get();

            BSONWritable doc = new BSONWritable(builder);

            context.write(NullWritable.get(), doc);
        }
    }

    public static class ReduceUpdater extends Reducer<IntWritable, DoubleWritable, NullWritable, MongoUpdateWritable> {
        @Override
        public void reduce(final IntWritable key, final Iterable<DoubleWritable> values, final Context context)
          throws IOException, InterruptedException {
            DescriptiveStatistics stats = new DescriptiveStatistics();
            for(DoubleWritable rating : values) {
                stats.addValue(rating.get());
            }

            BasicBSONObject query = new BasicBSONObject("movieid", key.get());
            DBObject statValues = new BasicDBObjectBuilder().start()
                .add("mean", stats.getMean())
                .add("median", stats.getPercentile(50))
                .add("std", stats.getStandardDeviation())
                .add("count", stats.getN())
                .add("total", stats.getSum())
                .get();
            BasicBSONObject movieStats = new BasicBSONObject("stats", statValues);
            BasicBSONObject update = new BasicBSONObject("$set", movieStats);

            context.write(NullWritable.get(), new MongoUpdateWritable(query, update));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if(args.length < 3) {
            System.out.println("Usage: MapReduceExercise " +
                "[mongodb input uri] " +
                "[mongodb output uri] " +
                "update=[true or false]");

            System.out.println("Example: MapReduceExercise " +
                "mongodb://127.0.0.1:27017/movielens.ratings " +
                "mongodb://127.0.0.1:27017/movielens.ratings.stats update=false");

            System.out.println("Example: MapReduceExercise " +
                "mongodb://127.0.0.1:27017/movielens.ratings " +
                "mongodb://127.0.0.1:27017/movielens.movies update=true");

            System.exit(-1);
        }

        Class outputValueClass = BSONWritable.class;
        Class reducerClass = Reduce.class;

        if(args[2].equals("update=true")) {
            outputValueClass = MongoUpdateWritable.class;
            reducerClass = ReduceUpdater.class;
        }

        Configuration conf = new Configuration();

        // Set MongoDB-specific configuration items
        conf.setClass("mongo.job.mapper", Map.class, Mapper.class);
        conf.setClass("mongo.job.reducer", reducerClass, Reducer.class);

        conf.setClass("mongo.job.mapper.output.key", IntWritable.class, Object.class);
        conf.setClass("mongo.job.mapper.output.value", DoubleWritable.class, Object.class);

        conf.setClass("mongo.job.output.key", NullWritable.class, Object.class);
        conf.setClass("mongo.job.output.value", outputValueClass, Object.class);

        conf.set("mongo.input.uri",  args[0]);
        conf.set("mongo.output.uri", args[1]);

        Job job = Job.getInstance(conf);

        // Set Hadoop-specific job parameters
        job.setInputFormatClass(MongoInputFormat.class);
        job.setOutputFormatClass(MongoOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(outputValueClass);

        job.setMapperClass(Map.class);
        job.setReducerClass(reducerClass);

        job.setJarByClass(MapReduceExercise.class);

        job.submit();
    }
}
