package com.nielsen.spark.examples.wordcount;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

public final class Main {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws IOException {
    StopWatch sw = new StopWatch();
    sw.start();

    String input = args[0];
    String output = args[1];

    deleteFileIfExists(output);

    SparkConf conf = new SparkConf();
    conf.setAppName("Spark Java Wordcount");

    SparkContext sc = SparkSession.builder().config(conf).getOrCreate().sparkContext();
    JavaSparkContext jsc = new JavaSparkContext(sc);

    JavaRDD<String> lines = jsc.textFile(input);
    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
    JavaPairRDD<String, Integer> counts = ones.reduceByKey((a, b) -> a + b);

    counts.saveAsTextFile(output);

    sc.stop();

    sw.stop();

    System.out.println("Spark Java WordCount Duration: " + sw);
  }

  private static void deleteFileIfExists(String filename) throws IOException {
    Path path = new Path(filename);
    FileSystem fs = FileSystem.get(new Configuration());
    if (fs.isDirectory(path) && fs.exists(path))
      fs.delete(path, true);
  }
}