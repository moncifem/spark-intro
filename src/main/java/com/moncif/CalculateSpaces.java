package com.moncif;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;;


import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.logging.Logger;


public class CalculateSpaces {
    public static void main(String[] args) {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss");
        String fileName = now.format(formatter);
        // 1. creer un SparkConf
        SparkConf conf = new SparkConf();
        conf.setAppName("ex1-spark");
        conf.setMaster("local[*]");
        conf.set("spark.eventLog.enabled", String.valueOf(true));
        conf.set("spark.eventLog.dir", "log_events");
        // 2. creer un SparkContext
        JavaSparkContext context = new JavaSparkContext(conf);
        context.setLogLevel("INFO");
        // 3. lire le fichier baskerville.txt
        JavaRDD<String> lines = context.textFile("baskerville.txt");
        // 4. compter les mots
        JavaRDD<String> words = lines
                .flatMap(line -> Arrays.asList(line.split("[\\p{Punct}\\s+]")).iterator())
                .filter(word -> word.length() > 0)
                .map(String::toLowerCase);
        JavaRDD<String> stopLines = context.textFile("stop_words_english.json");
        Function<String,String> removeLimits = line -> line.substring(1, line.length() -1);
        JavaRDD<String> stopWords = stopLines.map(removeLimits)
                .flatMap(line -> Arrays.asList(line.split(",")).iterator())
                .map(removeLimits);
        JavaPairRDD<String, Integer> counters = words.subtract(stopWords)
                .mapToPair(word -> new Tuple2<String, Integer>(word, 1));
        JavaPairRDD<String, Integer> results = counters.reduceByKey((v1, v2) -> v1 + v2);
        // 5. afficher le resultat
        //results.mapToPair(pair -> new Tuple2<Integer, String>(pair._2, pair._1))
          //      .sortByKey(false, 1);
                //.foreach(pair -> Logger.getGlobal().warning(pair._2 + " " + pair._1));
        // 6. sauvegarder le resultat
        //        .saveAsTextFile("output/" + fileName);
        JavaRDD<Tuple2<String, Integer>> output =
                results.map(pair -> new Tuple2<String, Integer>(pair._1, pair._2));
        output.sortBy(tuple -> tuple._2 ,false,1)
                .saveAsTextFile("output/" + fileName);
        Logger.getGlobal().info("File saved in output/" + fileName);
        try {
            System.out.println("Type Enter to finish: ");
            System.in.read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        context.close();
    }
}