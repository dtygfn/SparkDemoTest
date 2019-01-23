package day02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @Description lamda表达式开发wordcount
 * @Author dtygfn
 * @Date: 2019/1/2 10:12
 */
public class JavaLamdaWC {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("javaLamdWC").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("hdfs://mini1:8020/wc");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> tup = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> aggred = tup.reduceByKey((v1, v2) -> v1 + v2);
        JavaPairRDD<Integer, String> swaped = aggred.mapToPair(tuple -> tuple.swap());
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
        JavaPairRDD<String, Integer> res = sorted.mapToPair(sort -> sort.swap());
//        System.out.println(res.collect());
//        res.saveAsTextFile("hdfs://mini1:8020/out/2019-1-2");
//        jsc.stop();
        System.out.println(res.toDebugString());
    }
}
