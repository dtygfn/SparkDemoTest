package day02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;


/**
 * @Description java版的wordcount
 * @Author dtygfn
 * @Date: 2019/1/2 09:30
 */
public class JAVAWordCount {

    public static void main(String[] args) {
        //使用匿名内部类来实现wordcount
        //模板代码
        //spark运行模式
        //local模式，spark on yarn，
        SparkConf conf = new SparkConf();
        conf.setAppName("JavaWordCount");
        conf.setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //获取数据
        //调用hadoop的api来进行读数据
        JavaRDD<String> lines = jsc.textFile("hdfs://mini1:8020/wc");

        //切分
        //参数是输入的类型和输出的类型
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        //把单词生成元组
        JavaPairRDD<String, Integer> tup = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        //聚合
        JavaPairRDD<String, Integer> aggred = tup.reduceByKey(new Function2<Integer, Integer, Integer>() {
            //v1是之前结果的和
            //v2是当前数据的值
            //是将相同key的值的value进行聚合
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //spark中没有提供sortBy算子需要我们将数据对调一下再排序
        JavaPairRDD<Integer, String> swaped = aggred.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tup) throws Exception {
//                return new Tuple2<>(tup._2,tup._1);
                return tup.swap();
            }
        });

        //排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
        //把排序后的结果反转
        JavaPairRDD<String, Integer> res = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tup) throws Exception {
                return tup.swap();
            }
        });

        System.out.println(res.collect());
        jsc.stop();
    }
}
