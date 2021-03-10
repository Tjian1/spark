package test;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Spark03_reduceBy {
    public static void main(String[] args) {
        // TODO: 2021/1/13 配置环境
        SparkConf sc = new SparkConf();
        sc.set("spark.testing.memory", "2147480000");
        JavaSparkContext jsc = new JavaSparkContext("local", "FirstAPP", sc);
        jsc.setLogLevel("ERROR");

        // TODO: 2021/1/13 建立RDD
        List<Integer> data = Arrays.asList(1, 2, 3, 4,5);
        JavaRDD<Integer> dataRDD = jsc.parallelize(data, 2);

        JavaRDD<String> sRDD = jsc.textFile("source/1.txt");

        List<Integer> data1=Arrays.asList(2,3,5,6,7);
        JavaRDD<Integer> dataRDD1 =jsc.parallelize(data1,2);


        List<Tuple2<Integer,String>> list =Arrays.asList(new Tuple2(1,"a"),new Tuple2(2,"b"),new Tuple2<>(3,"c"),new Tuple2<>(4,"c")
        ,new Tuple2(1,"a"),new Tuple2(2,"b"),new Tuple2<>(3,"c"),new Tuple2<>(4,"c"));
        JavaPairRDD<Integer, String> integerStringJavaPairRDD = jsc.parallelizePairs(list, 2);
        JavaPairRDD<Integer, String> integerStringJavaPairRDD1 = integerStringJavaPairRDD.reduceByKey((x, y) -> x + y);

        System.out.print(integerStringJavaPairRDD1.collect());
    }
}
