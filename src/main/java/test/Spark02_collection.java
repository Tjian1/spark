package test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.codehaus.janino.Java;

import java.util.Arrays;
import java.util.List;

public class Spark02_collection {
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

        // TODO: intersection 交集
        JavaRDD<Integer> intersectionRDD = dataRDD.intersection(dataRDD1);

        // TODO: union 并集
        JavaRDD<Integer> unionRDD = dataRDD.union(dataRDD1);

        // TODO: subtract 差集
        JavaRDD<Integer> subRDD = dataRDD.subtract(dataRDD1);

        // TODO: zip 集合
        JavaPairRDD<Integer,Integer> zipRDD = dataRDD.zip(dataRDD1);


        System.out.print(zipRDD.collect());
    }
}
