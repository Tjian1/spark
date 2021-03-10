package test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

public class Spark01_glom {
    public static void main(String[] args) {
        SparkConf sc=new SparkConf();
        sc.set("spark.testing.memory","2147480000");
        JavaSparkContext jsc=new JavaSparkContext("local","FirstAPP",sc);


        List<Integer> data= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> dataRDD = jsc.parallelize(data,3);

        JavaRDD<String> sRDD=jsc.textFile("source/1.txt");

        JavaRDD<List<Integer>> glomRDD = dataRDD.glom();

        System.out.print(glomRDD.collect());
    }
}
