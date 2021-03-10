package test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Spark01_filter {
    public static void main(String[] args) {
        // TODO: 2021/1/13 配置环境
        SparkConf sc=new SparkConf();
        sc.set("spark.testing.memory","2147480000");
        JavaSparkContext jsc=new JavaSparkContext("local","FirstAPP",sc);

        // TODO: 2021/1/13 建立RDD
        List<Integer> data= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> dataRDD = jsc.parallelize(data,2);

        JavaRDD<String> sRDD=jsc.textFile("source/1.txt");


        JavaRDD<Integer> filterRDD = dataRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer % 2 == 0;
            }
        });

        JavaRDD<List<Integer>> glomRDD = filterRDD.glom();

        System.out.print(filterRDD.collect());
    }
}
