package test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Spark01_flatMap {
    public static void main(String[] args) {
        // TODO: 2021/1/13 配置环境
        SparkConf sc=new SparkConf();
        sc.set("spark.testing.memory","2147480000");
        JavaSparkContext jsc=new JavaSparkContext("local","FirstAPP",sc);
        jsc.setLogLevel("ERROR");

        // TODO: 2021/1/13 建立RDD
        List<Integer> data= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> dataRDD = jsc.parallelize(data,2);

        JavaRDD<String> sRDD=jsc.textFile("source/1.txt");

        JavaRDD<Integer> result = dataRDD.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public Iterable<Integer> call(Integer integer) throws Exception {
                List<Integer> list = new ArrayList<>();
                for (int i = 0; i <= integer; i++) {
                    list.add(i);
                }
                return list;
            }
        });

        System.out.println(result.collect());
    }
}
