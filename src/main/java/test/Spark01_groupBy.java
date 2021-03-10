package test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Spark01_groupBy {
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


        JavaPairRDD<Integer, Iterable<Integer>> chanRdd = dataRDD.groupBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                int i = integer % 3;
                return i;
            }
        }, 3);

        JavaRDD<Object> mapRDD = chanRdd.map(new Function<Tuple2<Integer, Iterable<Integer>>, Object>() {

            @Override
            public Object call(Tuple2<Integer, Iterable<Integer>> integerIterableTuple2) throws Exception {
                return integerIterableTuple2._2;
            }
        });

        System.out.print(mapRDD.collect());
    }
}
