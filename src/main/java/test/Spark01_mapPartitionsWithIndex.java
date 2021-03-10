package test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Spark01_mapPartitionsWithIndex {
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

        //function2 第一个为index，第二个为入参，第三个为返回值
        JavaRDD<String> result = dataRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer integer, Iterator<Integer> integerIterator) throws Exception {
                List<String> list = new ArrayList<>();
                while (integerIterator.hasNext()) {
                    list.add(integer + ":" + integerIterator.next());
                }
                return list.iterator();
            }
        }, true);

        result.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
//        result.foreach(new VoidFunction<Integer>() {
//            @Override
//            public void call(Integer integer) throws Exception {
//                System.out.println(integer);
//            }
//        });
//        result.saveAsTextFile("result");
    }
}
