package example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class exa_core_2 {

    public static void main(String[] args) {
        // TODO: 2021/1/13 配置环境
        SparkConf sc = new SparkConf();
        sc.set("spark.testing.memory", "2147480000");
        JavaSparkContext jsc = new JavaSparkContext("local", "FirstAPP", sc);
        jsc.setLogLevel("ERROR");

        //准备RDD
        JavaRDD<String> ZeroJavaRDD = jsc.textFile("source/user_visit_action.txt");
        JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> dataRDD = ZeroJavaRDD.flatMapToPair(new PairFlatMapFunction<String, String, Tuple3<Integer, Integer, Integer>>() {
            @Override
            public Iterable<Tuple2<String, Tuple3<Integer, Integer, Integer>>> call(String s) throws Exception {
                List<Tuple2<String, Tuple3<Integer, Integer, Integer>>> list = new ArrayList<>();
                String[] str = s.split("_");
                if (!str[6].equals("-1")) {
                    list.add(new Tuple2<>(str[6], new Tuple3<>(1, 0, 0)));
                } else if (!str[8].equals("null")) {
                    String[] str1 = str[8].split(",");
                    for (String ss :
                            str1) {
                        list.add(new Tuple2<>(ss, new Tuple3<>(0, 1, 0)));
                    }
                } else if (!str[10].equals("null")) {
                    String[] str1 = str[10].split(",");
                    for (String ss :
                            str1) {
                        list.add(new Tuple2<>(ss, new Tuple3<>(0, 0, 1)));
                    }
                }
                return list;
            }
        });

        JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> resultRDD = dataRDD.reduceByKey((x, y) -> new Tuple3<>(x._1() + y._1(), x._2() + y._2(), x._3() + y._3()));


        //排序,JavaPairRDD只有sortByKey方法，需要调换KV位置
        JavaPairRDD<Tuple3<Integer, Integer, Integer>, String> swapRDD = resultRDD.mapToPair(x -> new Tuple2<>(x._2, x._1)).sortByKey(new camp(),false);
        swapRDD.mapToPair(x->new Tuple2<>(x._2,x._1)).take(10).forEach(System.out::println);

    }
}




