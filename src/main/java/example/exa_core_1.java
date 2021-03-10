package example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

import java.lang.reflect.Array;
import java.util.*;

import static jdk.nashorn.internal.objects.NativeDebug.map;

public class exa_core_1 {

    public static void main(String[] args) {
        // TODO: 2021/1/13 配置环境
        SparkConf sc = new SparkConf();
        sc.set("spark.testing.memory", "2147480000");
        JavaSparkContext jsc = new JavaSparkContext("local", "FirstAPP", sc);
        jsc.setLogLevel("ERROR");

        //准备RDD
        JavaRDD<String> ZeroJavaRDD = jsc.textFile("source/user_visit_action.txt");
        JavaRDD<String[]> mapRDD = ZeroJavaRDD.map(x -> x.split(("_")));

        //点击量
        JavaRDD<String> clickRDD = mapRDD.map(new Function<String[], String>() {
            @Override
            public String call(String[] strings) throws Exception {
                return strings[6];
            }
        });

        JavaPairRDD<String, Integer> clickMapRDD = clickRDD.filter(x-> !x.equals("-1")).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {

                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> clickFinalRDD = clickMapRDD.reduceByKey((x, y) -> x + y);

        //下单量

        JavaRDD<String> orderRDD = mapRDD.map(new Function<String[], String>() {
            @Override
            public String call(String[] strings) throws Exception {
                return strings[8];
            }
        });


        JavaPairRDD<String, Integer> orderFinalRDD = orderRDD.filter(x -> !x.equals("null")).flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(String s) throws Exception {
                List<Tuple2<String, Integer>> list = new ArrayList<>();
                for (String ss : s.split(",")
                ) {
                    list.add(new Tuple2<>(ss, 1));
                }
                return list;
            }
        }).reduceByKey((x, y) -> x + y);

        //orderFinalRDD.collect().forEach(i->System.out.println(i));

        //支付量
        JavaRDD<String> applyRDD = mapRDD.map(new Function<String[], String>() {
            @Override
            public String call(String[] strings) throws Exception {
                return strings[10];
            }
        });

        JavaPairRDD<String,Integer> applyFinalRDD =applyRDD.filter(x->!x.equals("null")).flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(String s) throws Exception {
                List<Tuple2<String,Integer>> list =new ArrayList<>();
                for (String ss:s.split(",")
                     ) {
                    list.add(new Tuple2<>(ss,1));
                }
                return list;
            }
        }).reduceByKey((x,y)->x+y);
        //applyFinalRDD.collect().forEach(i->System.out.println(i));

        //合并
        JavaPairRDD<String, Tuple3<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>>> cogroupRDD = clickFinalRDD.cogroup(orderFinalRDD, applyFinalRDD);

        JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> resultRDD = cogroupRDD.mapValues(new Function<Tuple3<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>>, Tuple3<Integer, Integer, Integer>>() {
            @Override
            public Tuple3<Integer, Integer, Integer> call(Tuple3<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>> iterableIterableIterableTuple3) throws Exception {
                Integer clickC = 0, orderC = 0, applyC = 0;
                Iterator<Integer> iter1 = iterableIterableIterableTuple3._1().iterator();
                while (iter1.hasNext()) {
                    clickC =iter1.next();

                }
                Iterator<Integer> iter2 = iterableIterableIterableTuple3._2().iterator();
                while (iter2.hasNext()) {
                    orderC = iter2.next();
                }
                Iterator<Integer> iter3 = iterableIterableIterableTuple3._3().iterator();
                while (iter3.hasNext()) {
                    applyC =iter3.next();
                }
                return new Tuple3<>(clickC, orderC, applyC);
            }
        });

        //排序,JavaPairRDD只有sortByKey方法，需要调换KV位置
        JavaPairRDD<Tuple3<Integer, Integer, Integer>, String> swapRDD = resultRDD.mapToPair(x -> new Tuple2<>(x._2, x._1)).sortByKey(new camp(),false);
        swapRDD.mapToPair(x->new Tuple2<>(x._2,x._1)).take(10).forEach(System.out::println);

    }
}




