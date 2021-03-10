package example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class exa_core2 {

    public static void main(String[] args) {
        // TODO: 2021/1/13 配置环境
        SparkConf sc = new SparkConf();
        sc.set("spark.testing.memory", "2147480000");
        JavaSparkContext jsc = new JavaSparkContext("local", "FirstAPP", sc);
        jsc.setLogLevel("ERROR");

        //准备RDD
        JavaRDD<String> ZeroJavaRDD = jsc.textFile("source/user_visit_action.txt");

        List<String> top10Ids=top10(ZeroJavaRDD);
        System.out.println(top10Ids.toString());

        //过滤得到点击数据
        JavaRDD<String> filterRDD = ZeroJavaRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] str = s.split("_");
                return (!str[6].equals("-1") & top10Ids.contains(str[6]));
            }
        });

        //map数据  ((品类id,Session id),cnt)
        JavaPairRDD<Tuple2<String, String>, Integer> usefulRDD = filterRDD.mapToPair(new PairFunction<String, Tuple2<String, String>, Integer>() {
            @Override
            public Tuple2<Tuple2<String, String>, Integer> call(String s) throws Exception {
                String[] str = s.split("_");
                return new Tuple2<>(new Tuple2<>(str[6], str[2]), 1);
            }
        }).reduceByKey((x,y)->x+y);

        //得到(品类id，（Session id，1))
        JavaPairRDD<String, Tuple2<String, Integer>> dataRDD = usefulRDD.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                return new Tuple2<>(tuple2IntegerTuple2._1._1, new Tuple2<>(tuple2IntegerTuple2._1._2, tuple2IntegerTuple2._2));
            }
        });

        //分组
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupRDD = dataRDD.groupByKey();
        
        //取top10
        groupRDD.mapValues(new Function<Iterable<Tuple2<String, Integer>>, Object>() {
            @Override
            public Object call(Iterable<Tuple2<String, Integer>> tuple2s) throws Exception {
                List<Tuple2<String, Integer>> list = new ArrayList<>();
                List<Tuple2<String, Integer>> list1 = new ArrayList<>();
                tuple2s.forEach(list::add);

                list.sort(new Comparator<Tuple2<String, Integer>>() {
                    @Override
                    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                        return o1._2.compareTo(o2._2);
                    }
                }.reversed());
                //subList无法序列化
                for(int i=0;i<10;i++){
                    list1.add(list.get(i));
                }
                return list1;
            }
        }).collect().forEach(System.out::println);


    }
    
    private  static List<String> top10 (JavaRDD<String> ZeroRDD){
        JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> dataRDD = ZeroRDD.flatMapToPair(new PairFlatMapFunction<String, String, Tuple3<Integer, Integer, Integer>>() {
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
          return resultRDD.mapToPair(x -> new Tuple2<>(x._2, x._1)).sortByKey(new camp(), false)
                .map(x -> x._2).take(10);


    }
}




