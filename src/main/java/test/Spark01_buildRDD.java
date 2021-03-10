package test;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Int;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class Spark01_buildRDD {
    public static void main(String[] args) {
        SparkConf sc=new SparkConf();
        sc.set("spark.testing.memory","2147480000");
        JavaSparkContext jsc=new JavaSparkContext("local","FirstAPP",sc);


        List<Integer> data= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> dataRDD = jsc.parallelize(data);

        JavaRDD<String> sRDD=jsc.textFile("source/1.txt");
        sRDD.map(new Function<String,String[]>() {
            @Override
            public String[] call(String s) throws Exception {
                return s.split(" ");
            }
        });
        Function<Integer,Integer> test= i->i*2;
        JavaRDD<Integer> data1Rdd=dataRDD.map(new Function<Integer,Integer>(){

            @Override
            public Integer call(Integer integer) throws Exception {
                return null;
            }
        });
        for ( String[] a : sRDD.map(new Function<String,String[]>() {
            @Override
            public String[] call(String s) throws Exception {
                return s.split(" ");
            }
        }).collect()
             ) {
           for(String b:a){
               System.out.println(b);
           }
        };
       /* int total=dataRDD.reduce((a,b)->a+b);
        System.out.println(total);*/
    }
}
