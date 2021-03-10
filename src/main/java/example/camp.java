package example;

import scala.Serializable;
import scala.Tuple3;

import java.util.Comparator;


//SortByKey需要自己创建一个新的比较器，并且需要序列化。
//compare返回三种结果  o1>o2 return 1 o1=o2 return 0 o1<o2 return -1
class camp implements Comparator<Tuple3<Integer,Integer,Integer>>, Serializable{

    @Override
    public int compare(Tuple3<Integer, Integer, Integer> o1, Tuple3<Integer, Integer, Integer> o2) {
        if (o1._1()>o2._1()) {
            return 1;
        } else if (o1._1() < o2._1()) {
            return -1;
        } else {
            if (o1._2() > o2._2()) {
                return 1;
            } else if (o1._2() < o2._2()) {
                return -1;
            } else {
                if (o1._3() > o2._3()) {
                    return 1;
                } else if(o1._3()<o2._3()){
                    return -1;
                }else{
                    return 0;
                }
            }
        }
    }
}

