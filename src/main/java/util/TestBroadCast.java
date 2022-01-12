package util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

public class TestBroadCast {

    private static String appName = "spark.demo";
    private static String master = "local[*]";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster(master);

        JavaSparkContext sc = new JavaSparkContext(conf);

        //构造数据源
        List<Integer> dataOne = Arrays.asList(1, 2, 3, 4, 5);
        List<Integer> dataTwo = Arrays.asList(1, 2, 3, 4, 5);

        //并行化创建rdd
        JavaRDD<Integer> rddOne = sc.parallelize(dataOne);
        JavaRDD<Integer> rddTwo = sc.parallelize(dataTwo);

        //广播变量
        Broadcast<List<Integer>> broadcast = sc.broadcast(rddTwo.collect());

        List<Integer> value = broadcast.getValue();

        rddOne.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(value.indexOf(integer));
            }
        });


        sc.close();
    }
}
