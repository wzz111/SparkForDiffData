import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TestDemo {

    private static String appName = "spark.demo";
    private static String master = "local[*]";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster(master);

        JavaSparkContext sc = new JavaSparkContext(conf);

        //构造数据源
        List<Integer> dataOne = Arrays.asList(1, 2, 2, 2, 3, 4, 5);
        List<Integer> dataTwo = Arrays.asList(1, 1, 1, 1, 2, 3, 4, 5);

        Iterator<Integer> iterator = dataOne.iterator();
    }
}
