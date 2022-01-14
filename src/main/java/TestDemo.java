import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;


import java.util.Arrays;
import java.util.List;

public class TestDemo {

    private static String appName = "spark.demo";
    private static String master = "local[*]";

    public static void main(String[] args) {

        StringBuilder sb = new StringBuilder();

        sb.append("1").append("2");

        System.out.println(sb.toString());
    }
}
