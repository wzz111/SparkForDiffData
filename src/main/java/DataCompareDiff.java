import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import util.FileUtil;

import java.util.Arrays;
import java.util.List;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DataCompareDiff {

    private final static Logger logger = LoggerFactory.getLogger(DataCompareDiff.class);

    private static JavaSparkContext jsc = null;

    private static List<String> commOne;

    private static List<String> commTwo;

    private static List<String> diffOne;

    private static List<String> diffTwo;

    public static void main(String[] args) {
        // 1. 创建SparkConf对象, 设置Spark应用的配置信息
        SparkConf conf = new SparkConf()
                .setAppName("DataCompareDiffLocal")
                .setMaster("local");

        // 2. 创建JavaSparkContext对象
        // 在Spark中，SparkContext是Spark所有功能的一个入口，你无论是用java、scala，甚至是python编写
        // 都必须要有一个SparkContext，它的主要作用，包括初始化Spark应用程序所需的一些核心组件，包括
        // 调度器（DAGSchedule、TaskScheduler），还会去到Spark Master节点上进行注册，等等
        // 一句话，SparkContext，是Spark应用中，可以说是最最重要的一个对象
        // 但是呢，在Spark中，编写不同类型的Spark应用程序，使用的SparkContext是不同的，如果使用scala，
        // 使用的就是原生的SparkContext对象
        // 但是如果使用Java，那么就是JavaSparkContext对象
        jsc = new JavaSparkContext(conf);

        commOne = Lists.newArrayList();

        commTwo = Lists.newArrayList();

        diffOne = Lists.newArrayList();

        diffTwo = Lists.newArrayList();

        String filePathOne = "C:\\Users\\user\\Desktop\\秋招\\国泰\\项目\\Test for Java\\a.txt";
        String filePathTwo = "C:\\Users\\user\\Desktop\\秋招\\国泰\\项目\\Test for Java\\b.txt";

        String filePathThree = "C:\\Users\\user\\Desktop\\秋招\\国泰\\项目\\Demo\\Ref\\cash_fund_61.xml";

        dataCompareDiff(filePathOne, filePathThree, "UTF-8", false, false);

        saveFile(commOne, commTwo, diffOne, diffTwo);
    }

    private static void dataCompareDiff(String filePathOne, String filePathTwo
            , String encoding, boolean spaceSenFlag, boolean caseSenFlag) {
        JavaRDD<String> linesOne = FileUtil.fileRead(jsc, filePathOne, encoding);
        JavaRDD<String> linesTwo = FileUtil.fileRead(jsc, filePathTwo, encoding);

        JavaPairRDD<String, List<String>> transLinesOne = FileUtil.eachKeyCount(linesOne, spaceSenFlag, caseSenFlag, "1");
        JavaPairRDD<String, List<String>> transLinesTwo = FileUtil.eachKeyCount(linesTwo, spaceSenFlag, caseSenFlag, "2");

        JavaPairRDD<String, List<String>> union = transLinesOne.union(transLinesTwo);

        JavaPairRDD<String, Iterable<List<String>>> groupUnion = union.groupByKey();

        groupUnion.foreach(new VoidFunction<Tuple2<String, Iterable<List<String>>>>() {
            @Override
            public void call(Tuple2<String, Iterable<List<String>>> line) throws Exception {

                List<List<String>> lists = StreamSupport.stream(line._2.spliterator(), true).collect(Collectors.toList());

                int size = lists.size();

                List<String> listOne = lists.get(0);

                String[] eleOriOne = listOne.get(1).split("SplitOne");

                String fileName = listOne.get(2);

                switch (size) {
                    case 1:
                        if (fileName.equals("1")) {
                            diffOne.add(Arrays.toString(eleOriOne));
                        } else {
                            diffTwo.add(Arrays.toString(eleOriOne));
                        }

                        break;
                    case 2:
                        List<String> listTwo = lists.get(1);

                        String[] eleOriTwo = listTwo.get(1).split("SplitOne");

                        int countOne = Integer.parseInt(listOne.get(0));
                        int countTwo = Integer.parseInt(listTwo.get(0));

                        if (fileName.equals("2")) {
                            eleOriOne = listTwo.get(1).split("SplitOne");
                            eleOriTwo = listOne.get(1).split("SplitOne");

                            countOne = Integer.parseInt(listTwo.get(0));
                            countTwo = Integer.parseInt(listOne.get(0));
                        }

                        FileUtil.resStat(jsc, countOne, countTwo
                                , eleOriOne, eleOriTwo
                                , commOne, commTwo, diffOne, diffTwo);

                        break;
                }
            }
        });
    }

    private static void saveFile(List<String> commonOne
            , List<String> commonTwo
            , List<String> diffOne
            , List<String> diffTwo) {

        System.out.println("=============文件1相同元素=============");
        for (String s : commonOne) {
            System.out.println(s);
        }

        System.out.println("=============文件2相同元素=============");
        for (String s : commonTwo) {
            System.out.println(s);
        }

        System.out.println("=============文件1不同元素=============");
        for (String s : diffOne) {
            System.out.println(s);
        }

        System.out.println("=============文件2不同元素=============");
        for (String s : diffTwo) {
            System.out.println(s);
        }
    }
}