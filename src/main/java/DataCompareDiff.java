import com.clearspring.analytics.util.Lists;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
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

    private static List<String> commonOne;

    private static List<String> commonTwo;

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

        commonOne = Lists.newArrayList();

        commonTwo = Lists.newArrayList();

        diffOne = Lists.newArrayList();

        diffTwo = Lists.newArrayList();

        String filePathOne = "C:\\Users\\user\\Desktop\\秋招\\国泰\\项目\\Test for Java\\a.txt";
        String filePathTwo = "C:\\Users\\user\\Desktop\\秋招\\国泰\\项目\\Test for Java\\b.txt";

        dataCompareDiffTwo(filePathOne, filePathTwo, "UTF-8", false, false);

        System.out.println("=========文件1相同元素===========");
        for (String s : commonOne) {
            System.out.println(s);
        }

        System.out.println("=========文件2相同元素===========");
        for (String s : commonTwo) {
            System.out.println(s);
        }

        System.out.println("=========文件1不同元素===========");
        for (String s : diffOne) {
            System.out.println(s);
        }

        System.out.println("=========文件2不同元素===========");
        for (String s : diffTwo) {
            System.out.println(s);
        }
    }

    private static void dataCompareDiffOne(String filePathOne, String filePathTwo
            , String encoding, boolean spaceSenFlag, boolean caseSenFlag) {
        JavaRDD<String> linesOne = FileUtil.fileRead(jsc, filePathOne, encoding);
        JavaRDD<String> linesTwo = FileUtil.fileRead(jsc, filePathTwo, encoding);

        JavaPairRDD<String, List<String>> transLinesOne = FileUtil.eachKeyCount(linesOne, spaceSenFlag, caseSenFlag, "1");
        JavaPairRDD<String, List<String>> transLinesTwo = FileUtil.eachKeyCount(linesTwo, spaceSenFlag, caseSenFlag, "2");

        Broadcast<List<Tuple2<String, List<String>>>> broOne = jsc.broadcast(transLinesOne.collect());
        Broadcast<List<Tuple2<String, List<String>>>> broTwo = jsc.broadcast(transLinesTwo.collect());

        List<Tuple2<String, List<String>>> valueOne = broOne.value();
        List<Tuple2<String, List<String>>> valueTwo = broTwo.value();

        transLinesOne.foreach(new VoidFunction<Tuple2<String, List<String>>>() {

            @Override
            public void call(Tuple2<String, List<String>> line) throws Exception {
                String keyOne = line._1;

                int countOne = Integer.parseInt(line._2.get(0));

                String[] oriKeyArrOne = line._2.get(1).split("_");

                List<Tuple2<String, List<String>>> find = valueTwo.stream().filter(s ->
                        s._1.equals(keyOne)
                ).collect(Collectors.toList());

                if (find == null || find.size() == 0) {
                    diffOne.add(
                            Arrays.toString(oriKeyArrOne)
                    );

                    return;
                }

                Tuple2<String, List<String>> tmp = find.get(0);

                int countTwo = Integer.parseInt(tmp._2.get(0));

                String[] oriKeyArrTwo = tmp._2.get(1).split("_");

                FileUtil.resStat(countOne, countTwo, oriKeyArrOne, oriKeyArrTwo
                        , commonOne, commonTwo, diffOne, diffTwo);
            }
        });

        transLinesTwo.foreach(new VoidFunction<Tuple2<String, List<String>>>() {

            @Override
            public void call(Tuple2<String, List<String>> line) throws Exception {
                String keyTwo = line._1;

                int countTwo = Integer.parseInt(line._2.get(0));

                String[] oriKeyArrTwo = line._2.get(1).split("_");

                List<Tuple2<String, List<String>>> find = valueOne.stream().filter(s ->
                        s._1.equals(keyTwo)
                ).collect(Collectors.toList());

                if (find == null || find.size() == 0) {
                    diffTwo.add(
                            Arrays.toString(oriKeyArrTwo)
                    );

                    return;
                }
            }
        });
    }

    private static void dataCompareDiffTwo(String filePathOne, String filePathTwo
            , String encoding, boolean spaceSenFlag, boolean caseSenFlag) {
        JavaRDD<String> linesOne = FileUtil.fileRead(jsc, filePathOne, encoding);
        JavaRDD<String> linesTwo = FileUtil.fileRead(jsc, filePathTwo, encoding);

        JavaPairRDD<String, List<String>> transLinesOne = FileUtil.eachKeyCount(linesOne, spaceSenFlag, caseSenFlag, "1");
        JavaPairRDD<String, List<String>> transLinesTwo = FileUtil.eachKeyCount(linesTwo, spaceSenFlag, caseSenFlag, "2");

        JavaPairRDD<String, List<String>> union = transLinesOne.union(transLinesTwo);

        // 优化:分区器
        union = union.partitionBy(new Partitioner() {

            private int partition = 5;

            @Override
            public int numPartitions() {
                return partition;
            }

            @Override
            public int getPartition(Object key) {
                return key.hashCode() % partition;
            }
        });

        JavaPairRDD<String, Iterable<List<String>>> groupUnion = union.groupByKey();

        groupUnion.foreach(new VoidFunction<Tuple2<String, Iterable<List<String>>>>() {
            @Override
            public void call(Tuple2<String, Iterable<List<String>>> line) throws Exception {
                List<List<String>> lists = StreamSupport.stream(line._2.spliterator(), false).collect(Collectors.toList());

                int size = lists.size();

                List<String> list = lists.get(0);

                String fileName = list.get(2);

                switch (size) {
                    case 1:
                        String[] oriArr = list.get(1).split("_");

                        if (fileName.equals("1")) {
                            diffOne.add(
                                    Arrays.toString(oriArr)
                            );
                        } else {
                            diffTwo.add(
                                    Arrays.toString(oriArr)
                            );
                        }

                        break;
                    case 2:
                        String[] oriArrOne = lists.get(0).get(1).split("_");
                        String[] oriArrTwo = lists.get(1).get(1).split("_");

                        int countOne = Integer.parseInt(lists.get(0).get(0));
                        int countTwo = Integer.parseInt(lists.get(1).get(0));

                        if (fileName.equals("2")) {
                            oriArrOne = lists.get(1).get(2).split("_");
                            oriArrTwo = lists.get(0).get(2).split("_");

                            countOne = Integer.parseInt(lists.get(1).get(0));
                            countTwo = Integer.parseInt(lists.get(0).get(0));
                        }

                        FileUtil.resStat(countOne, countTwo, oriArrOne, oriArrTwo
                                , commonOne, commonTwo, diffOne, diffTwo);

                        break;
                }
            }
        });
    }
}