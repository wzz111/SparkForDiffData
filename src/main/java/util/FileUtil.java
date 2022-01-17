package util;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class FileUtil {

    private final static Logger logger = LoggerFactory.getLogger(FileUtil.class);
    // 1. 创建SparkConf对象, 设置Spark应用的配置信息
    private static SparkConf conf = new SparkConf()
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
    private static JavaSparkContext jsc = new JavaSparkContext(conf);

    private static JavaRDD<String> commOne = jsc.emptyRDD();

    private static JavaRDD<String> commTwo = jsc.emptyRDD();

    private static JavaRDD<String> diffOne = jsc.emptyRDD();

    private static JavaRDD<String> diffTwo = jsc.emptyRDD();

    private static List<String> pathListOne = Lists.newArrayList();

    private static List<String> pathListTwo = Lists.newArrayList();

    private static List<String> fileNumListOne = Lists.newArrayList();

    private static List<String> fileNumListTwo = Lists.newArrayList();

    private static String fileOrDirone = null;

    private static String fileOrDirTwo = null;

    private static FileWriter fw = null;

    static {
        try {
            fw = new FileWriter(new File("Analysis.txt"));
        } catch (IOException e) {
            e.printStackTrace();

            System.out.println("文件初始化失败！！！");
        }
    }

    private static StringBuilder sb = null;

    private static JavaRDD<String> fileRead(String filePath, String encoding) {
        // 要针对输入源（hdfs文件、本地文件，等等），创建一个初始的RDD
        // 输入源中的数据会打散，分配到RDD的每个partition中，从而形成一个初始的分布式的数据集
        // 我们这里呢，因为是本地测试，所以呢，就是针对本地文件
        // SparkContext中，用于根据文件类型的输入源创建RDD的方法，叫做textFile()方法
        // 在Java中，创建的普通RDD，都叫做JavaRDD
        // 在这里呢，RDD中，有元素这种概念，如果是hdfs或者本地文件呢，创建的RDD，每一个元素就相当于
        // 是文件里的一行
        // 当数据集规模太大, 使用hadoopFile读取文件, 第一个类为输入格式类, 第二个类为key类, 第三个类为value类
        // TextInputFormat是系统默认的数据输入格式，可以将文本文件分块并逐行读入以便Map节点进行处理.
        // 读入一行时，所产生的主键Key就是当前行在整个文本文件中的字节偏移位置，而value就是该行的内容.
        // 它是系统默认的输入格式, 当用户程序不设置任何数据输入格式时，系统自动使用这个数据输入格式.
        // KeyValueTextInputFo0rmat是另一个常用的数据输入格式, 可将一个按照格式逐行存放的文本文件逐行读出, 并自动解
        // 析生成相应的key和value.
        // RDD映射为使用encoding编码的新生成的字符串
        return jsc.hadoopFile(filePath, TextInputFormat.class, LongWritable.class
                , Text.class).map(p -> new String(p._2.getBytes(), 0, p._2.getLength(), encoding));
    }

    /**
     * 读取一个目录下的文件
     *
     * @param dirPath
     * @return
     */
    private static JavaPairRDD<String, String> dirRead(String dirPath) {
        JavaNewHadoopRDD<LongWritable, Text> newHadoopRDD = (JavaNewHadoopRDD) jsc.newAPIHadoopFile(dirPath
                , org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class
                , LongWritable.class
                , Text.class, jsc.hadoopConfiguration());

        JavaRDD<String> javaRDD = newHadoopRDD.mapPartitionsWithInputSplit((inputSplit, iterator) -> {
            FileSplit fs = (FileSplit) inputSplit;

            String path = fs.getPath().toUri().getPath();

            ArrayList<String> list = Lists.newArrayList(path);

            return list.iterator();
        }, false);

        return javaRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                String[] split = line.split("/");

                return new Tuple2<String, String>(split[split.length - 1], line);
            }
        });
    }

    private static JavaRDD<String> dirReadAndCombine(String dirPath) {
        JavaNewHadoopRDD<LongWritable, Text> newHadoopRDD = (JavaNewHadoopRDD) jsc.newAPIHadoopFile(dirPath
                , org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat.class
                , LongWritable.class
                , Text.class, jsc.hadoopConfiguration());

        return newHadoopRDD.mapPartitionsWithInputSplit((inputSplit, iterator) -> {

            ArrayList<String> list = Lists.newArrayList();

            while (iterator.hasNext()) {
                list.add(iterator.next()._2.toString());
            }

            return list.iterator();
        }, false);
    }

    /**
     * 比较两个目录下文件的差异
     *
     * @param dirPathOne
     * @param dirPathTwo
     * @param encoding
     * @param spaceSenFlag
     * @param caseSenFlag
     */
    public static void dataCompareDiffTwoDirs(String dirPathOne, String dirPathTwo
            , String encoding, boolean spaceSenFlag, boolean caseSenFlag) {

        findSameFileName(dirPathOne, dirPathTwo, spaceSenFlag, caseSenFlag);

        StringBuilder sb = new StringBuilder();

        String[] split = dirPathOne.split("\\\\");

        fileOrDirone = sb.append("目录").append(split[split.length - 2]).toString();

        sb = new StringBuilder();

        split = dirPathTwo.split("\\\\");

        fileOrDirTwo = sb.append("目录").append(split[split.length - 2]).toString();

        for (int i = 0; i < fileNumListOne.size(); i++) {
            String filePathOne = pathListOne.get(i);
            String filePathTwo = pathListTwo.get(i);
            String fileNumOne = fileNumListOne.get(i);
            String fileNumTwo = fileNumListTwo.get(i);

            dataCompareDiffTwoFiles(filePathOne, filePathTwo, fileNumOne, fileNumTwo
                    , encoding, spaceSenFlag, caseSenFlag, true);
        }

        saveFile();
    }

    /**
     * 寻找两个目录下相同的文件
     *
     * @param dirPathOne
     * @param dirPathTwo
     * @param spaceSenFlag
     * @param caseSenFlag
     */
    private static void findSameFileName(String dirPathOne, String dirPathTwo
            , boolean spaceSenFlag, boolean caseSenFlag) {

        JavaPairRDD<String, String> pairRDDOne = FileUtil.dirRead(dirPathOne);
        JavaPairRDD<String, String> pairRDDTwo = FileUtil.dirRead(dirPathTwo);

        JavaPairRDD<String, Iterable<String>> group = pairRDDOne.union(pairRDDTwo).groupByKey();

        group.foreach(iterable -> {

            List<String> list = StreamSupport.stream(iterable._2.spliterator(), true).collect(Collectors.toList());

            if (list.size() == 2) {

                String pathOne = list.get(0);
                String pathTwo = list.get(1);

                pathListOne.add(pathOne);
                pathListTwo.add(pathTwo);

                String[] splitOne = pathOne.split("/");
                String[] splitTwo = pathTwo.split("/");

                StringBuilder sb = new StringBuilder();

                String fileNumOne = sb.append(splitOne[splitOne.length - 2]).append("/").append(splitOne[splitOne.length - 1]).toString();

                fileNumListOne.add(fileNumOne);

                sb = new StringBuilder();

                String fileNumTwo = sb.append(splitTwo[splitTwo.length - 2]).append("/").append(splitTwo[splitTwo.length - 1]).toString();

                fileNumListTwo.add(fileNumTwo);
            }
        });
    }

    /**
     * 比较两个目录的差异, 考虑文件合并
     *
     * @param dirPathOne
     * @param dirPathTwo
     * @param dirNumOne
     * @param dirNumTwo
     * @param spaceSenFlag
     * @param caseSenFlag
     */
    public static void dataCompareDiffTwoCombineDirs(String dirPathOne, String dirPathTwo
            , String dirNumOne, String dirNumTwo
            , boolean spaceSenFlag, boolean caseSenFlag) {

        JavaRDD<String> linesOne = FileUtil.dirReadAndCombine(dirPathOne);
        JavaRDD<String> linesTwo = FileUtil.dirReadAndCombine(dirPathTwo);

        StringBuilder sb = new StringBuilder();

        fileOrDirone = sb.append("目录").append(dirNumOne).toString();

        sb = new StringBuilder();

        fileOrDirTwo = sb.append("目录").append(dirNumTwo).toString();

        JavaPairRDD<String, List<String>> transLinesOne = eachKeyCount(linesOne, spaceSenFlag, caseSenFlag, dirNumOne);
        JavaPairRDD<String, List<String>> transLinesTwo = eachKeyCount(linesTwo, spaceSenFlag, caseSenFlag, dirNumTwo);

        JavaPairRDD<String, List<String>> union = transLinesOne.union(transLinesTwo);

        JavaPairRDD<String, Iterable<List<String>>> groupUnion = union.groupByKey();


        groupUnion.foreach(line -> {
            List<List<String>> lists = StreamSupport.stream(line._2.spliterator(), true).collect(Collectors.toList());

            int size = lists.size();

            List<String> listOne = lists.get(0);

            String[] eleOriOne = listOne.get(1).split("SplitOne");

            String fileName = listOne.get(2);

            switch (size) {
                case 1:
                    if (fileName.equals(dirNumOne)) {
                        diffOne = diffOne.union(jsc.parallelize(Arrays.asList(Arrays.toString(eleOriOne))));
                    } else {
                        diffTwo = diffTwo.union(jsc.parallelize(Arrays.asList(Arrays.toString(eleOriOne))));
                    }

                    break;
                case 2:
                    List<String> listTwo = lists.get(1);

                    String[] eleOriTwo = listTwo.get(1).split("SplitOne");

                    int countOne = Integer.parseInt(listOne.get(0));
                    int countTwo = Integer.parseInt(listTwo.get(0));

                    if (fileName.equals(dirNumTwo)) {
                        eleOriOne = listTwo.get(1).split("SplitOne");
                        eleOriTwo = listOne.get(1).split("SplitOne");

                        countOne = Integer.parseInt(listTwo.get(0));
                        countTwo = Integer.parseInt(listOne.get(0));
                    }

                    resStat(countOne, countTwo
                            , eleOriOne, eleOriTwo);

                    break;
            }
        });

        saveFile();
    }

    /**
     * 比较两个文件的差异, 不考虑文件合并
     *
     * @param filePathOne
     * @param filePathTwo
     * @param fileNumOne
     * @param fileNumTwo
     * @param encoding
     * @param spaceSenFlag
     * @param caseSenFlag
     */
    public static void dataCompareDiffTwoFiles(String filePathOne, String filePathTwo
            , String fileNumOne, String fileNumTwo
            , String encoding, boolean spaceSenFlag, boolean caseSenFlag
            , boolean callFlag) {

        JavaRDD<String> linesOne = fileRead(filePathOne, encoding);
        JavaRDD<String> linesTwo = fileRead(filePathTwo, encoding);

        if (StringUtils.isBlank(fileOrDirone)) {
            StringBuilder sb = new StringBuilder();

            fileOrDirone = sb.append("文件").append(fileNumOne).toString();

            sb = new StringBuilder();

            fileOrDirTwo = sb.append("文件").append(fileNumTwo).toString();
        }

        JavaPairRDD<String, List<String>> transLinesOne = eachKeyCount(linesOne, spaceSenFlag, caseSenFlag, fileNumOne);
        JavaPairRDD<String, List<String>> transLinesTwo = eachKeyCount(linesTwo, spaceSenFlag, caseSenFlag, fileNumTwo);

        JavaPairRDD<String, List<String>> union = transLinesOne.union(transLinesTwo);

        JavaPairRDD<String, Iterable<List<String>>> groupUnion = union.groupByKey();

        groupUnion.foreach(line -> {
            List<List<String>> lists = StreamSupport.stream(line._2.spliterator(), true).collect(Collectors.toList());

            int size = lists.size();

            List<String> listOne = lists.get(0);

            String[] eleOriOne = listOne.get(1).split("SplitOne");

            String fileName = listOne.get(2);

            switch (size) {
                case 1:
                    if (fileName.equals(fileNumOne)) {
                        diffOne = diffOne.union(jsc.parallelize(Arrays.asList(Arrays.toString(eleOriOne))));
                    } else {
                        diffTwo = diffTwo.union(jsc.parallelize(Arrays.asList(Arrays.toString(eleOriOne))));
                    }

                    break;
                case 2:
                    List<String> listTwo = lists.get(1);

                    String[] eleOriTwo = listTwo.get(1).split("SplitOne");

                    int countOne = Integer.parseInt(listOne.get(0));
                    int countTwo = Integer.parseInt(listTwo.get(0));

                    if (fileName.equals(fileNumTwo)) {
                        eleOriOne = listTwo.get(1).split("SplitOne");
                        eleOriTwo = listOne.get(1).split("SplitOne");

                        countOne = Integer.parseInt(listTwo.get(0));
                        countTwo = Integer.parseInt(listOne.get(0));
                    }

                    resStat(countOne, countTwo
                            , eleOriOne, eleOriTwo);

                    break;
            }
        });

        if (!callFlag) {
            saveFile();
        }
    }

    private static void saveFile() {
        try {
            sb = new StringBuilder();


            fw.write(sb.append("=================").append(fileOrDirone).append("相同元素").append("=================").append("\r\n").toString());

            commOne.sortBy(new Function<String, Integer>() {
                @Override
                public Integer call(String s) throws Exception {

                    return Integer.valueOf(s.split(",")[0].split("行")[0].split("第")[1]);
                }
            },true,3).foreach(line -> {
                sb = new StringBuilder();
                fw.write(sb.append(line).append("\r\n").toString());
            });

            fw.write("\r\n\r\n");

            sb = new StringBuilder();
            fw.write(sb.append("=================").append(fileOrDirTwo).append("相同元素").append("=================").append("\r\n").toString());

            commTwo.sortBy(new Function<String, Integer>() {
                @Override
                public Integer call(String s) throws Exception {

                    return Integer.valueOf(s.split(",")[0].split("行")[0].split("第")[1]);
                }
            },true,3).foreach(line -> {
                sb = new StringBuilder();
                fw.write(sb.append(line).append("\r\n").toString());
            });

            fw.write("\r\n\r\n");

            sb = new StringBuilder();
            fw.write(sb.append("=================").append(fileOrDirone).append("不同元素").append("=================").append("\r\n").toString());

            diffOne.sortBy(new Function<String, Integer>() {
                @Override
                public Integer call(String s) throws Exception {

                    return Integer.valueOf(s.split(",")[0].split("行")[0].split("第")[1]);
                }
            },true,3).foreach(line -> {
                sb = new StringBuilder();
                fw.write(sb.append(line).append("\r\n").toString());
            });

            fw.write("\r\n\r\n");

            sb = new StringBuilder();
            fw.write(sb.append("=================").append(fileOrDirTwo).append("不同元素").append("=================").append("\r\n").toString());

            diffTwo.sortBy(new Function<String, Integer>() {
                @Override
                public Integer call(String s) throws Exception {

                    return Integer.valueOf(s.split(",")[0].split("行")[0].split("第")[1]);
                }
            },true,3).foreach(line -> {
                sb = new StringBuilder();
                fw.write(sb.append(line).append("\r\n").toString());
            });

            fw.close();
        } catch (IOException e) {
            e.printStackTrace();

            System.out.println("写入文件出错！！！");
        }
    }

    private static JavaPairRDD<String, List<String>> eachKeyCount(JavaRDD<String> lines, boolean spaceSenFlag,
                                                                  boolean caseSenFlag, String fileNum) {

        // 接着，需要将每一个单词，映射为(单词, 1)的这种格式
        // 因为只有这样，后面才能根据单词作为key，来进行每个单词的出现次数的累加
        // mapToPair，其实就是将每个元素，映射为一个(v1,v2)这样的Tuple2类型的元素
        // 如果大家还记得scala里面讲的tuple，那么没错，这里的tuple2就是scala类型，包含了两个值
        // mapToPair这个算子，要求的是与PairFunction配合使用，第一个泛型参数代表了输入类型
        // 第二个和第三个泛型参数，代表的输出的Tuple2的第一个值和第二个值的类型
        // JavaPairRDD的两个泛型参数，分别代表了tuple元素的第一个值和第二个值的类型
        JavaPairRDD<String, List<String>> pairs = lines.mapToPair(

                new PairFunction<String, String, List<String>>() {

                    private static final long serialVersionUID = 1L;

                    private int count = 1;

                    @Override
                    public Tuple2<String, List<String>> call(String line) throws Exception {
                        String transLine = line;

                        if (!spaceSenFlag) {
                            transLine = line.replaceAll(" ", "");
                        }

                        if (!caseSenFlag) {
                            transLine = transLine.toLowerCase();
                        }

                        StringBuilder sb = new StringBuilder();

                        String[] split = fileNum.split("\\.");

                        if (split.length == 1) {
                            sb.append("目录");
                        } else {
                            sb.append("文件");
                        }

                        return new Tuple2<String, List<String>>(transLine, Arrays.asList("1", sb.append(fileNum).append("第").append(String.valueOf(count++)).append("行").append(":").append(line).toString(), fileNum));
                    }
                });

        // 接着，需要以单词作为key，统计每个单词出现的次数
        // 这里要使用reduceByKey这个算子，对每个key对应的value，都进行reduce操作
        // 比如JavaPairRDD中有几个元素，分别为(hello, 1) (hello, 1) (hello, 1) (world, 1)
        // reduce操作，相当于是把第一个值和第二个值进行计算，然后再将结果与第三个值进行计算
        // 比如这里的hello，那么就相当于是，首先是1 + 1 = 2，然后再将2 + 1 = 3
        // 最后返回的JavaPairRDD中的元素，也是tuple，但是第一个值就是每个key，第二个值就是key的value
        // reduce之后的结果，相当于就是每个单词出现的次数

        return pairs.reduceByKey(

                new Function2<List<String>, List<String>, List<String>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public List<String> call(List<String> V1, List<String> V2) throws Exception {
                        String eleOne = V1.get(1);
                        String eleTwo = V2.get(1);

                        Integer countOne = Integer.parseInt(V1.get(0));
                        Integer countTwo = Integer.parseInt(V2.get(0));

                        String count = String.valueOf(countOne + countTwo);

                        StringBuilder sb = new StringBuilder();

                        sb.append(eleOne).append("SplitOne").append(eleTwo);

                        return Arrays.asList(count, sb.toString(), V1.get(2));
                    }
                });
    }

    private static void resStat(int countOne, int countTwo
            , String[] eleOriOne
            , String[] eleOriTwo) {

        int flag = Integer.compare(countOne, countTwo);

        switch (flag) {
            case 1:
                diffOne = diffOne.union(jsc.parallelize(Arrays.asList(Arrays.toString(Arrays.copyOfRange(eleOriOne, countTwo, countOne)))));

                commOne = commOne.union(jsc.parallelize(Arrays.asList(Arrays.toString(Arrays.copyOfRange(eleOriOne, 0, countTwo)))));

                commTwo = commTwo.union(jsc.parallelize(Arrays.asList(Arrays.toString(Arrays.copyOfRange(eleOriTwo, 0, countTwo)))));

                break;
            case -1:
                diffTwo = diffTwo.union(jsc.parallelize(Arrays.asList(Arrays.toString(Arrays.copyOfRange(eleOriTwo, countOne, countTwo)))));

                commOne = commOne.union(jsc.parallelize(Arrays.asList(Arrays.toString(Arrays.copyOfRange(eleOriOne, 0, countOne)))));

                commTwo = commTwo.union(jsc.parallelize(Arrays.asList(Arrays.toString(Arrays.copyOfRange(eleOriTwo, 0, countOne)))));

                break;
            case 0:
                commOne = commOne.union(jsc.parallelize(Arrays.asList(Arrays.toString(eleOriOne))));

                commTwo = commTwo.union(jsc.parallelize(Arrays.asList(Arrays.toString(eleOriTwo))));

                break;
        }
    }

    // 优化:分区器
    private static JavaPairRDD<String, List<String>> partitions(JavaPairRDD<String, List<String>> union, int num) {

        return union.partitionBy(new Partitioner() {

            private int partition = num;

            @Override
            public int numPartitions() {
                return partition;
            }

            @Override
            public int getPartition(Object key) {
                return key.hashCode() % partition;
            }
        });
    }
}
