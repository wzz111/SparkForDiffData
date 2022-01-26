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
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class FileUtil {

    private final static Logger logger = LoggerFactory.getLogger(FileUtil.class);
    // 1. 创建SparkConf对象, 设置Spark应用的配置信息
    // 开启8个线程
    // 设置每个Stage的默认任务数量
    private static SparkConf conf = new SparkConf()
            .setAppName("DataCompareDiffLocal")
            .setMaster("local[1]")
            .set("spark.num.executors", "10")
            .set("spark.executor.cores", "3")
            .set("spark.executor.memory", "8g")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.driver.memory", "5g")
            .set("spark.storage.memoryFraction", "0.45")
            .set("spark.shuffle.memoryFraction", "0.4")
            .set("spark.shuffle.manager", "hash")
            .set("spark.shuffle.consolidateFiles", "true")
            .set("spark.reducer.maxSizeInFlight", "256m");

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

    private static List<String> middRddCommOne = Lists.newArrayList();

    private static List<String> middRddCommTwo = Lists.newArrayList();

    private static List<String> middRddDiffOne = Lists.newArrayList();

    private static List<String> middRddDiffTwo = Lists.newArrayList();

    private static Integer limit = 100000;

    private static String fileOrDirone = null;

    private static String fileOrDirTwo = null;

    private static FileWriter fwCommOne = null;

    private static FileWriter fwCommTwo = null;

    private static FileWriter fwDiffOne = null;

    private static FileWriter fwDiffTwo = null;

    private static StringBuilder sb = null;

    private static JavaPairRDD<String, List<String>> fileRead(String filePath, String fileNum, String care, String encoding
            , boolean spaceSenFlag, boolean caseSenFlag, boolean callFlag) {
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

        JavaPairRDD<String, List<String>> pairRDD = jsc.hadoopFile(filePath, TextInputFormat.class, LongWritable.class
                , Text.class).mapToPair(new PairFunction<Tuple2<LongWritable, Text>, String, List<String>>() {

            private int count = 1;

            // 接着，需要将每一个单词，映射为(单词, 1)的这种格式
            // 因为只有这样，后面才能根据单词作为key，来进行每个单词的出现次数的累加
            // mapToPair，其实就是将每个元素，映射为一个(v1,v2)这样的Tuple2类型的元素
            // 如果大家还记得scala里面讲的tuple，那么没错，这里的tuple2就是scala类型，包含了两个值
            // mapToPair这个算子，要求的是与PairFunction配合使用，第一个泛型参数代表了输入类型
            // 第二个和第三个泛型参数，代表的输出的Tuple2的第一个值和第二个值的类型
            // JavaPairRDD的两个泛型参数，分别代表了tuple元素的第一个值和第二个值的类型
            @Override
            public Tuple2<String, List<String>> call(Tuple2<LongWritable, Text> p) throws Exception {
                String line = new String(p._2.getBytes(), 0, p._2.getLength(), encoding);

                String transLine = line;

                if (!spaceSenFlag) {
                    transLine = line.replaceAll(" ", "");
                }

                if (!caseSenFlag) {
                    transLine = transLine.toLowerCase();
                }

                StringBuilder sb = new StringBuilder();

                if (callFlag) {
                    sb.append("文件").append(fileNum);
                }

                return new Tuple2<String, List<String>>(transLine, Arrays.asList("1", sb.append(String.valueOf(count++)).append(":").append(line).toString(), fileNum));
            }
        });

        if (!care.equals("")) {
            Broadcast<String> broadCare = jsc.broadcast(care);

            pairRDD = pairRDD.filter(new Function<Tuple2<String, List<String>>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, List<String>> line) throws Exception {
                    return line._1.indexOf(broadCare.getValue()) != -1;
                }
            });
        }

        return pairRDD;
    }

    /**
     * 读取一个目录下的文件
     *
     * @param dirPath
     * @return
     */
    private static JavaPairRDD<String, String> dirRead(String dirPath, String care) {
        JavaNewHadoopRDD<LongWritable, Text> newHadoopRDD = (JavaNewHadoopRDD) jsc.newAPIHadoopFile(dirPath
                , org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class
                , LongWritable.class
                , Text.class, jsc.hadoopConfiguration());

        JavaRDD<String> javaRDD = newHadoopRDD.mapPartitionsWithInputSplit((inputSplit, iterator) -> {
            FileSplit fs = (FileSplit) inputSplit;

            String path = fs.getPath().toUri().getPath();

            ArrayList<String> list = Lists.newArrayList();

            String[] split = path.split("\\.")[0].split("/");

            if (split[split.length - 1].indexOf(care) != -1) {
                list.add(path);
            }

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

    private static JavaPairRDD<String, List<String>> dirReadAndCombine(String dirPath, String dirNum, String care, String encoding, boolean spaceSenFlag, boolean caseSenFlag) {
        JavaNewHadoopRDD<LongWritable, Text> newHadoopRDD = (JavaNewHadoopRDD) jsc.newAPIHadoopFile(dirPath
                , org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat.class
                , LongWritable.class
                , Text.class, jsc.hadoopConfiguration());

        return newHadoopRDD.mapToPair(new PairFunction<Tuple2<LongWritable, Text>, String, List<String>>() {

            @Override
            public Tuple2<String, List<String>> call(Tuple2<LongWritable, Text> p) throws Exception {
                System.out.println(p._1);

                String line = new String(p._2.getBytes(), 0, p._2.getLength(), encoding);

                String transLine = line;

                if (!spaceSenFlag) {
                    transLine = line.replaceAll(" ", "");
                }

                if (!caseSenFlag) {
                    transLine = transLine.toLowerCase();
                }

                return new Tuple2<String, List<String>>(transLine, Arrays.asList("1", line, dirNum));
            }
        }).filter(new Function<Tuple2<String, List<String>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, List<String>> line) throws Exception {
                return line._1.indexOf(care) != -1;
            }
        });
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
            , String fileNameCare, String fileContentCare, String encoding, boolean spaceSenFlag, boolean caseSenFlag) {

        findSameFileName(dirPathOne, dirPathTwo, fileNameCare, spaceSenFlag, caseSenFlag);

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
                    , fileContentCare, encoding, spaceSenFlag, caseSenFlag, true);
        }

        saveFile(false);
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
            , String care, boolean spaceSenFlag, boolean caseSenFlag) {

        JavaPairRDD<String, String> pairRDDOne = FileUtil.dirRead(dirPathOne, care);
        JavaPairRDD<String, String> pairRDDTwo = FileUtil.dirRead(dirPathTwo, care);

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

                String fileNumOne = sb.append(splitOne[splitOne.length - 1]).toString();

                fileNumListOne.add(fileNumOne);

                sb = new StringBuilder();

                String fileNumTwo = sb.append(splitTwo[splitTwo.length - 1]).toString();

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
            , String care, String encoding, boolean spaceSenFlag, boolean caseSenFlag) {

        JavaPairRDD<String, List<String>> pairsOne = FileUtil.dirReadAndCombine(dirPathOne, dirNumOne, care, encoding, spaceSenFlag, caseSenFlag);
        JavaPairRDD<String, List<String>> pairsTwo = FileUtil.dirReadAndCombine(dirPathTwo, dirNumOne, care, encoding, spaceSenFlag, caseSenFlag);

        StringBuilder sb = new StringBuilder();

        fileOrDirone = sb.append("目录").append(dirNumOne).toString();

        sb = new StringBuilder();

        fileOrDirTwo = sb.append("目录").append(dirNumTwo).toString();

        conf.setMaster("local[4]")
                .set("spark.default.parallelism", "60");

        if (StringUtils.isBlank(fileOrDirone)) {
            sb = new StringBuilder();

            fileOrDirone = sb.append("文件").append(dirNumOne).toString();

            sb = new StringBuilder();

            fileOrDirTwo = sb.append("文件").append(dirNumTwo).toString();
        }

        JavaPairRDD<String, List<String>> union = pairsOne.union(pairsTwo);

        JavaPairRDD<String, ArrayList<List<String>>> groupUnion = union.aggregateByKey(
                new ArrayList<List<String>>()
                , (ArrayList<List<String>> begin, List<String> V1) -> {
                    ArrayList<List<String>> temp = new ArrayList<List<String>>();

                    if (begin.size() == 0) {
                        temp.add(V1);
                    } else {
                        List<String> V2 = begin.get(0);

                        combine(temp, V1, V2);
                    }

                    return temp;
                }
                , (ArrayList<List<String>> valOne, ArrayList<List<String>> valTwo) -> {

                    ArrayList<List<String>> temp = new ArrayList<List<String>>();

                    List<String> V1 = valOne.get(0);
                    List<String> V2 = valTwo.get(0);

                    String fileNameOne = V1.get(2);
                    String fileNameTwo = V2.get(2);

                    if (fileNameOne.equals(fileNameTwo)) {
                        combine(temp, V1, V2);
                    } else {
                        temp.add(V1);
                        temp.add(V2);
                    }

                    return temp;
                });

        groupUnion.foreachPartition(new VoidFunction<Iterator<Tuple2<String, ArrayList<List<String>>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, ArrayList<List<String>>>> iterator) throws Exception {
                while (iterator.hasNext()) {
                    List<List<String>> lists = iterator.next()._2;

                    int size = lists.size();

                    List<String> listOne = lists.get(0);

                    String[] eleOriOne = null;
                    String[] eleOriTempOne = listOne.get(1).split("SplitOne");

                    eleOriOne = eleOriTempOne;

                    String fileName = listOne.get(2);

                    switch (size) {
                        case 1:
                            if (fileName.equals(dirNumOne)) {
                                if (middRddDiffOne.size() == limit) {
                                    diffOne = diffOne.union(jsc.parallelize(middRddDiffOne));
                                    middRddDiffOne = Lists.newArrayList();
                                }

                                middRddDiffOne.add(Arrays.toString(eleOriOne));
                            } else {
                                if (middRddDiffTwo.size() == limit) {
                                    diffTwo = diffTwo.union(jsc.parallelize(middRddDiffTwo));
                                    middRddDiffTwo = Lists.newArrayList();
                                }

                                middRddDiffTwo.add(Arrays.toString(eleOriOne));
                            }

                            break;
                        case 2:
                            List<String> listTwo = lists.get(1);

                            String[] eleOriTwo = null;

                            String[] eleOriTempTwo = listTwo.get(1).split("SplitOne");

                            eleOriTwo = eleOriTempTwo;

                            int countOne = Integer.parseInt(listOne.get(0));
                            int countTwo = Integer.parseInt(listTwo.get(0));
                            int countTempOne = countOne;
                            int countTempTwo = countTwo;

                            if (fileName.equals(dirNumTwo)) {
                                eleOriOne = eleOriTempTwo;
                                eleOriTwo = eleOriTempOne;

                                countOne = countTempTwo;
                                countTwo = countTempOne;
                            }

                            resStat(countOne, countTwo
                                    , eleOriOne, eleOriTwo);

                            break;
                    }
                }
            }
        });

        saveFile(true);
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
            , String care, String encoding
            , boolean spaceSenFlag, boolean caseSenFlag, boolean callFlag) {

        JavaPairRDD<String, List<String>> pairsOne = fileRead(filePathOne, fileNumOne, care
                , encoding, spaceSenFlag, caseSenFlag, callFlag);
        JavaPairRDD<String, List<String>> pairsTwo = fileRead(filePathTwo, fileNumTwo, care
                , encoding, spaceSenFlag, caseSenFlag, callFlag);

        conf.setMaster("local[4]")
                .set("spark.default.parallelism", "60");

        if (StringUtils.isBlank(fileOrDirone)) {
            StringBuilder sb = new StringBuilder();

            fileOrDirone = sb.append("文件").append(fileNumOne).toString();

            sb = new StringBuilder();

            fileOrDirTwo = sb.append("文件").append(fileNumTwo).toString();
        }

        JavaPairRDD<String, List<String>> union = pairsOne.union(pairsTwo);

        JavaPairRDD<String, ArrayList<List<String>>> groupUnion = union.aggregateByKey(
                new ArrayList<List<String>>()
                , (ArrayList<List<String>> begin, List<String> V1) -> {
                    ArrayList<List<String>> temp = new ArrayList<List<String>>();

                    if (begin.size() == 0) {
                        temp.add(V1);
                    } else {
                        List<String> V2 = begin.get(0);

                        combine(temp, V1, V2);
                    }

                    return temp;
                }
                , (ArrayList<List<String>> valOne, ArrayList<List<String>> valTwo) -> {

                    ArrayList<List<String>> temp = new ArrayList<List<String>>();

                    List<String> V1 = valOne.get(0);
                    List<String> V2 = valTwo.get(0);

                    String fileNameOne = V1.get(2);
                    String fileNameTwo = V2.get(2);

                    if (fileNameOne.equals(fileNameTwo)) {
                        combine(temp, V1, V2);
                    } else {
                        temp.add(V1);
                        temp.add(V2);
                    }

                    return temp;
                });


        groupUnion.foreachPartition(new VoidFunction<Iterator<Tuple2<String, ArrayList<List<String>>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, ArrayList<List<String>>>> iterator) throws Exception {
                while (iterator.hasNext()) {
                    List<List<String>> lists = iterator.next()._2;

                    int size = lists.size();

                    List<String> listOne = lists.get(0);

                    String[] eleOriOne = null;
                    String[] eleOriTempOne = listOne.get(1).split("SplitOne");

                    eleOriOne = eleOriTempOne;

                    String fileName = listOne.get(2);

                    switch (size) {
                        case 1:
                            if (fileName.equals(fileNumOne)) {
                                if (middRddDiffOne.size() == limit) {
                                    diffOne = diffOne.union(jsc.parallelize(middRddDiffOne));
                                    middRddDiffOne = Lists.newArrayList();
                                }

                                middRddDiffOne.add(Arrays.toString(eleOriOne));
                            } else {
                                if (middRddDiffTwo.size() == limit) {
                                    diffTwo = diffTwo.union(jsc.parallelize(middRddDiffTwo));
                                    middRddDiffTwo = Lists.newArrayList();
                                }

                                middRddDiffTwo.add(Arrays.toString(eleOriOne));
                            }

                            break;
                        case 2:
                            List<String> listTwo = lists.get(1);

                            String[] eleOriTwo = null;

                            String[] eleOriTempTwo = listTwo.get(1).split("SplitOne");

                            eleOriTwo = eleOriTempTwo;

                            int countOne = Integer.parseInt(listOne.get(0));
                            int countTwo = Integer.parseInt(listTwo.get(0));
                            int countTempOne = countOne;
                            int countTempTwo = countTwo;

                            if (fileName.equals(fileNumTwo)) {
                                eleOriOne = eleOriTempTwo;
                                eleOriTwo = eleOriTempOne;

                                countOne = countTempTwo;
                                countTwo = countTempOne;
                            }

                            resStat(countOne, countTwo
                                    , eleOriOne, eleOriTwo);

                            break;
                    }
                }
            }
        });

        if (!callFlag) {
            saveFile(false);
        }
    }

    private static void saveFile(boolean combine) {
        try {

            String[] splitOne = fileOrDirone.split("\\.")[0].split("/");
            String[] splitTwo = fileOrDirTwo.split("\\.")[0].split("/");

            fwCommOne = new FileWriter(new File("Result\\" + "Comm " + splitOne[splitOne.length - 1] + ".txt"));
            fwDiffOne = new FileWriter(new File("Result\\" + "Diff " + splitOne[splitOne.length - 1] + ".txt"));
            fwCommTwo = new FileWriter(new File("Result\\" + "Comm " + splitTwo[splitTwo.length - 1] + ".txt"));
            fwDiffTwo = new FileWriter(new File("Result\\" + "Diff " + splitTwo[splitTwo.length - 1] + ".txt"));

            if (middRddCommOne.size() != 0) {
                commOne = commOne.union(jsc.parallelize(middRddCommOne));
            }

            if (middRddCommTwo.size() != 0) {
                commTwo = commTwo.union(jsc.parallelize(middRddCommTwo));
            }

            if (middRddDiffOne.size() != 0) {
                diffOne = diffOne.union(jsc.parallelize(middRddDiffOne));
            }

            if (middRddDiffTwo.size() != 0) {
                diffTwo = diffTwo.union(jsc.parallelize(middRddDiffTwo));
            }

            commOne.sortBy(new Function<String, Integer>() {
                @Override
                public Integer call(String s) throws Exception {
                    return Integer.valueOf(s.split(":")[0].split("\\[")[1]);
                }
            }, true, 3).foreachPartition(
                    iterator -> {
                        while (iterator.hasNext()) {
                            fwCommOne.write(iterator.next() + "\r\n");
                        }
                    }
            );

            commTwo.sortBy(new Function<String, Integer>() {
                @Override
                public Integer call(String s) throws Exception {
                    return Integer.valueOf(s.split(":")[0].split("\\[")[1]);
                }
            }, true, 3).foreachPartition(
                    iterator -> {
                        while (iterator.hasNext()) {
                            fwCommTwo.write(iterator.next() + "\r\n");
                        }
                    }
            );

            diffOne.sortBy(new Function<String, Integer>() {
                @Override
                public Integer call(String s) throws Exception {
                    return Integer.valueOf(s.split(":")[0].split("\\[")[1]);
                }
            }, true, 3).foreachPartition(
                    iterator -> {
                        while (iterator.hasNext()) {
                            fwDiffOne.write(iterator.next() + "\r\n");
                        }
                    }
            );

            diffTwo.sortBy(new Function<String, Integer>() {
                @Override
                public Integer call(String s) throws Exception {
                    return Integer.valueOf(s.split(":")[0].split("\\[")[1]);
                }
            }, true, 3).foreachPartition(
                    iterator -> {
                        while (iterator.hasNext()) {
                            fwDiffTwo.write(iterator.next() + "\r\n");
                        }
                    }
            );

            fwCommOne.close();
            fwCommTwo.close();
            fwDiffOne.close();
            fwDiffTwo.close();

        } catch (IOException e) {
            e.printStackTrace();

            System.out.println("写入文件出错！！！");
        }
    }

    private static void resStat(int countOne, int countTwo
            , String[] eleOriOne
            , String[] eleOriTwo) {

        int flag = Integer.compare(countOne, countTwo);

        switch (flag) {
            case 1:
                if (middRddDiffOne.size() == limit) {
                    diffOne = diffOne.union(jsc.parallelize(middRddDiffOne));
                    middRddDiffOne = Lists.newArrayList();
                }

                if (middRddCommOne.size() == limit) {
                    commOne = commOne.union(jsc.parallelize(middRddCommOne));
                    middRddCommOne = Lists.newArrayList();
                }

                if (middRddCommTwo.size() == limit) {
                    commTwo = commTwo.union(jsc.parallelize(middRddCommTwo));
                    middRddCommTwo = Lists.newArrayList();
                }

                middRddDiffOne.add(Arrays.toString(Arrays.copyOfRange(eleOriOne, countTwo, countOne)));
                middRddCommOne.add(Arrays.toString(Arrays.copyOfRange(eleOriOne, 0, countTwo)));
                middRddCommTwo.add(Arrays.toString(Arrays.copyOfRange(eleOriTwo, 0, countTwo)));

                break;
            case -1:
                if (middRddDiffTwo.size() == limit) {
                    diffTwo = diffTwo.union(jsc.parallelize(middRddDiffTwo));
                    middRddDiffTwo = Lists.newArrayList();
                }

                if (middRddCommOne.size() == limit) {
                    commOne = commOne.union(jsc.parallelize(middRddCommOne));
                    middRddCommOne = Lists.newArrayList();
                }

                if (middRddCommTwo.size() == limit) {
                    commTwo = commTwo.union(jsc.parallelize(middRddCommTwo));
                    middRddCommTwo = Lists.newArrayList();
                }

                middRddDiffTwo.add(Arrays.toString(Arrays.copyOfRange(eleOriTwo, countOne, countTwo)));
                middRddCommOne.add(Arrays.toString(Arrays.copyOfRange(eleOriOne, 0, countOne)));
                middRddCommTwo.add(Arrays.toString(Arrays.copyOfRange(eleOriTwo, 0, countOne)));

                break;
            case 0:
                if (middRddCommOne.size() == limit) {
                    commOne = commOne.union(jsc.parallelize(middRddCommOne));
                    middRddCommOne = Lists.newArrayList();
                }

                if (middRddCommTwo.size() == limit) {
                    commTwo = commTwo.union(jsc.parallelize(middRddCommTwo));
                    middRddCommTwo = Lists.newArrayList();
                }

                middRddCommOne.add(Arrays.toString(eleOriOne));
                middRddCommTwo.add(Arrays.toString(eleOriTwo));

                break;
        }
    }

    private static void combine(ArrayList<List<String>> temp, List<String> V1, List<String> V2) {

        int countOne = Integer.parseInt(V1.get(0));
        int countTwo = Integer.parseInt(V2.get(0));

        String eleOne = V1.get(1);
        String eleTwo = V2.get(1);

        String count = String.valueOf(countOne + countTwo);

        StringBuilder sb = new StringBuilder();

        sb.append(eleOne).append("SplitOne").append(eleTwo);

        temp.add(Arrays.asList(count, sb.toString(), V1.get(2)));
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
