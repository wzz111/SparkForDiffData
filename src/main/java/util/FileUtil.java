package util;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class FileUtil {

    private final static Logger logger = LoggerFactory.getLogger(FileUtil.class);

    public static JavaRDD<String> fileRead(JavaSparkContext jsc, String filePath, String encoding) {
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

    public static JavaPairRDD<String, List<String>> eachKeyCount(JavaRDD<String> lines, boolean spaceSenFlag,
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

                    @Override
                    public Tuple2<String, List<String>> call(String line) throws Exception {
                        String transLine = line;

                        if (!spaceSenFlag) {
                            transLine = line.replaceAll(" ", "");
                        }

                        if (!caseSenFlag) {
                            transLine = transLine.toLowerCase();
                        }

                        return new Tuple2<String, List<String>>(transLine, Arrays.asList("1", line, fileNum));
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

                        eleOne = eleOne + "_" + eleTwo;

                        return Arrays.asList(count, eleOne, V1.get(2));
                    }
                });
    }

    public static void resStat(int countOne, int countTwo, String[] oriKeyArrOne, String[] oriKeyArrTwo
            , List<String> commonOne
            , List<String> commonTwo
            , List<String> diffOne
            , List<String> diffTwo) {

        int flag = Integer.compare(countOne, countTwo);

        switch (flag) {
            case 1:
                diffOne.add(
                        Arrays.toString(Arrays.copyOfRange(oriKeyArrOne, countTwo, countOne))
                );

                commonOne.add(
                        Arrays.toString(Arrays.copyOfRange(oriKeyArrOne, 0, countTwo))
                );

                commonTwo.add(
                        Arrays.toString(Arrays.copyOfRange(oriKeyArrTwo, 0, countTwo))
                );

                break;
            case -1:
                diffTwo.add(
                        Arrays.toString(Arrays.copyOfRange(oriKeyArrTwo, countOne, countTwo))
                );

                commonOne.add(
                        Arrays.toString(Arrays.copyOfRange(oriKeyArrOne, 0, countOne))
                );

                commonTwo.add(
                        Arrays.toString(Arrays.copyOfRange(oriKeyArrTwo, 0, countOne))
                );

                break;
            case 0:
                commonOne.add(
                        Arrays.toString(oriKeyArrOne)
                );

                commonTwo.add(
                        Arrays.toString(oriKeyArrTwo)
                );

                break;
        }
    }
}
