import org.apache.commons.cli._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Study {
    val logger = LoggerFactory.getLogger(DataCompare.getClass)
    def main(args: Array[String]): Unit = {
      val options = new Options
      options.addOption("l", "left", true, "源数据目录")
      options.addOption("r", "right", true, "目的数据目录")
      options.addOption("t", "fileType", true, "文件类型，可选：text、parquet、orc、json")
      options.addOption("o", "output", true, "输出结果目录")
      val parser = new BasicParser
      val cmd = parser.parse(options, args)
      val hf = new HelpFormatter
      if (!cmd.hasOption('l') || !cmd.hasOption('r')) {
        hf.printHelp("必须指定两个目录", options)
        return
      }
      if (!cmd.hasOption('o')) {
        hf.printHelp("必须指定输出目录", options)
        return
      }
      val leftPath = cmd.getOptionValue("l")
      val rightPath = cmd.getOptionValue('r')
      val outputPath = cmd.getOptionValue('o')
      var fileType = "text"
      if (cmd.hasOption('t')) {
        fileType = cmd.getOptionValue('t')
      }
      val conf = new SparkConf().setAppName("DataCompare")
      if (!leftPath.startsWith("hdfs://")) {
        conf.setMaster("local")
        conf.set("spark.local.dir", "D:/tmp")
        conf.set("spark.driver.host", "0.0.0.0")
      }
      val sc = new SparkContext(conf);
      val sqlContext = new HiveContext(sc)
      var tmpLeftDF = getSourceDF(sqlContext, leftPath, fileType)
      var tmpRightDF = getSourceDF(sqlContext, rightPath, fileType)
      // 将所有字段拼接为一个字符串
      val leftDF = tmpLeftDF.map(row => (row.toString(), row.toString()))
      val rightDF = tmpRightDF.map(row => (row.toString(), row.toString()))
      val joinDF = leftDF.fullOuterJoin(rightDF)
      // 左表有且右表无，输出左表内容
      joinDF.filter(t => t._2._1.nonEmpty && t._2._2.isEmpty).map(t => t._2._1.get).saveAsTextFile(outputPath + "/left")
      // 右表有且左表无，输出右表内容
      joinDF.filter(t => t._2._1.isEmpty && t._2._2.nonEmpty).map(t => t._2._2.get).saveAsTextFile(outputPath + "/right")
    }

    def getSourceDF(sqlContext: SQLContext, path: String, fileType: String) = fileType match {
      case "parquet" => sqlContext.read.parquet(path)
      case "orc" => sqlContext.read.orc(path)
      case "json" => sqlContext.read.json(path)
      case _ => sqlContext.read.text(path)
    }
}
