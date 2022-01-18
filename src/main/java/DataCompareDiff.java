import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.FileUtil;

public class DataCompareDiff {

    private final static Logger logger = LoggerFactory.getLogger(DataCompareDiff.class);

    public static void main(String[] args) {
        String filePathOne = "C:\\Users\\user\\Desktop\\秋招\\国泰\\项目\\Test for Java\\a.txt";
        String filePathTwo = "C:\\Users\\user\\Desktop\\秋招\\国泰\\项目\\Test for Java\\b.txt";

        String filePathThree = "C:\\Users\\user\\Desktop\\秋招\\国泰\\项目\\Demo\\Ref\\cash_fund_61.xml";

        String testOne = "C:\\Users\\user\\Desktop\\秋招\\国泰\\项目\\Test for Java\\dirOne\\***.***";
        String testTwo = "C:\\Users\\user\\Desktop\\秋招\\国泰\\项目\\Test for Java\\dirTwo\\***.***";

        FileUtil.dataCompareDiffTwoFiles(filePathOne, filePathThree, "dirOne/a.txt", "dirTwo/b.txt", "UTF-8", false, false, false);

        // FileUtil.dataCompareDiffTwoDirs(testOne, testTwo, "UTF-8",false,false);
    }
}