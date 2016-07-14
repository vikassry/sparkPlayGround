
import com.tw.samples.ScalaWordCount
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class WordCountTest {
    @transient
    protected var sparkContext: SparkContext = null

    @Before
    def setUp() {
        val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local")
        sparkContext = new SparkContext(sparkConf)
    }

    @After
    def tearDown() {
        sparkContext.stop()
    }

    @Test def shouldBeAbleToDoWordCountOnTheSimpleStrings() {
        val initialDataset = sparkContext.parallelize(Array("This is a text", "Another text", "More text", "a text"))
        val wordCount = ScalaWordCount.wordCount(initialDataset)
        val result: Array[(String, Int)] = wordCount.collect
        val expected: Array[(String, Int)] = Array(("Another", 1), ("is", 1), ("a", 2), ("text", 4), ("This", 1), ("More", 1))
        assert(result sameElements expected)
    }
}