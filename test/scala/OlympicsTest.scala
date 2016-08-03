import com.tw.samples.Olympics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit.{After, Before, Test}

class OlympicsTest {
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


  @Test def shouldfindtotalmedalswonbycountryforgivenyear {
    val olympicsData = "src/main/resources/olympic.csv"

    val inputRDD: RDD[String] = sparkContext.textFile(olympicsData)

    val totalMedals: Int = Olympics(inputRDD).totalMedalsWon("United States",2002)

    assert(17 == totalMedals)
  }

  @Test def shouldfindtotalmedalswonbycountryforgivenyear_ {
    val olympicsData = "src/main/resources/olympic.csv"
    val inputRDD: RDD[String] = sparkContext.textFile(olympicsData)

    val actualYear: Int = Olympics(inputRDD).findYearOfMaxMedals("United States")

    assert(2004 == actualYear)
  }

  @Test def shouldfindtotalmedalswonbycountryforgiven_year {
    val olympicsData = "src/main/resources/olympic.csv"
    val inputRDD: RDD[String] = sparkContext.textFile(olympicsData)

    val players: Array[(String, String)] = Olympics(inputRDD).playersWithMedalsInMoreThanOneSport(2000)

    val expectedPlayers = Array(("Li Na", "China"))
    assert(players.length == 1)
    assert(players.contains(expectedPlayers(0)))
  }

  @Test def shouldfindtotalmedalswonbycountry_for_givenyear {
    val olympicsData = "src/main/resources/olympic.csv"
    val inputRDD: RDD[String] = sparkContext.textFile(olympicsData)

    val country: String = Olympics(inputRDD).countryWithMaxMedalsFor(2002)

    assert("United States" == country)
  }
}
