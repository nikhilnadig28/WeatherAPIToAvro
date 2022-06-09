import com.accuweather.WeatherDataToAvro;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static com.accuweather.WeatherDataToAvro.convertWeatherDataToAvro;


public class WeatherDataToAvroTest {
  private static Dataset<Row> sparkDataset;
  private static SparkSession sparkSession;
  private static Dataset<Row> avroDataset;
  private static String executionDate = "test";
  private static String inputPathPrefix = "src/test/resources/input";
  private static String outputPathPrefix = "src/test/resources/output";
  private static String[] columnsInAvro = {"EpochTime", "HasPrecipitation", "Temperature", "WeatherText", "Hour", "Day", "Month", "Year", "DateTime", "Temperature_Imperial", "Temperature_Metric"};
  private static Logger logger = LogManager.getLogger(WeatherDataToAvro.class);
  @BeforeClass
  public static void beforeClass() {
     sparkSession = SparkSession.builder()
            .master("local[1]")
            .appName("JSON to Avro")
            .getOrCreate();

     sparkDataset = sparkSession.read()
            .format("json")
            .option("allowUnquotedFieldNames", "true")
            .option("allowBackslashEscapingAnyCharacter", "true")
            .option("primitivesAsString", "true")
            .option("multiline", "true")
            .load(inputPathPrefix + "/weather_test.json");
    logger.info("Created spark context");

    logger.info("Converting JSON to AVRO");
    convertWeatherDataToAvro(sparkSession, executionDate, inputPathPrefix, outputPathPrefix);

    //Loading data written as Avro to dataset
    avroDataset = sparkSession.read().format("avro").load(outputPathPrefix + "/weather_" + executionDate);

  }

  @Test
  public void countNumberOfRowsInSourceAndDestination() {
    assertEquals(sparkDataset.count(), avroDataset.count());

  }

  @Test
  public void countNumberOfColumnsInDestination() {
    assertEquals(columnsInAvro.length, avroDataset.columns().length);
  }

  @AfterClass
  public static void afterClass() {
    if (sparkSession != null) {
      sparkSession.stop();
    }
  }
}
