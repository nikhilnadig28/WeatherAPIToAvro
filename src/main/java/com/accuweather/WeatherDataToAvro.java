package com.accuweather;

import java.io.FileWriter;
import java.net.*;

import com.beust.jcommander.JCommander;
import com.google.common.io.Resources;
import com.google.common.base.Charsets;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.net.URL;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.dayofmonth;
import static org.apache.spark.sql.functions.from_unixtime;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.year;


public class WeatherDataToAvro {
  private static Logger logger = LogManager.getLogger(WeatherDataToAvro.class);

  public static void main(String[] args) {
    WeatherDataToAvroOptions options = new WeatherDataToAvroOptions();
    JCommander jCommander = JCommander.newBuilder()
            .addObject(options)
            .build();
    jCommander.parse(args);

    SparkSession sparkSession = SparkSession.builder()
            .master("local[1]")
            .appName("JSON to Avro")
            .getOrCreate();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    Date date = new Date();
    String executionDate = dateFormat.format(date);
    fetchWeatherData(executionDate, options.inputDir, options.apiKey);
    convertWeatherDataToAvro(sparkSession, executionDate, options.outputDir);
  }


  public static void convertWeatherDataToAvro(SparkSession sparkSession, String executionDate, String outputPathPrefix) {

        Dataset<Row> sparkDataset = sparkSession.read()
                .format("json")
                .option("allowUnquotedFieldNames", "true")
                .option("allowBackslashEscapingAnyCharacter", "true")
                .option("primitivesAsString", "true")
                .option("multiline", "true")
                .load("input/weather_"+ executionDate +".json");
        logger.info("Selecting the columns to build the dataset");

        Dataset<Row> selectedData = sparkDataset.select("EpochTime", "HasPrecipitation", "Temperature", "WeatherText");

        logger.info("Creating new columns for day, month, year, and temperature");
        Dataset<Row> finalData = selectedData
                .withColumn("Hour", hour(from_unixtime(selectedData.col("EpochTime"))))
                .withColumn("Day", dayofmonth(from_unixtime(selectedData.col("EpochTime"))))
                .withColumn("Month", month(from_unixtime(selectedData.col("EpochTime"))))
                .withColumn("Year", year(from_unixtime(selectedData.col("EpochTime"))))
                .withColumn("DateTime", from_unixtime(selectedData.col("EpochTime")))
                .withColumn("Temperature_Imperial", col("Temperature.Imperial"))
                .withColumn("Temperature_Metric", col("Temperature.Metric"));

        logger.info("Starting writing data to avro files");
        finalData.write().format("avro").save(outputPathPrefix +"/weather_" + executionDate );
        logger.info("Finished writing data to avro files");
      }

   public static void fetchWeatherData(String executionDate, String inputPathPrefix, String apiKey) {
    try {

      String stockholmCityCode = "314929";
      String urlFormatting = MessageFormat.format("https://dataservice.accuweather.com/currentconditions/v1/{0}/historical/24?apikey={1}", stockholmCityCode, apiKey);

      URL url = new URL(urlFormatting);
      HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
      urlConnection.setRequestMethod("GET");
      logger.info("Fetching data from AccuWeather API");
      urlConnection.connect();

      int responseCode = urlConnection.getResponseCode();
      if (responseCode != 200) {
        logger.error("HttpResponseCode: " + responseCode);
      } else {

        String readData = Resources.toString(url, Charsets.UTF_8);
        try (FileWriter file = new FileWriter(inputPathPrefix + "/weather_" + executionDate + ".json")) {
          logger.info("Starting writing API response payload to JSON file");
          file.write(readData);
          file.flush();
          logger.info("Finished writing API response payload to JSON file");
        } catch (Exception e) {
                logger.error("Error writing data to file");
                logger.error(e.getMessage());
        }
      }
    }
    catch (Exception e) {
      logger.error("Error reading API response");
      logger.error(e.getMessage());
    }
  }
}
