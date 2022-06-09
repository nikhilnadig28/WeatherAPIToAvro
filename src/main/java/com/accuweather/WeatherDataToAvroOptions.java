package com.accuweather;

import com.beust.jcommander.Parameter;

public class WeatherDataToAvroOptions {

  @Parameter(names = { "-i", "--inputDir" }, description = "Input directory where the raw JSON file will be stored")
   String inputDir = "input";

  @Parameter(names = { "-o", "--outputDir"}, description = "Output directory where the converted AVRO files will be stored")
   String outputDir;

  @Parameter(names = { "-k", "--apiKey"}, description = "API Key used to fetch data from AccuWeather")
   String apiKey;
}
