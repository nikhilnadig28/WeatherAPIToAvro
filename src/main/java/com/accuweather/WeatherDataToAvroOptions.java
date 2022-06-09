package com.accuweather;

import com.beust.jcommander.Parameter;

public class WeatherDataToAvroOptions {

  @Parameter(names = { "-i", "--inputDir" }, description = "Level of verbosity")
   String inputDir = "input";

  @Parameter(names = { "-o", "--outputDir"}, description = "Comma-separated list of group names to be run")
   String outputDir;

  @Parameter(names = { "-k", "--apiKey"}, description = "Comma-separated list of group names to be run")
   String apiKey;
}
