# Weather Data from API to AVRO

This tool pulls data from the AccuWeather's `currentconditions` API for the last 24 hours in Stockholm

# Running Locally

If you're running this through IntelliJ make sure to add the following arguments as CLI arguments in the configuration
```
--inputDir <input directory to store raw json file> --outputDir <output directory for avro files> --apiKey <api-key>
```
Alternatively, if you would like to run the project on command line:
- Make sure you have Spark installed 

Create a package by running
```
mvn package
```
We have the maven assembly plugin installed which will create a fat jar with all the necessary dependencies

To execute the jar, run the following
```
spark-submit --master local --class com.accuweather.WeatherDataToAvro target/WeatherAPIToAvro-1.0-SNAPSHOT-jar-with-dependencies.jar --inputDir input --outputDir output --apiKey <add your key here>
```

Note:
If you run the tests multiple times, make sure to delete the output files that are created in `src/test/resources/output` before running them again