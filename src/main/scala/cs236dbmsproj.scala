import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions

object cs236dbmsproj {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("DBMS Proj")
      .config("spark.master", "local[*]")
      .getOrCreate()
    val start_time = System.nanoTime();
    runBasicDataFrame(spark)
    spark.stop();
    println("Time to execute: "+(System.nanoTime() - start_time)/1000000000 + "seconds")
  }

  private def runBasicDataFrame(spark: SparkSession): Unit = {
    //APPEND BASE LOCATION ACCORDING TO MACHINE
    val base = "/home/yogesh/IdeaProjects/CS236.ysing012.DBMSProj/"

    val weather_stations_df = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(""+base+"Locations/WeatherStationLocations.csv")
    weather_stations_df.createOrReplaceTempView("Stations_Data")
    weather_stations_df.show();
    weather_stations_df.printSchema();

    val df_Y06 = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(""+base+"Recordings/2006.txt")
    df_Y06.createOrReplaceTempView("2006_data")
    df_Y06.printSchema()

    val df_Y07 = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(""+base+"Recordings/2007.txt")
    df_Y07.createOrReplaceTempView("2007_data")

    val df_Y08 = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(""+base+"Recordings/2008.txt")
    df_Y08.createOrReplaceTempView("2008_data")

    val df_Y09 = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(""+base+"Recordings/2009.txt")
    df_Y09.createOrReplaceTempView("2009_data")

    //GET STATION WITH CTRY AS US
    /*********************************************
     * TASK 1: AS YOUR DATASET CONTAINS DATA FROM STATIONS AROUND THE WORLD WHILE WHAT YOU ARE ASKED IS ABOUT
     * US STATES, YOU WOULD FIRST NEED TO IDENTIFY WHICH STATIONS ARE WITHIN THE UNITED STATES
     */
    val US_Stations_df = spark.sql("SELECT * from Stations_Data where CTRY = 'US'")
    US_Stations_df.createOrReplaceTempView("US_Stations")
    //Get Results Important for this Project
    val Year_2006_df = spark.sql("SELECT US_Stations.USAF, US_Stations.STATE, 2006_data._c3 AS YEARMODA, 2006_data._c14 as Precipitation" +
      "FROM 2006_data INNER JOIN US_Stations ON 2006_data.'STN---' = US_Stations.USAF")
    val Year_2007_df = spark.sql("SELECT US_Stations.USAF, US_Stations.STATE, 2007_data._c3 AS YEARMODA, 2007_data._c14 as Precipitation" +
      "FROM 2007_data INNER JOIN US_Stations ON 2007_data.'STN---' = US_Stations.USAF")
    val Year_2008_df = spark.sql("SELECT US_Stations.USAF, US_Stations.STATE, 2008_data._c3 AS YEARMODA, 2008_data._c14 as Precipitation" +
      "FROM 2008_data INNER JOIN US_Stations ON 2008_data.'STN---' = US_Stations.USAF")
    val Year_2009_df = spark.sql("SELECT US_Stations.USAF, US_Stations.STATE, 2009_data._c3 AS YEARMODA, 2009_data._c14 as Precipitation" +
      "FROM 2009_data INNER JOIN US_Stations ON 2009_data.'STN---' = US_Stations.USAF")

    val mergeResults = Year_2006_df.union(Year_2007_df).union(Year_2008_df).union(Year_2009_df);
    mergeResults.createOrReplaceTempView("ProjectData")

    //Compute PRCP value
    //Get PRCP INTEGER VALUE
    val prcp_int_df = mergeResults.select(col("USAF, Precipitation"), substring(col("Precipitation"),size(col("Precipitation"))-2, size(col("Precipitation"))-1).as("PRCP_I"))
    //GET PRCP LETTER VALUE
    val prcp_letter_df = mergeResults.select(col("USAF, Precipitation"), substring(col("Precipitation"),0, size(col("Precipitation"))).as("PRCP_L"))
    //PUT THEM IN SINGLE DF IN DIFFERENT COLS PRCP_I AND PRCP_L
    val prcp_df = prcp_int_df.as("df1").join(prcp_letter_df, prcp_int_df("USAF") === prcp_letter_df("USAF"))
    //JOIN THE TWO NEW COLS TO OUR PROJECT DATA
    mergeResults.join(prcp_df, mergeResults("USAF") === prcp_df("USAF"))

    mergeResults.createOrReplaceTempView("ProjectData")

    //CONVERT PRCP LETTER TO CORRESPONDING NUMBER OF HOURS
    // ASSIGN NUMERIC VALUE TO PRECIPITATION STRING // ASSIGN NUMERIC VALUE TO PRECIPITATION STRING
    spark.sql("UPDATE Project_Data SET PRCP_L='6' WHERE PRCP_L LIKE 'A'")
    spark.sql("UPDATE Project_Data SET PRCP_L='12' WHERE PRCP_L LIKE 'B'")
    spark.sql("UPDATE Project_Data SET PRCP_L='18' WHERE PRCP_L LIKE 'C'")
    spark.sql("UPDATE Project_Data SET PRCP_L='24' WHERE PRCP_L LIKE 'D'")
    spark.sql("UPDATE Project_Data SET PRCP_L='12' WHERE PRCP_L LIKE 'E'")
    spark.sql("UPDATE Project_Data SET PRCP_L='24' WHERE PRCP_L LIKE 'F'")
    spark.sql("UPDATE Project_Data SET PRCP_L='24' WHERE PRCP_L LIKE 'G'")
    spark.sql("UPDATE Project_Data SET PRCP_L='0' WHERE PRCP_L LIKE 'H'")
    spark.sql("UPDATE Project_Data SET PRCP_L='0' WHERE PRCP_L LIKE 'I'")

    // CHANGE 99.99 VALUES TO 0
    spark.sql("UPDATE Project_Data SET PRCP_I='0' WHERE PRCP_I LIKE '99.99'")

    mergeResults.withColumn("PRCP_I", col("PRCP_I").cast(Double)).withColumn("PRCP_L",col("PRCP_L").cast(Double))
    mergeResults.createOrReplaceTempView("Project_Data")

    // MULTIPLY THE VALUES
    val Prcp_fixed_df = spark.sql("SELECT USAF, STATE, YEARMODA, PRECIPITATION, PRCP_I*PRCP_L AS PRCP_FINAL FROM Project_Data")
    Prcp_fixed_df.createOrReplaceTempView("Project_Data")

    /** ******************************************************************************************************************************************
     * Task 2 (25 points) : For each state with readings, you will first need to find the average precipitation recorded for each month (ignoring year).
     */
    //Grouping the above table on State and Month and Ordered by STATE
    val group_by_month_df = spark.sql("SELECT AVG(PRCP_FINAL) as AvgPRCP, STATE, FLOOR((YEARMODA/100)%100) AS MONTH FROM Project_Data WHERE STATE IS NOT NULL GROUP BY STATE")
    group_by_month_df.createOrReplaceTempView("State_Groups")

    /********************************************************************************************************************************************
        Task 3 (25 points) : Then find the months with the highest and lowest averages for each state.
     */
    //Finding Max Avg Precipitation and Min Avg Precipitation for each state for each month.
    val state_max_df = spark.sql("SELECT t1.AvgPRCP as MaxPRCP,  t1.STATE, t1.MONTH as MaxMonth FROM State_Groups as t1 " +
      "JOIN (SELECT MAX(AvgPRCP) as maxPRCP, STATE FROM State_Groups GROUP BY STATE) t2 " +
      "ON t1.STATE = t2.STATE  AND t1.AvgPRCP = t2.maxPRCP");
    val state_min_df = spark.sql("SELECT t1.AvgPRCP as MinPRCP,  t1.STATE, t1.MONTH as MinMonth FROM State_Groups as t1 " +
      "JOIN (SELECT MIN(AvgPRCP) as minPRCP, STATE FROM State_Groups GROUP BY STATE) t2 " +
      "ON t1.STATE = t2.STATE  AND t1.AvgPRCP = t2.minPRCP");

    state_max_df.createOrReplaceTempView("State_Max");
    state_min_df.createOrReplaceTempView("State_MIN");

    //Joining both tables to get Max Precipitation and Min Precipitation by state in view
    val state_max_min_df = spark.sql( "SELECT State_Max.STATE, State_Max.MaxPRCP, State_Max.MaxMonth as MaxMonth, MinByState.MinPRCP, MinByState.MinMonth as MinMonth " +
      "FROM State_Max JOIN MinByState ON  State_Max.STATE = MinByState.STATE");
    state_max_min_df.createOrReplaceTempView("StateMaxMin");

    /********************************************************************************************************************************************
        Task 4 (25 points) : You will need to order the states by the difference between the highest and lowest month average, in ascending order.
     */
    //Creating a new column o show the difference between Max and Min Precipitations and order them in ascending order
    val max_min_diff_df = spark.sql("SELECT *, MaxPRCP-MinPRCP as Difference FROM StatewMaxMin ORDER BY Difference ASC")
    //        Max_Min_Diff.show();

    //Provide month name for month numbers
    val monthly_data_df = max_min_diff_df.withColumn("Highest_Month", functions.when(max_min_diff_df.col("MaxMonth").equalTo(1), "January")
      .when(max_min_diff_df.col("MaxMonth").equalTo(2), "February")
      .when(max_min_diff_df.col("MaxMonth").equalTo(3), "March")
      .when(max_min_diff_df.col("MaxMonth").equalTo(4), "April")
      .when(max_min_diff_df.col("MaxMonth").equalTo(5), "May")
      .when(max_min_diff_df.col("MaxMonth").equalTo(6), "June")
      .when(max_min_diff_df.col("MaxMonth").equalTo(7), "July")
      .when(max_min_diff_df.col("MaxMonth").equalTo(8), "August")
      .when(max_min_diff_df.col("MaxMonth").equalTo(9), "September")
      .when(max_min_diff_df.col("MaxMonth").equalTo(10), "October")
      .when(max_min_diff_df.col("MaxMonth").equalTo(11), "November")
      .when(max_min_diff_df.col("MaxMonth").equalTo(12), "December"))
      .withColumn("Lowest_Month", functions.when(max_min_diff_df.col("MinMonth").equalTo(1), "January")
        .when(max_min_diff_df.col("MinMonth").equalTo(2), "February")
        .when(max_min_diff_df.col("MinMonth").equalTo(3), "March")
        .when(max_min_diff_df.col("MinMonth").equalTo(4), "April")
        .when(max_min_diff_df.col("MinMonth").equalTo(5), "May")
        .when(max_min_diff_df.col("MinMonth").equalTo(6), "June")
        .when(max_min_diff_df.col("MinMonth").equalTo(7), "July")
        .when(max_min_diff_df.col("MinMonth").equalTo(8), "August")
        .when(max_min_diff_df.col("MinMonth").equalTo(9), "September")
        .when(max_min_diff_df.col("MinMonth").equalTo(10), "October")
        .when(max_min_diff_df.col("MinMonth").equalTo(11), "November")
        .when(max_min_diff_df.col("MinMonth").equalTo(12), "December"))

    //      monthlyData.show();
    monthly_data_df.createOrReplaceTempView("MonthDatFinal")

    val allDataOrderedByMonth = spark.sql("SELECT STATE, MaxPRCP AS Highest_Avg_Prcp, Highest_Month, MinPRCP AS Lowest_Avg_Prcp, Lowest_Month, Difference FROM MonthDataFinal ORDER BY Difference")
    // Write final Data frame to CSV
    allDataOrderedByMonth.write.format("com.databricks.spark.csv")
      .option("inferSchema", "false")
      .option("header", "true")
      .option("charset", "UTF-8")
      .mode("overwrite")
      .save(""+base+"Output/") //Append Your Path

  }
}
