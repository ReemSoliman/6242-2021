// Databricks notebook source
// STARTER CODE - DO NOT EDIT THIS CELL
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
val customSchema = StructType(Array(StructField("lpep_pickup_datetime", StringType, true), StructField("lpep_dropoff_datetime", StringType, true), StructField("PULocationID", IntegerType, true), StructField("DOLocationID", IntegerType, true), StructField("passenger_count", IntegerType, true), StructField("trip_distance", FloatType, true), StructField("fare_amount", FloatType, true), StructField("payment_type", IntegerType, true)))

// COMMAND ----------

// STARTER CODE - YOU CAN LOAD ANY FILE WITH A SIMILAR SYNTAX.
val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/nyc_tripdata.csv") // the csv file which you want to work with
   .withColumn("pickup_datetime", from_unixtime(unix_timestamp(col("lpep_pickup_datetime"), "MM/dd/yyyy HH:mm")))
   .withColumn("dropoff_datetime", from_unixtime(unix_timestamp(col("lpep_dropoff_datetime"), "MM/dd/yyyy HH:mm")))
   .drop($"lpep_pickup_datetime")
   .drop($"lpep_dropoff_datetime")

// COMMAND ----------

// LOAD THE "taxi_zone_lookup.csv" FILE SIMILARLY AS ABOVE. CAST ANY COLUMN TO APPROPRIATE DATA TYPE IF NECESSARY.
// ENTER THE CODE BELOW
 val taxi_df = spark.read.format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(StructType(Array(StructField("LocationID",IntegerType,true), StructField("Borough",StringType,true), StructField("Zone",StringType,true), StructField("service_zone",StringType,true) ))).load("/FileStore/tables/taxi_zone_lookup.csv")


// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Some commands that you can use to see your dataframes and results of the operations. You can comment the df.show(5) and uncomment display(df) to see the data differently. You will find these two functions useful in reporting your results.
// display(df)
df.show(5) // view the first 5 rows of the dataframe

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Filter the data to only keep the rows where "PULocationID" and the "DOLocationID" are different and the "trip_distance" is strictly greater than 2.0 (>2.0).

// VERY VERY IMPORTANT: ALL THE SUBSEQUENT OPERATIONS MUST BE PERFORMED ON THIS FILTERED DATA

val df_filter = df.filter($"PULocationID" =!= $"DOLocationID" && $"trip_distance" > 2.0)
df_filter.show(5)

// COMMAND ----------

// PART 1a: The top-5 most popular drop locations - "DOLocationID", sorted in descending order - if there is a tie, then one with lower "DOLocationID" gets listed first
// Output Schema: DOLocationID int, number_of_dropoffs int 

// Hint: Checkout the groupBy(), orderBy() and count() functions.
// ENTER THE CODE BELOW
var df_DO = df_filter.groupBy($"DOLocationID").count().orderBy($"count".desc, $"DOLocationID".asc).withColumnRenamed("count","number_of_dropoffs").limit(5)
df_DO.show()



// COMMAND ----------

// PART 1b: The top-5 most popular pickup locations - "PULocationID", sorted in descending order - if there is a tie, then one with lower "PULocationID" gets listed first 
// Output Schema: PULocationID int, number_of_pickups int

// Hint: Code is very similar to part 1a above.
// ENTER THE CODE BELOW
var df_PU = df_filter.groupBy($"PULocationID").count().orderBy($"count".desc, $"PULocationID".asc).withColumnRenamed("count","number_of_pickups").limit(5)
df_PU.show()


// COMMAND ----------

// PART 2: List the top-3 locations with the maximum overall activity, i.e. sum of all pickups and all dropoffs at that LocationID. In case of a tie, the lower LocationID gets listed first.
// Output Schema: LocationID int, number_activities int

// Hint: In order to get the result, you may need to perform a join operation between the two dataframes that you created in earlier parts (to come up with the sum of the number of pickups and dropoffs on each location). 
// ENTER THE CODE BELOW
df_DO = df_DO.withColumnRenamed("DOLocationID","LocationID").withColumnRenamed("number_of_dropoffs","number_activities")
df_PU = df_PU.withColumnRenamed("PULocationID","LocationID").withColumnRenamed("number_of_pickups","number_activities")
var locations_act = df_PU.union(df_DO)//, df_PU("PULocationID") ===df_DO("DOLocationID"),"inner")
var summariezed = locations_act.groupBy($"LocationID").agg(sum("number_activities").alias("number_activities")).orderBy($"number_activities".desc, $"LocationID".asc).limit(3)
summariezed.show(10)



// COMMAND ----------

// PART 3: List all the boroughs in the order of having the highest to lowest number of activities (i.e. sum of all pickups and all dropoffs at that LocationID), along with the total number of activity counts for each borough in NYC during that entire period of time.
// Output Schema: Borough string, total_number_activities int

// Hint: You can use the dataframe obtained from the previous part, and will need to do the join with the 'taxi_zone_lookup' dataframe. Also, checkout the "agg" function applied to a grouped dataframe.
// ENTER THE CODE BELOW
var df_D = df_filter.groupBy($"DOLocationID").count().withColumnRenamed("count","number_activities").withColumnRenamed("DOLocationID","LocationID")
var df_P = df_filter.groupBy($"PULocationID").count().withColumnRenamed("count","number_activities").withColumnRenamed("PULocationID","LocationID")
var loc_act = df_PU.union(df_DO)
var summ = locations_act.groupBy($"LocationID").agg(sum("number_activities").alias("number_activities"))
//summ.show()
var joined = summ.join(taxi_df, summ("LocationID") === taxi_df("LocationID"))
//joined.show()
var total_act = joined.groupBy($"Borough").agg(sum("number_activities").alias("total_number_activities")).orderBy($"total_number_activities".desc)
total_act.show()



// COMMAND ----------

// PART 4: List the top 2 days of week with the largest number of (daily) average pickups, along with the values of average number of pickups on each of the two days. The day of week should be a string with its full name, for example, "Monday" - not a number 1 or "Mon" instead.
// Output Schema: day_of_week string, avg_count float

// Hint: You may need to group by the "date" (without time stamp - time in the day) first. Checkout "to_date" function.

// ENTER THE CODE BELOW

var df_PU_Date = df_filter.select($"PULocationID", $"pickup_datetime")
.withColumn("PickupDate", date_format($"pickup_datetime", "MM-dd-yyyy"))

var df_PU_GbyDate = df_PU_Date.groupBy($"PickupDate").count().alias("count_pickups")
                    .withColumnRenamed("count","count_pickups")
                    .withColumn("PickupDate_ts",to_timestamp(col("PickupDate"), "MM-dd-yyyy"))
                    .withColumn("day_of_week", date_format(col("PickupDate_ts"), "EEEE"))

//df_PU_GbyDate.show()

var day_df = df_PU_GbyDate.groupBy($"day_of_week").agg(count("PickupDate") as "day_c", sum($"count_pickups") as "no_pickups")
             .withColumn("avg_count",$"no_pickups"/$"day_c").orderBy($"avg_count".desc).limit(2)
             .drop($"day_c")
             .drop($"no_pickups")
day_df.show()


// COMMAND ----------

// PART 5: For each particular hour of a day (0 to 23, 0 being midnight) - in their order from 0 to 23, find the zone in Brooklyn borough with the LARGEST number of pickups. 
// Output Schema: hour_of_day int, zone string, max_count int

// Hint: You may need to use "Window" over hour of day, along with "group by" to find the MAXIMUM count of pickups

// ENTER THE CODE BELOW

var df_brooklyn = df_filter.join(taxi_df, df_filter("PULocationID") === taxi_df("LocationID"))
             .filter($"Borough" === "Brooklyn")
             .withColumn("hour", hour(col("pickup_datetime")))
             
//df_brooklyn.show(false)
var df_brooklyn_hour = df_brooklyn.groupBy($"hour", $"Zone").count()
//df_brooklyn_hour.show()
var df_brooklyn_hour_max = df_brooklyn_hour.groupBy($"hour").agg(max($"count"))
                           .withColumnRenamed("hour", "hourOfTheDay")

//df_brooklyn_hour_max.show()
var x = df_brooklyn_hour.join(df_brooklyn_hour_max, df_brooklyn_hour("hour") === df_brooklyn_hour_max("hourOfTheDay") && df_brooklyn_hour("count") === df_brooklyn_hour_max("max(count)"))
        .drop("hourOfTheDay").drop("count")
       .orderBy($"hour")
x.show(24, truncate = false)

// COMMAND ----------

// PART 6 - Find which 3 different days of the January, in Manhattan, saw the largest percentage increment in pickups compared to previous day, in the order from largest increment % to smallest increment %. 
// Print the day of month along with the percent CHANGE (can be negative), rounded to 2 decimal places, in number of pickups compared to previous day.
// Output Schema: day int, percent_change float
// Hint: You might need to use lag function, over a window ordered by day of month.
// ENTER THE CODE BELOW
var df_jan = df_filter.withColumn("day", dayofmonth(col("pickup_datetime")))
            .withColumn("month", month(col("pickup_datetime")))
            .filter($"month" === "1")
            .groupBy($"day").count().orderBy($"day")
val windowSpec = Window.orderBy('day)
  //df_jan.show()      
df_jan.withColumn("lag", lag('count, 1) over windowSpec)
      .withColumn("percent_change", (($"count"-$"lag")/$"lag" *100))
      .withColumn("percent_change", col("percent_change").cast("Decimal(4,2)"))
      .orderBy($"percent_change".desc).limit(3)
      .drop("count"). drop("lag")
      .show 


