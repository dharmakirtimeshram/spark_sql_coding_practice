import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q12_movie_theater_showtimes {

  def main(args:Array[String]):Unit= {
    val sparkconf = new SparkConf().setAppName("qb12").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val movies= List((1,"Action Hero","2024-01-10",8),(2,"Comedy Nights","2024-01-15",25),
                     (3,"Action Packed","2024-01-20",55),(4,"Romance Special","2024-02-01",5),
                      (5,"Action Force","2024-02-10",45),(6,"Drama Series","2024-03-01",70))
                      .toDF("Show_Id","Movie_Title","Show_Time","Seats_Available")
    //movies.show()

    /*#Create a new column availability based on seats_available:
  'Full' if seats_available <= 10
  'Limited' if 11 >= seats_available <= 50
  'Plenty' if seats_available > 50
  ======================================*/
    val seats =movies.withColumn("Availability", when(($"Seats_Available")<=10,"Full")
                                               .when(($"Seats_Available")>=11 && ($"Seats_Available")<=50,"Limited")
                                               .when(($"Seats_Available") >50,"Plenty"))
    seats.show()

/*# Filter showtimes where movie_title contains 'Action'.
=============================================================*/
    val filt = movies.filter(($"Movie_Title").contains("Action"))
    filt.show()

/*# Calculate the total (sum), average (avg), maximum (max) and minimum (min) seats_available for each availability.
===================================================================================================================*/
    val calc = seats.groupBy("Availability")
                                      .agg(sum($"Seats_Available").alias("Total Seats Available"),
                                        avg($"Seats_Available").alias("Average Seats Available"),
                                        max($"Seats_Available").alias("Maximum Seats Available"),
                                        min($"Seats_Available").alias("Minimum seats Available"))
   calc.show()

      spark.stop()
  }
}