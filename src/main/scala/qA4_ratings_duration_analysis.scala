import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object qA4_ratings_duration_analysis {
  def main(args:Array[String]):Unit={

    val sparkconf = new SparkConf().setAppName("q4").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val movies_data =List((1,"The Matrix",9,136),(2,"Inception",8,148),(3,"The Godfather",9,175),(4,"toy Story",7,81),
                            (5,"The Shawshank Redemption",10,142),(6,"The Silence of the lambs",8,118))
                            .toDF("Id","Name","Rating","Duration_minutes")
  //  df.show()

    /* #Create a new column rating_category based on rating:
   'Excellent' if rating >= 8
   'Good' if 6 >= rating < 8
  'Average' if rating < 6
  ================================  */

    val rating_category = movies_data.select(col("Id"),col("Name"),col("Rating"),col("Duration_minutes"),
                              when(col("Rating")>=8,"Excellent")
                              .when(col("Rating") >= 6 && col("Rating") < 8,"Good")
                              .when(col("Rating")<6,"Average").alias("Rating Category"))

   // rating_category.show()

   /* #Create a new column duration_category based on duration_minutes:
    'Long' if duration_minutes > 150
    'Medium' if 90 >= duration_minutes <= 150
    'Short' if duration_minutes < 90
    ===============================================================    */

    val duration_category = movies_data.withColumn("Duration Category",
                                    when(col("Duration_minutes")>150,"Long")
                                   .when(col("Duration_minutes")>= 90 && col("Duration_minutes")<=150,"Medium")
                                    .when(col("Duration_minutes")<90,"Short"))
    duration_category.show()

    /*  #Filter movies whose movie_name starts with 'T'.
    =======================================================     */

    val filt = movies_data.filter(col("Name").startsWith("T"))
   filt.show()

    /*  Filter movies whose movie_name ends with 'e'.
   ====================================================     */

    val end_with = movies_data.filter(col("Name").endsWith("e"))
   end_with.show()


    /*  #Calculate the total (sum), average (avg), maximum (max), and minimum (min) duration_minutes for each rating_category
    ==========================================================================================================================     */

    val calc = rating_category.groupBy("Rating category")
                               .agg(sum(col("Duration_minutes")).alias("Total Duration Minutes"),
                                avg(col("Duration_minutes")).alias("Avg Duration Minutes"),
                                 max(col("Duration_minutes")).alias("Maximum Duration Minutes"),
                                 min(col("Duration_minutes")).alias("Minimum Duration Minutes"))
    calc.show()

    spark.stop()
  }

}
