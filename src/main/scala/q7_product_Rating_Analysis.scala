import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q7_product_Rating_Analysis {
  def main(args:Array[String]):Unit={

    val sparkconf = new SparkConf().setAppName("qb7").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import  spark.implicits._

    val data = List((1,"Smartphone",4,"2024-01-15"),(2,"Speaker",3,"2024-01-20"),(3,"Smartwatch",4,"2024-02-15"),
                    (4,"Screen",2,"2024-02-20"),(5,"Speaker",2,"2024-03-05"),(6,"Soundbar",3,"2024-03-12"),
                     (7,"Smartphone",3,"2024-04-15"),(8,"Headphone",2,"2024-04-25"),(9,"Speaker",4,"2024-05-06"),
                     (10,"Headphone",3,"2024-05-23"),(11,"Smartwatch",4,"2024-06-08"),(12,"Smartphone",4,"2024-06-24"))
                     .toDF("Review_Id","Product_Name","Rating","Review_Date")
         //  data.show()
    /*#Create a new column rating_category based on rating:
  'High' if rating >= 4
  'Medium' if 3 >= rating < 4
  'Low' if rating < 3
  ==============================================================*/

    val rating_category = data.withColumn("Rating_Category",
                                         when(($"Rating")>=4,"High")
                                         .when(($"Rating")>=3 && ($"Rating")<4, "Medium")
                                         .when(($"Rating")<3,"Low"))
   // rating_category.show()

    /*#Filter reviews where product_name starts with 'S'.
    =======================================================*/

    val filt = data.filter(($"Product_Name").startsWith("S"))
   // filt.show()

    /*#Calculate the total count of reviews and average rating for each rating_category.
    ===================================================================================*/
    val calc = rating_category.groupBy("Rating_Category").agg(count($"Rating").alias("Total Count of Reviews"),
                                                          avg($"Rating").alias("Average Rating"))
    calc.show()

    spark.stop()
  }

}
