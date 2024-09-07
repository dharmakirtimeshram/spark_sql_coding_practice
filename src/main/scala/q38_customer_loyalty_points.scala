import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q38_customer_loyalty_points {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q38").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Customer_Id","Loyalty_Points","Transaction_Date")

    val customer_loyalty = Seq((1,12000,"2028-04-01"),(2,6000,"2028-04-05"),(3,4000,"2028-04-10"),
                               (4,8000,"2028-04-15"),(5,15000,"2028-04-20"),(6,3000,"2028-04-25"))

    val df = spark.createDataFrame(customer_loyalty).toDF(schema:_*)
    df.show()

    /* Create a new column points_category based on loyalty_points:
         o 'Gold' if loyalty_points >= 10000
         o 'Silver' if 5000 >= loyalty_points < 10000
         o 'Bronze' if loyalty_points < 5000
    =========================================================== */
    val df1 = df.withColumn("Points_Category",
                   when($"Loyalty_Points" >= 10000, "Gold")
                   .when($"Loyalty_Points" >= 5000 && $"Loyalty_Points" < 10000, "Silver")
                   .when($"Loyalty_Points" < 5000, "Bronze"))
    df1.show()

/* Filter records where transaction_date is in 'April 2028'.
============================================================== */

    val df2 = df.filter($"Transaction_Date".between("2028-04-01","2028-04-31"))
    df2.show()

/* Calculate the total (sum), average (avg), maximum (max), and minimum (min) loyalty_points for each points_category.
======================================================================================================================*/

    val df3 = df1.groupBy("Points_Category")
                     .agg(sum($"Loyalty_Points").alias("Total_Loyalty_Points"),
                       avg($"Loyalty_Points").alias("Average_Loyalty_Points"),
                       max($"Loyalty_Points").alias("Maximum_Loyalty_Points"),
                       min($"Loyalty_Points").alias("Minimum_Loyalty_Points"))
    df3.show()

spark.stop()
  }
}

