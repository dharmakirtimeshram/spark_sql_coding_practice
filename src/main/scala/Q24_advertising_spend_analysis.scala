import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q24_advertising_spend_analysis {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q24").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Ad_Campaign_Id","Spend_Amount","Spend_Date")

    val ad_spend = Seq((1,35000,"2027-04-01"),(2,18000,"2027-04-5"),(3,12000,"2027-04-10"),
                        (4,25000,"2027-04-15"),(5,40000,"2027-4-20"),(6,10000,"2027-04-25"))

    val df = spark.createDataFrame(ad_spend).toDF(schema:_*)
    df.show()

    /* Create a new column spend_category based on spend_amount:
          o 'High' if spend_amount > 30000
          o 'Medium' if 15000 >= spend_amount <= 30000
          o 'Low' if spend_amount < 15000
      ====================================================== */
    val spend_category = df.withColumn("Spend_Category",
                           when($"Spend_Amount" > 30000, "High")
                             .when($"Spend_Amount" >= 15000 && $"Spend_Amount" <= 30000, "Medium")
                             .when($"Spend_Amount" < 15000, "Low"))
    spend_category.show()

/* Filter records where spend_date is in 'April 2027'.
======================================================== */
    val df1 = df.filter($"Spend_Date".between("2027-04-01","2027-04-30"))
    df1.show()

/* Calculate the total (sum), average (avg), maximum (max), and minimum (min) spend_amount for each spend_category.
=====================================================================================================================*/
    val df2 = spend_category.groupBy("Spend_Category")
                             .agg(sum($"Spend_Amount").alias("Total_Spend_Amount"),
                               avg($"Spend_Amount").alias("Average_Send_Amount"),
                               max($"Spend_Amount").alias("Maximum_Send_Amount"),
                               min($"Spend_Amount").alias("Minimum_Send_Amount"))
    df2.show()

spark.stop()
  }
}
