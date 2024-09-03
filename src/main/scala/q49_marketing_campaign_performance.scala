import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q49_marketing_campaign_performance {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb49").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema = List("Campaign_Id","Campaign_Cost","Campaign_Date")
    val marketing_campaign  = List((1,27000,"2025-05-01"),(2,15000,"2025-05-05"),(3,7000,"2025-05-10"),
                                   (4,18000,"2025-05-15"),(5,30000,"2025-05-20"),(6,5000,"2025-05-25"))
     val df = spark.createDataFrame(marketing_campaign).toDF(schema:_*)
    df.show()

    /* #Create a new column performance_category based on campaign_cost:
    'High' if campaign_cost > 25000
    'Medium' if 10000 >= campaign_cost <= 25000
   'Low' if campaign_cost < 10000
   =======================================================================  */
    val performance_category = df.withColumn("Performance_Category",
                                     when($"Campaign_Cost" > 25000,"High")
                                       .when($"Campaign_Cost" >= 10000 && $"Campaign_Cost" <= 25000,"Medium")
                                       .when($"Campaign_Cost" < 10000,"Low"))
    performance_category.show()

/* #Filter records where campaign_date is in 'May 2025'.
==========================================================  */
    val df1 = df.filter($"Campaign_Date".between("2025-05-01","2025-05-31"))
    df1.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) campaign_cost for each performance_category.
========================================================================================================================*/

    val df2 = performance_category.groupBy("Performance_Category")
                                    .agg(sum($"Campaign_Cost").alias("Total_Campaign_Cost"),
                                      avg($"Campaign_Cost").alias("Average_Campaign_Cost"),
                                      max($"Campaign_Cost").alias("Maximum_Campaign_Cost"),
                                      min($"Campaign_Cost").alias("Maximum_Campaign_Cost"))
    df2.show()

spark.stop()
  }
}
