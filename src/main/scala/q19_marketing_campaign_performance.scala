import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q19_marketing_campaign_performance {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q19").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Campaign_Id","Campaign_Cost","Campaign_Date")

    val marketing_campaign = Seq((1,25000,"2026-11-01"),(2,15000,"2026-11-05"),(3,8000,"2026-11-10"),
                                 (4,18000,"2026-11-15"),(5,30000,"202-11-15"),(6,5000,"2026-11-25"))
    val df = spark.createDataFrame(marketing_campaign).toDF(schema:_*)
    df.show()

    /* Create a new column campaign_category based on campaign_cost:
          o 'High' if campaign_cost > 20000
          o 'Medium' if 10000 >= campaign_cost <= 20000
          o 'Low' if campaign_cost < 10000
   ================================================================ */

    val campaign_category = df.withColumn("Campaign_Category",
                               when($"Campaign_Cost" > 20000,"High")
                                 .when($"Campaign_Cost" >= 10000 && $"Campaign_Cost" <= 20000,"Medium")
                                 .when($"Campaign_Cost" < 10000,"Low"))
    campaign_category.show()

/* Filter records where campaign_date is in 'November 2026'.
==================================================================  */

    val df1 = df.filter($"Campaign_Date".between("2026-11-01","2026-11-30"))
   df1.show()

/* Calculate the total (sum), average (avg), maximum (max), and minimum (min) campaign_cost for each campaign_category.
======================================================================================================================*/

    val df2 = campaign_category.groupBy("Campaign_Category")
                                .agg(sum($"Campaign_Cost").alias("Total_Campaign_Cost"),
                                  avg($"Campaign_Cost").alias("Average_Campaign_Cost"),
                                  max($"Campaign_Cost").alias("Maximum_Campaign_Cost"),
                                  min($"Campaign_Cost").alias("Minimum_Campaign_Cost"))

    df2.show()

    spark.stop()
  }
}
