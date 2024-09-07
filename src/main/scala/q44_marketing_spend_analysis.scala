import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q44_marketing_spend_analysis {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q44").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Campaign_Id","Spend_Amount","Expected_Revenue","Campaign_Date")

    val marketing_spend = Seq((1,60000,150000,"2028-10-01"),(2,30000,80000,"2028-10-05"),
                              (3,15000,30000,"2028-10-10"),(4,40000,100000,"2028-10-15"),
                               (5,70000,100000,"2028-10-20"),(6,12000,25000,"2028-10-25"))

    val df = spark.createDataFrame(marketing_spend).toDF(schema:_*)
    df.show()

/*Create a new column spend_efficiency calculated as:
      o expected_revenue / spend_amount
============================================== */

    val df1 = df.withColumn("Spend_Efficiency", $"Expected_Revenue" / $"Spend_Amount")
           df1.show()

/* Create a new column spend_category based on spend_amount:
      o 'High' if spend_amount > 50000
      o 'Medium' if 20000 >= spend_amount <= 50000
      o 'Low' if spend_amount < 20000
  ================================================ */

    val df2 = df1.withColumn("Spend_Category",
                   when($"Spend_Amount" > 50000,"High")
                     .when($"Spend_Amount" >= 20000 && $"Spend_Amount" <= 50000,"Medium")
                     .when($"Spend_Amount" < 20000,"Low"))
    df2.show()

/*  Filter records where campaign_date is in 'October 2028'.
================================================================= */

    val df3 = df2.filter($"Campaign_Date".between("2028-10-01","2028-10-31"))
       df3.show()

/*  For each spend_category, calculate the total (sum), average (avg), maximum (max), and minimum (min) expected_revenue.
========================================================================================================================*/

    val df4 = df2.groupBy("Spend_Category")
                 .agg(sum($"Expected_Revenue").alias("Total_Expected_Revenue"),
                   avg($"Expected_Revenue").alias("Average_Expected_Revenue"),
                   max($"Expected_Revenue").alias("Maximum_Expected_Revenue"),
                   min($"Expected_Revenue").alias("Minimum_Expected_Revenue"))
    df4.show()

    spark.stop()
  }
}
