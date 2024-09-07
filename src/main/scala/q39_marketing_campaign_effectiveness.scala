import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q39_marketing_campaign_effectiveness {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q39").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Campaign_Id","Investment_Amount","Return_On_Investment","Campaign_Date")

   val investment_category = Seq((1,60000,70000,"2028-05-01"),(2,30000,35000,"2028-05-05"),
                                 (3,15000,20000,"2028-05-10"),(4,40000,20000,"2028-05-15"),
                                (5,70000,80000,"2028-05-20"),(6,10000,12000,"2028-05-25"))

    val df = spark.createDataFrame(investment_category).toDF(schema:_*)
    df.show()

    /* Create a new column investment_category based on investment_amount:
          o 'High' if investment_amount > 50000
          o 'Medium' if 20000 >= investment_amount <= 50000
          o 'Low' if investment_amount < 20000
     ============================================================ */
     val df1 = df.withColumn("Investment_Category",
                   when($"Investment_Amount" > 50000,"High")
                   when($"Investment_Amount" >= 20000 && $"Investment_Amount" <= 50000, "Medium")
                   when($"Investment_Amount" < 20000,"Low"))
    df1.show()

/* Filter records where campaign_date is in 'May 2028'.
=========================================================== */

    val df2 = df.filter($"Campaign_Date".between("2028-05-01","2028-05-31"))
    df2.show()

/* For each investment_category, calculate the total (sum), average (avg), maximum (max), and minimum (min) return_on_investment.
=================================================================================================================================*/

    val df3 = df1.groupBy("Investment_Category")
                  .agg(sum($"Investment_Amount").alias("Total_Investment_Amount"),
                    avg($"Investment_Amount").alias("Average_Investment_Amount"),
                    max($"Investment_Amount").alias("Maximum_Investment_Amount"),
                    min($"Investment_Amount").alias("Minimum_Investment_Amount"))

    df3.show()

spark.stop()
  }
}
