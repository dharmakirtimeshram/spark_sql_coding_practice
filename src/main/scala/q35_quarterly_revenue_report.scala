import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q35_quarterly_revenue_report {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q35").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Quarter","Revenue_Amount","Report_Date")

    val quarterly_revenue = Seq(("Q1",160000,"2028-01-01"),("Q2",80000,"2028-01-05"),("Q3",60000,"2028-01-10"),
                                 ("Q4",120000,"2028-01-15"),("Q5",2000000,"2028-01-20"),("Q6",50000,"2028-01-25"))
    val df = spark.createDataFrame(quarterly_revenue).toDF(schema:_*)
    df.show()

  /* Create a new column revenue_band based on revenue_amount:
       o 'High' if revenue_amount > 150000
       o 'Medium' if 70000 >= revenue_amount <= 150000
       o 'Low' if revenue_amount < 70000
    ==================================================== */
    val df1 = df.withColumn("Revenue_Band",
                  when($"Revenue_Amount" > 150000,"High")
                  .when($"Revenue_Amount" >= 70000 && $"Revenue_Amount" <= 150000,"Medium")
                  .when($"Revenue_Amount" < 70000,"Low"))
    df1.show()

/* Filter records where report_date is in 'January 2028'.
=============================================================*/
    val df2 = df.filter($"Report_Date".between("2028-01-01","2028-01-31"))
    df2.show()

/* Calculate the total (sum), average (avg), maximum (max), and minimum (min) revenue_amount for each revenue_band.
==================================================================================================================*/

    val df3 = df1.groupBy("Revenue_Band")
                  .agg(sum($"Revenue_Amount").alias("Total_Revenue_Amount"),
                    avg($"Revenue_Amount").alias("Average_Revenue_Amount"),
                    max($"Revenue_Amount").alias("Maximum_Revenue_Amount"),
                    min($"Revenue_Amount").alias("Minimum_Revenue_Amount"))
    df3.show()

spark.stop()
  }
}