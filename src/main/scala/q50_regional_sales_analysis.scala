import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q50_regional_sales_analysis {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q50").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Region","Sales_Amount","Sales_Date")

    val regional_sales = Seq(("North",120000,"2029-03-01"),("South",70000,"2029-03-05"),
                             ("East",30000,"2029-03-10"), ("West",80000,"2029-03-15"),
                             ("Central",50000,"2029-03-20"),("North",15000,"2029-03-25"))

    val df = spark.createDataFrame(regional_sales).toDF(schema:_*)
    df.show()

    /* Create a new column sales_category based on sales_amount:
           o 'High' if sales_amount > 100000
           o 'Medium' if 50000 >= sales_amount <= 100000
           o 'Low' if sales_amount < 50000
       ============================================================  */

    val df1 =df.withColumn("Sales_Category",
                 when($"Sales_Amount" > 100000, "High")
                 when($"Sales_Amount" >= 50000 && $"Sales_Amount" <= 100000, "Medium")
                 when($"Sales_Amount" < 50000, "Low"))

    df1.show()

/* Create a new column quarter extracted from sales_date.
============================================================== */

    val df2 = df.withColumn("Quarter", quarter($"Sales_Date"))
    df2.show()

/* Filter records where sales_date is in 'March 2029'.
======================================================== */

    val df3 = df.filter($"Sales_Date".between("2029-03-01","2029-03-31"))
    df3.show()


/* For each region and sales_category, calculate the total (sum), average (avg), maximum (max),
            and minimum (min) sales_amount.
======================================================================================================*/

    val df4 =df1.groupBy("Region","Sales_Category")
                   .agg(
                     sum($"Sales_Amount").alias("Total_Sales_Amount"),
                     avg($"Sales_Amount").alias("Average_Sales_Amount"),
                     max($"Sales_Amount").alias("Maximum_Sales_Amount"),
                     min($"Sales_Amount").alias("Minimum_Sales_Amount")
                   )
      df4.show()

    spark.stop()
  }
}
