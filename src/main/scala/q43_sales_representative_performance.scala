import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q43_sales_representative_performance {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q43").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Rep_Id","Sales_Amount","Target_Amount","Report_Date")

    val sales_performance = Seq((1,120000,100000,"2028-09-01"),(2,95000,90000,"2028-09-05"),
                                (3,85000,90000,"2028-09-10"),(4,110000,100000,"2028-09-15"),
                                (5,75000,80000,"2028-09-20"),(6,75000,70000,"2028-09-25"))
    val df = spark.createDataFrame(sales_performance).toDF(schema:_*)
     df.show()

    /*  Create a new column performance_status based on sales_amount and target_amount:
          o 'Exceeded Target' if sales_amount > target_amount * 1.1
          o 'Met Target' if sales_amount >= target_amount * 1.1
          o 'Below Target' if sales_amount < target_amount * 1.1
     ==================================================================== */
    val df1 = df.withColumn("performance_Status",
                   when($"Sales_Amount" > $"Target_Amount" * 1.1 ,"Exceeded")
                   .when($"Sales_Amount" >= $"Target_Amount" * 1.1, "Met Target")
                   .when($"Sales_Amount" < $"Target_Amount" * 1.1 , "Below Target"))

    df1.show()

/*  Filter records where report_date is in 'September 2028'.
================================================================ */
    val df2 = df.filter($"Report_Date".between("2028-09-01","2028-09-30"))
    df2.show()


/* For each performance_status, calculate the total (sum), average (avg), maximum (max), and minimum (min) sales_amount.
==========================================================================================================================*/

    val df3 = df1.groupBy("Performance_Status")
                   .agg(sum($"Sales_Amount").alias("Total_Sales_Amount"),
                     avg($"Sales_Amount").alias("Average_Sales_Amount"),
                     max($"Sales_Amount").alias("Maximum_Sales_Amount"),
                     min($"Sales_Amount").alias("Minimum_Sales_Amount"))

    df3.show()

spark.stop()
  }
}