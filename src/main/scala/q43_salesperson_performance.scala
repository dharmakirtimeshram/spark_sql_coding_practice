import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object q43_salesperson_performance {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb43").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema = Seq("Salesperson_Id","Sales_Amount","Sales_Date")

    val sales_performance = Seq((1,25000,"2024-12-01"),(2,15000,"2024-12-05"),(3,8000,"2024-12-10"),
                                 (4,12000,"2024-12-15"),(5,18000,"2024-12-20"),(6,5000,"2024-12-25"))

      val df = spark.createDataFrame(sales_performance).toDF(schema:_*)
    df.show()

    /*  #Create a new column performance_category based on sales_amount:
          'Top Performer' if sales_amount > 20000
         'Average Performer' if 10000 >= sales_amount <= 20000
          'Low Performer' if sales_amount < 10000
          ====================================================== */
    val performance_category =df.withColumn("Performance_Category",
                                 when($"Sales_Amount" > 20000,"Top Performer")
                                   .when($"Sales_Amount" >= 10000 && $"Sales_Amount" <= 20000,"Average Performer")
                                   .when($"Sales_Amount" < 10000,"Low Performer"))
    performance_category.show()

/* # Filter records where sales_date is in 'December 2024'.
============================================================= */
    val df1 = df.filter($"Sales_Date".between("2024-12-01","2024-12-31"))
    df1.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) sales_amount for each performance_category.
=======================================================================================================================*/

    val df2 = performance_category.groupBy("Performance_Category")
                 .agg(sum($"Sales_Amount").alias("Total Sales Amount"),
                   avg($"Sales_Amount").alias("Average Sales Amount"),
                   max($"Sales_Amount").alias("Maximum Sales Amount"),
                   min($"Sales_Amount").alias("Minimum Sales Amount"))
    df2.show()

  spark.stop()
  }
}
