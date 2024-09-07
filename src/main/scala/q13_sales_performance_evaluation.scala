import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q13_sales_performance_evaluation {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q13").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Sales_Person","Sales_Amount","Sales_Date")

    val sales_performance = Seq(("A",35000,"2026-05-01"),("B",20000,"2026-05-05"),("C",8000,"2026-05-10"),
                                ("D",12000,"2026-05-15"),("E",40000,"2026-05-15"),("F",4000,"2026-05-25"))

    val df = spark.createDataFrame(sales_performance).toDF(schema:_*)

    df.show()

    /*  Create a new column performance_grade based on sales_amount:
             'Excellent' if sales_amount > 30000
              'Good' if 15000 >= sales_amount <= 30000
              'Average' if 5000 >= sales_amount < 15000
              'Below Average' if sales_amount < 5000
         ============================================================ */
    val performance_grade = df.withColumn("Performance_Grade",
                               when($"Sales_Amount" > 30000,"Excellent")
                                 .when($"Sales_Amount" >= 15000 && $"Sales_Amount" < 30000,"Good")
                                 .when($"Sales_Amount" >= 5000 && $"Sales_Amount" < 15000,"Average")
                                 .when($"Sales_Amount" < 5000,"Below Average"))
    performance_grade.show()

/*  Filter records where sales_date is in 'May 2026'.
======================================================= */
    val df1 = df.filter($"Sales_Date".between("2026-05-01","2026-05-31"))
    df1.show()

/*  Calculate the total (sum), average (avg), maximum (max), and minimum (min) sales_amount for each performance_grade.
======================================================================================================================*/
    val df2 = performance_grade.groupBy("Performance_Grade")
                                 .agg(sum($"Sales_Amount").alias("Total_Sales_Amount"),
                                   avg($"Sales_Amount").alias("Average_Sales_Amount"),
                                   max($"Sales_Amount").alias("Maximum_Sales_Amount"),
                                   min($"Sales_Amount").alias("Minimum_Sales_Amount"))
    df2.show()


    spark.stop()
  }
}