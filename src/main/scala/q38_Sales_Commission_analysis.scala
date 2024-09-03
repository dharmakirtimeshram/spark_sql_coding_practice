import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q38_Sales_Commission_analysis {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb38").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema = Seq("Sales_Id","SalesPerson_Name","Commission_Amount","Commission_Date")

    val sales_commission = Seq((1,"Alice Johnson",12000,"2025-01-01"),(2,"Bob Smith",7000,"2025-01-05"),
                               (3,"Charlie Davis",3000,"2025-01-10"),(4,"David Lee",8000,"2025-01-15"),
                                (5,"Emily Clark",11000,"2025-01-20"),(6,"Frank Adams",2000,"2025-01-25"))
        val df = sales_commission.toDF(schema:_*)
    df.show()

    /*  # Create a new column commission_level based on commission_amount:
   'High' if commission_amount > 10000
   'Medium' if 5000 >= commission_amount <= 10000
   'Low' if commission_amount < 5000
   ============================================== */
    val commission_level = df.withColumn("Commission_Level",
                             when($"Commission_Amount" > 10000,"High")
                               .when($"Commission_Amount" >= 5000 && $"Commission_Amount" <= 10000,"Medium")
                               .when($"Commission_Amount" < 5000,"Low"))
      commission_level.show()

/* # Filter commissions where commission_date is in 'January 2025'.
================================================================ */

    val df1 = df.filter($"Commission_Date".between("2025-01-01","2025-01-31"))
         df1.show()
/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) commission_amount for each commission_level.
========================================================================================================================= */
    val df2 = commission_level.groupBy("Commission_Level")
                           .agg(sum($"Commission_Amount").alias("Total Commission"),
                             avg($"Commission_Amount").alias("Average Commission"),
                             max($"Commission_Amount").alias("Maximum Commission"),
                             min($"Commission_Amount").alias("Minimum Commission"))
    df2.show()
   spark.stop()
  }
}
