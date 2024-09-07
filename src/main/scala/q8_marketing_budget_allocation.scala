import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q8_marketing_budget_allocation {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q8").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._
    val schema = Seq("Campaign_Id","Budget_Amount","Allocation_Date")

    val marketing_budget = Seq((1,25000,"2026-01-01"),(2,15000,"2026-01-05"),(3,8000,"2026-01-10"),
                                (4,18000,"2026-01-15"),(5,30000,"2026-01-20"),(6,6000,"2026-01-25"))

    val df = spark.createDataFrame(marketing_budget).toDF(schema:_*)
    df.show()

    /* Create a new column budget_category based on budget_amount:
              'High' if budget_amount > 20000
              'Medium' if 10000 <= budget_amount <= 20000
               'Low' if budget_amount < 10000
           ========================================================== */
    val budget_category =df.withColumn("Budget_Category",
                             when($"Budget_Amount" > 20000,"High")
                               .when($"Budget_Amount" >= 10000 && $"Budget_Amount" <= 20000,"Medium")
                               .when($"Budget_Amount" < 10000,"Low"))
    budget_category.show()

/* Filter records where allocation_date is in 'January 2026'.
======================================================================  */
    val df1 = df.filter($"Allocation_Date".between("2026-01-01","2026-01-31"))
    df1.show()

/*  Calculate the total (sum), average (avg), maximum (max), and minimum (min) budget_amount for each budget_category.
======================================================================================================================*/

    val df2 = budget_category.groupBy("Budget_Category")
                                .agg(sum($"Budget_Amount").alias("Total_Budget_Amount"),
                                  avg($"Budget_Amount").alias("Average_Budget_Amount"),
                                  max($"Budget_Amount").alias("Maximum_Budget_Amount"),
                                  min($"Budget_Amount").alias("Minimum_Budget_Amount"))
    df2.show()

spark.stop()
  }
}