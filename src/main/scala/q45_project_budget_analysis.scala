import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q45_project_budget_analysis {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q45").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Project_Id","Allocated_Budget","Spent_Amount","Budget_Date")

    val project_budget = Seq((1,100000,120000,"2028-11-01"),(2,80000,75000,"2028-11-05"),
                              (3,50000,50000,"2028-11-10"),(4,90000,95000,"2028-11-15"),
                               (5,60000,70000,"2028-11-20"),(6,30000,25000,"2028-11-25"))
    val df = spark.createDataFrame(project_budget).toDF(schema:_*)
    df.show()

    /* Create a new column budget_status based on allocated_budget and spent_amount:
           o 'Over Budget' if spent_amount > allocated_budget
           o 'On Budget' if spent_amount <= allocated_budget
      ============================================================= */

    val df1 = df.withColumn("Budget_Status",
                 when($"Spent_Amount" > $"Allocated_Budget" , "Over Budget")
                 when($"Spent_Amount" <= $"Allocated_Budget" , "On Budget"))
    df1.show()

 /*  Create a new column budget_efficiency calculated as:
           o spent_amount / allocated_budget
       ============================================ */

    val df2 = df.withColumn("Budget_Efficiency", $"Spent_Amount" / $"Allocated_Budget")
    df2.show()

/*  Filter records where budget_date is in 'November 2028'.
================================================================ */

    val df3 = df.filter($"Budget_Date".between("2028-11-01","2028-11-30"))
    df3.show()

/*  For each budget_status, calculate the total (sum), average (avg), maximum (max), and minimum (min) spent_amount.
===================================================================================================================*/

    val df4 =df1.groupBy("Budget_Status")
                  .agg(sum($"Spent_Amount").alias("Total_Spent_Amount"),
                    avg($"Spent_Amount").alias("Average_Spent_Amount"),
                    max($"Spent_Amount").alias("Maximum_Spent_Amount"),
                    min($"Spent_Amount").alias("Minimum_Spent_Amount"))

    df4.show()

    spark.stop()
  }
}
