import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q17_project_budget_strategy {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb17").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val budget =List((1,"New Website",50000,55000),(2,"Old Software",30000,25000),(3,"New App",40000,40000),
                      (4,"New Marketing",15000,10000),(5,"Old Campaign",20000,18000),(6,"New Research",60000,70000))

    val df = spark.createDataFrame(budget).toDF("Project_id", "Project_name", "Budget", "Spent_amount")
    df.show()

   /* #Create a new column budget_status based on the difference between budget and spent_amount:
      'Over Budget' if spent_amount > budget
    'On Budget' if spent_amount == budget
    'Under Budget' if spent_amount < budget
    =============================================*/
    val budget_status =df.withColumn("Budget_Status",
                                        when($"Spent_amount" > $"Budget","Over_Budget")
                                          .when($"Spent_amount" === $"Budget","On_Budget")
                                          .when($"Spent_amount" < $"Budget","Under_Budget"))
    budget_status.show()

    /*# Filter projects where project_name starts with 'New'.
    ==============================================================*/
    val filt_df = df.filter(($"Project_name").startsWith("New"))
    filt_df.show()

   /* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) spent_amount for each budget_status.
   ================================================================================================================== */
    val calc_df = budget_status.groupBy("Budget_Status")
                                       .agg(sum($"Spent_amount").alias("Total Spent Amount"),
                                         avg($"Spent_amount").alias("Average Spent Amount"),
                                         max($"Spent_amount").alias("Maximum Spent Amount"),
                                         min($"Spent_amount").alias("Minimum Spent Amount"))
    calc_df.show()

   spark.stop()
  }
}
