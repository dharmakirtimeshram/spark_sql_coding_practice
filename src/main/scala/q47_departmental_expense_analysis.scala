import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q47_departmental_expense_analysis {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb47").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema = Seq("Expense_Id","Department","Expense_Amount","Expense_Date")

    val dept_expenses = Seq((1,"Finance",12000,"2025-03-01"),(2,"Finance",6000,"2025-03-05"),
                            (3,"HR",3000,"2025-03-10"),(4,"Finance",8000,"2025-03-15"),
                            (5,"Finance",15000,"2025-03-20"),(6,"IT",4000,"2025-03-25"))
       val df = spark.createDataFrame(dept_expenses).toDF(schema:_*)
       df.show()

    /* #Create a new column expense_type based on expense_amount:
      'High' if expense_amount > 10000
    'Medium' if 5000 >= expense_amount <= 10000
    'Low' if expense_amount < 5000
    ================================================ */
    val expense_type =df.withColumn("Expense_TYpe",
                        when($"Expense_Amount" > 10000, "High")
                          .when($"Expense_Amount" >= 5000 && $"Expense_Amount" <= 10000 , "Medium")
                          .when($"Expense_Amount" < 5000, "Low"))
      expense_type.show()

   /* #Filter records where department starts with 'Finance'.
    ========================================================== */

      val df1 = df.filter($"Department".startsWith("Finance"))

               df1.show()

  /*  #Calculate the total (sum), average (avg), maximum (max), and minimum (min) expense_amount for each expense_type.
    ==================================================================================================================== */

      val df2 = expense_type.groupBy("Expense_Type")
                            .agg(sum($"Expense_Amount").alias("Total Expense Amount"),
                              avg($"Expense_Amount").alias("Average Expense Amount"),
                              max($"Expense_Amount").alias("Maximum Expense Amount"),
                              min($"Expense_Amount").alias("Total Expense Amount"))
    df2.show()
    spark.stop()
  }
}
