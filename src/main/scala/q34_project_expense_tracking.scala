import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q34_project_expense_tracking {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb34").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema =Seq("Expenses_Id","Project_Name","Expense_Amount","Expense_Date")
    val project_expenses = Seq((1,"Development_Project",8000,"2024-09-01"),(2,"Development_plan",4500,"2024-09-05"),
                            (3,"Marketing_campaign",2500,"2024-09-10"),(4,"Development_Phase",3000,"2024-09-10"),
                            (5,"Development_Task",1000,"2024-09-20"),(6,"R&D_Project",1500,"2024-09-25"))

      val data = spark.sparkContext.parallelize(project_expenses)
      val  df = spark.createDataFrame(data).toDF(schema:_*)
    df.show()

    /* #Create a new column expense_type based on expense_amount:
    'High' if expense_amount > 7000
    'Medium' if 3000 >= expense_amount <= 7000
    'Low' if expense_amount < 3000
    ====================================== */
    val expense_type = df.withColumn("Expense_Type",
                        when($"Expense_Amount" > 7000,"High")
                          .when($"Expense_Amount" >= 3000 && $"Expense_Amount" <= 7000,"Medium")
                          .when($"Expense_Amount" < 3000,"Low"))
        expense_type.show()

/* #Filter expenses where project_name contains 'Development'.
================================================================== */
    val df1 =df.filter($"Project_Name".contains("Development"))
           df1.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) expense_amount for each expense_type.
===================================================================================================================== */

         val df2 = expense_type.groupBy("Expense_Type")
                               .agg(sum($"Expense_Amount").alias("Total_Expense_Amount"),
                                 avg($"Expense_Amount").alias("Average_Expense_Amount"),
                                 max($"Expense_Amount").alias("Maximum_Expense_Amount"),
                                 min($"Expense_Amount").alias("Minimum_Expense_Amount"))

      df2.show()
    spark.stop()
  }
}
