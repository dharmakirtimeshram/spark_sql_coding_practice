import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q32_corporate_training_expenses {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb32").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val schema =Seq("Expenses_Id","Department","Expense_Amount","Expense_Date")

    val training_expenses = Seq((1,"HR",3500,"2024-07-01"),(2,"IT",1200,"2024-07-05"),(3,"HR",600,"2024-07-10"),
                                (4,"HR",2500,"2024-07-15"),(5,"IT",800,"2024-07-15"),(6,"HR",4000,"2024-07-25"))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(training_expenses)).toDF(schema:_*)

    df.show()

    /* #Create a new column expense_category based on expense_amount:
   'High' if expense_amount > 3000
   'Medium' if 1000 >= expense_amount <= 3000
    'Low' if expense_amount < 1000
    ===================================================== */

    val expense_category = df.withColumn("Expense_Category",
                  when($"Expense_Amount" > 3000,"High")
                    .when($"Expense_Amount" >= 1000 && $"Expense_Amount" <= 3000,"Medium")
                    .when($"Expense_Amount" < 1000,"Low"))

    expense_category.show()

 /* #Filter expenses where department starts with 'HR'.
  ===================================================*/
    val df2 = df.filter($"Department".startsWith("HR"))
    df2.show

/* # Calculate the total (sum), average (avg), maximum (max), and minimum (min) expense_amount for each expense_category.
========================================================================================================================*/

    val df4 = expense_category.groupBy("Expense_Category")
                          .agg(sum($"Expense_Amount").alias("Total Expense Amount"),
                            avg($"Expense_Amount").alias("Average Expense Amount"),
                            max($"Expense_Amount").alias("Maximum Expense Amount"),
                            min($"Expense_Amount").alias("Minimum Expense Amount"))
    df4.show()

  spark.stop()
  }
}
