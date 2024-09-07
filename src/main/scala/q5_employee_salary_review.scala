import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q5_employee_salary_review {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q5").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Employee_Id","Salary_Amount","Review_date")

    val emp_salaries = Seq((1,90000,"2025-10-01"),(2,50000,"2025-10-05"),(3,35000,"025-10-10"),
                            (4,60000,"2025-10-15"),(5,85000,"2025-10-20"),(6,30000,"2025-10-25"))

    val df = spark.createDataFrame(emp_salaries).toDF(schema:_*)
    df.show()

    /* # Create a new column salary_grade based on salary_amount:
    'High' if salary_amount > 80000
    'Medium' if 40000 >= salary_amount <= 80000
    'Low' if salary_amount < 40000
    ==================================================  */
    val salary_grade =df.withColumn("Salary_Grade",
                          when($"Salary_Amount" > 80000,"High")
                            .when($"Salary_Amount" >= 40000 && $"Salary_Amount" <= 80000,"Medium")
                            .when($"Salary_Amount" < 40000,"Low"))
    salary_grade.show()

/* # Filter records where review_date is in 'October 2025'.
=========================================================== */
    val df1 =df.filter($"Review_Date".between("2025-10-01","2025-10-31"))
    df1.show()

/* # Calculate the total (sum), average (avg), maximum (max), and minimum (min) salary_amount for each salary_grade.
===================================================================================================================*/
    val df2 = salary_grade.groupBy("Salary_Grade")
                           .agg(sum($"Salary_Amount").alias("Total_Salary_Amount"),
                             avg($"Salary_Amount").alias("Average_Salary_Amount"),
                             max($"Salary_Amount").alias("Maximum_Salary_Amount"),
                             min($"Salary_Amount").alias("minimum_Salary_Amount"))
    df2.show()

spark.stop()
  }
}