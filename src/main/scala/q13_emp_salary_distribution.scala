import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q13_emp_salary_distribution {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb13").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val salaries =List((1,"IT",130000,"2024-01-10"),(2,"HR",80000,"2024-01-15"),(3,"IT",60000,"2024-02-20"),
                       (4,"IT",70000,"2024-02-25"),(5,"Sales",50000,"2024-02-25"),(6,"IT",90000,"2024-03-12"))
                     .toDF("Emp_id","Department","Salary","Last_Increment_Date")
       salaries.show()

    /*# Create a new column salary_band based on salary:
 'High' if salary > 120000
 'Medium' if 60000 >= salary <= 120000
 'Low' if salary < 60000
 =============================================*/
      val salary_band = salaries.withColumn("Salary_Band", when(($"Salary")>120000,"High")
                                                       .when(($"Salary") >=60000 && ($"Salary")<=120000,"Medium")
                                                       .when(($"Salary") <60000,"Low"))
   salary_band.show()

/*#Filter salaries where department starts with 'IT'.
=======================================================*/
    val filt = salaries.filter(($"Department").startsWith("IT"))
    filt.show()

/*#Calculate the total (sum), average (avg), maximum (max), and minimum (min) salary for each salary_band.
=========================================================================================================*/
    val calc = salary_band.groupBy("Salary_Band")
                                             .agg(sum($"Salary").alias("Total Salary"),
                                               avg($"Salary").alias("Average salary"),
                                               max($"Salary").alias("Maximum Salary"),
                                               min($"Salary").alias("Minimum Salary"))
    calc.show()

 spark.stop()
  }
}
