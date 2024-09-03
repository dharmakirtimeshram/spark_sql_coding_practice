import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q3_emp_work_hr_analysis {
  def main(args:Array[String]):Unit={
    val sparkconf = new SparkConf().setAppName("qb3").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val data = List((1,"2024-01-10",9,"Sales"),(2,"2024-01-11",7,"Support"),(3,"2024-01-12",9,"Marketing"),
                    (4,"2024-01-13",10,"Marketing"), (5,"2024-01-14",5,"Sales"),(6,"2024-01-15",6,"Support"),
                    (7,"2024-01-16",9,"Sales"),(8,"2024-01-17",7,"Support"),(9,"2024-01-18",10,"Sales"),
                    (10,"2024-01-19",8,"Marketing"),(11,"2024-01-20",6,"Support"),(12,"2024-01-21",8,"Sales"))
                    .toDF("Employee_id","Work_date","Hours_worked","Department")

       //data.show()

    /* #Create a new column hours_category based on hours_worked:
   'Overtime' if hours_worked > 8
   'Regular' if hours_worked <= 8
   ================================================================  */

    val hrs_category = data.withColumn("Hours_category",when(($"Hours_worked")>8,"Overtime")
                               .when(($"Hours_worked")<=8,"Regular"))
           hrs_category.show()

    /* #Filter work hours where department starts with 'S'.
    =====================================================*/
    val filt = data.filter(($"Department").startsWith("S"))

           filt.show()

    /* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) hours_worked for each department.
    ================================================================================================================ */

    val calc = data.groupBy("Department").agg(sum($"Hours_worked").alias("Total worked Hours"),
                                           avg($"Hours_worked").alias("Average Hours"),
                                            max($"Hours_worked").alias("Maximum Worked Hours"),
                                            min($"Hours_worked").alias("Minimum worked Hours"))
    calc.show()

   spark.stop()
  }
}
