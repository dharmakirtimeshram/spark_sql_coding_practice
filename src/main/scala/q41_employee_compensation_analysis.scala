import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q41_employee_compensation_analysis {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q41").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Employee_Id","Base_Salary","Bonus","Deductions","Review_Date")

    val employee_compensation = Seq((1,90000,15000,5000,"2028-07-01"),(2,60000,8000,4000,"2028-07-05"),
                                    (3,40000,3000,2000,"2028-07-10"),(4,75000,10000,6000,"2028-07-15"),
                                    (5,85000,12000,4000,"2028-07-20"),(6,55000,5000,3000,"2028-07-25"))

    val df = spark.createDataFrame(employee_compensation).toDF(schema:_*)
    df.show()

    /* Create a new column net_compensation calculated as:
            o base_salary + bonus - deductions
        =======================================================*/

    val df1 = df.withColumn("Net_Compensation",$"Base_Salary" + $"Bonus" - $"Deductions" )
                   df1.show()

/* Create a new column compensation_category based on net_compensation:
         o 'High' if net_compensation > 100000
         o 'Medium' if 50000 >= net_compensation <= 100000
         o 'Low' if net_compensation < 50000
     ============================================================= */

    val df2 = df1.withColumn("Compensation_Category",
                      when($"Net_Compensation" > 100000,"High")
                      when($"Net_Compensation" >= 50000 && $"Net_Compensation" <= 100000,"Medium")
                      when($"Net_Compensation" < 50000,"Low"))
    df2.show()

/*  Filter records where review_date is in 'July 2028'.
========================================================== */

    val df3 = df.filter($"Review_Date".between("2028-07-01","2028-07-31"))
    df3.show()

/* For each compensation_category, calculate the total (sum), average (avg), maximum (max), and minimum (min) net_compensation.
==============================================================================================================================*/

    val df4 = df2.groupBy("Compensation_Category")
                         .agg(sum($"Net_Compensation").alias("Total_Net_Compensation"),
                           avg($"Net_Compensation").alias("Average_Net_Compensation"),
                           max($"Net_Compensation").alias("Maximum_Net_Compensation"),
                           min($"Net_Compensation").alias("Maximum_Net_Compensation"))
    df4.show()

  spark.stop()

  }
}
