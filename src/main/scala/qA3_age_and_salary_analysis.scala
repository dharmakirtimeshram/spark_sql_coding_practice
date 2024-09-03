import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object qA3_age_and_salary_analysis {
  def main(args:Array[String]):Unit={

    val sparkconf = new SparkConf().set("spark.app.name","q3").set("spark.master","local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val data = List((1,"John",28,60000),(2,"Jane",32,75000),(3,"Mike",45,120000),(4,"Alice",55,90000),(5,"Steve",62,110000),
                   (6,"Claire",40,40000),(7,"Piter",45,55000),(8,"kat",62,80000),(9,"Jim",56,92000),(10,"Sam",38,75000))
                   .toDF("Id","Name","Age","Salary")
   //data.show()

    /*#Create a new column age_group based on age:
  'Young' if age < 30
  'Mid' if 30 >= age <= 50
  'Senior' if age > 50
   ======================== */

   val age_grp = data.select(col("Id"),col("Name"),col("Age"),col("Salary"),
                      when(col("Age")<30,"Young")
                        .when(col("Age") >= 30 && col("Age")<=50,"Mid")
                        .when(col("Age")>50,"Senior").alias("Age_group"))
    age_grp.show()

   /* #Create a new column salary_range based on salary:
      'High' if salary > 100000
    'Medium' if 50000 >= salary <= 100000
    'Low' if salary < 50000
    ====================================== */

    val salary_range = data.select(col("Id"),col("Name"),col("Age"),col("Salary"),
                         when(col("Salary")>100000,"High")
                        .when(col("Salary") >= 50000 && col("salary")<=100000,"Medium")
                       .when(col("Salary")<50000,"Low").alias(" Salary Range"))
    salary_range.show()

    /* #Filter employees whose name starts with 'J'.
    ============================================= */

    val srt_with = data.filter(col("Name").startsWith("J"))
    srt_with.show()

    /* #Filter employees whose name ends with 'e'.
     ========================================*/
    val ends_with = data.filter(col("Name").endsWith("e"))
     ends_with.show()

    /* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) salary for each age_group.
    ===============================================================================================================*/

    val calc = age_grp.groupBy("Age").agg(sum(col("Salary")).alias("Total Salary"),
                                  avg(col("Salary")).alias("Average Salary"),
                                 max(col("Salary")).alias("Maximum Salary"),
                                min(col("Salary")).alias("Minimum").alias("Minimum"))
     calc.show()


      spark.stop()
    }
}
