import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object qA1_student_grade_calculation {
  def main(args:Array[String]):Unit={

      val sparkconf = new SparkConf()
        .set("spark.app.name","grades")
        .set("spark.master","local[*]")

      val spark = SparkSession.builder().config(sparkconf).getOrCreate()

   import spark.implicits._

    val data = List((1,"Nitesh",92,"Math"),(2,"Vishal",85,"Math"),(3,"Seema",77,"Science"),
                  (4,"Dipali",65,"Science"),(5,"Emlie",50,"Math"),(6,"Faiz",82,"Science"),
                   (7,"Avni",95,"Math"),(8,"Beena",67,"Math"),(9,"Kirti",93,"Math"),
                  (10,"Reena",94,"Science"))
                 .toDF("Student_id","Name","Score","Subject")

    //Give Grade based on score

    val grade =data.select(col("Student_id"),col("Name"),col("Score"),col("Subject"),
                        when(col("Score")>=90,"A")
                        .when(col("Score")>= 80 && (col("Score")<90),"B")
                        .when(col("score")>=70 && (col("Score")<80),"C" )
                        .when(col("Score")>= 60 && (col("Score")<70),"D")
                        .when(col("Score")<60,"F").alias("Grade"))
    grade.show()

    //Calculate the average score per subject.

    val avg_score = data.groupBy("subject").agg(avg(col("Score")).alias("Average Score"))
    avg_score.show()


   // Find the maximum and minimum score per subject.

    val min_max = data.groupBy("Subject").agg(min(col("Score")).alias("Minimum Score"),
                          max(col("Score")).alias("Maximum Score"))
     min_max.show()

    //Count the number of students in each grade category per subject.

    val count_student = grade.groupBy("Grade").agg(count(col("Name")).alias("Total Students")).orderBy("Grade")
    count_student.show()


 spark.stop()
  }

}
