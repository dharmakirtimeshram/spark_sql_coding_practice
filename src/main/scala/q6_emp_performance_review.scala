import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q6_emp_performance_review {
  def main(args:Array[String]):Unit={

    val sparkconf = new SparkConf().setAppName("qb6").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val data = List((1,"2024-01-10",8,"Good Performance"),(2,"2024-01-15",9,"Excellent Work"),(3,"2024-02-20",6,"Needs Improvement"),
                    (4,"2024-02-25",7,"Good Effort"),(5,"2024-03-05",10,"Outstanding"),(6,"2024-03-12",5,"Needs Improvement"),
                     (7,"2024-04-06",8,"Well DOne"),(8,"2024-04-25",3,"Poor Performance"),(9,"2024-05-07",6,"Good Work"),
                    (10,"2024-05-28",9,"Excellent"),(11,"2024-06-7",5,"Needs Improvement"),(12,"2024-06-15",7,"Good Effort"))
                   .toDF("Employee_Id","Review_Date","Performance_Score","Review_Text")
      //data.show()

    /*  #Create a new column performance_category based on performance_score:
  'Excellent' if performance_score >= 9
  'Good' if 7 >= performance_score < 9
  'Needs Improvement' if performance_score < 7
  =============================================================*/

    val perf_category = data.withColumn("Performance Category",
                                         when(($"Performance_Score") >= 9,"Excellent")
                                        .when(($"Performance_Score")>=7 && ($"Performance_Score")<9,"Good")
                                         .when(($"Performance_Score")<7,"Needs Improvement"))
        perf_category.show()

    /* # Filter reviews where review_text contains 'excellent'.
    =============================================================*/

    val filt = data.filter(($"Review_Text").contains("Excellent"))
       filt.show()

    /* # Calculate the average performance_score per month.
    =========================================================== */

    val mnth =data.withColumn("Review_Month",month($"Review_Date"))
    val avg_perf_score = mnth.groupBy("Review_Month").agg(avg($"Performance_Score"))

      avg_perf_score.show()



    spark.stop()
  }

}
