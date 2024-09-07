import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q17_project_task_completion {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q17").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Task_Id","Task_Status","Completion_Date")

    val project_task = Seq((1,"Done","2026-09-01"),(2,"Ongoing","2026-09-05"),(3,"Not Started","2026-09-10"),
                           (4,"Done","2026-09-15"),(5,"Ongoing","2026-09-20"),(6,"Done","2026-09-25"))

    val df = spark.createDataFrame(project_task).toDF(schema:_*)
    df.show()

    /* Create a new column completion_status based on task_status:
             'Completed' if task_status is 'Done'
             'In Progress' if task_status is 'Ongoing'
             'Not Started' if task_status is 'Not Started'
        ================================================================ */
    val completion_status = df.withColumn("Completion_Status",
                               when($"Task_Status" === "Done","Completed")
                                 .when($"Task_Status" === "Ongoing","In Progress")
                                 .when($"Task_Status" === "Done","Not Started"))

    completion_status.show()

/* Filter records where completion_date is in 'September 2026'.
=====================================================================  */
    val df1 = df. filter($"Completion_Date".between("2026-09-01","2026-09-30"))
    df1.show()

/* Calculate the count of each completion_status.
==============================================================================================================*/
    val df2 = completion_status.groupBy("Completion_Status").count()
    df2.show()

  spark.stop()
  }
}