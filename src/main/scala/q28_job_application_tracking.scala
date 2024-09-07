import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q28_job_application_tracking {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q28").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Application_Id","Applicant_Id","Application_Status","Application_Date")

    val job_application = Seq((1,401,"Hired","2027-06-01"),(2,402,"Interview Scheduled","2027-06-05"),
                               (3,403,"Rejected","2027-06-10"),(4,404,"Applied","2027-06-15"),
                               (5,405,"Hired","2027-06-20"),(6,406,"Applied","2027-06-25"))

    val df = spark.createDataFrame(job_application).toDF(schema:_*)
    df.show()

  /* Create a new column status_category based on application_status:
         o 'Accepted' if application_status is 'Hired'
         o 'In Review' if application_status is 'Interview Scheduled'
         o 'Rejected' if application_status is 'Rejected'
         o 'Pending' if application_status is 'Applied'
       ==========================================================  */
    val df1 =df.withColumn("Status_Category",
                 when($"Application_Status" === "Hired" ,"Accepted")
                   .when($"Application_Status" === "Interview Scheduled" ,"In Review")
                   .when($"Application_Status" === "Rejected" ,"Rejected")
                   .when($"Application_Status" === "Applied" ,"Pending"))
    df1.show()

/*  Filter records where application_date is in 'June 2027'.
   ==============================================================   */
    val df2 = df.filter($"Application_Date".between("2027-06-01","2027-06-30"))
    df2.show()

/* Calculate the total count of each status_category.
=====================================================*/
    val df3 = df1.groupBy("Status_Category").count()
    df3.show()

  spark.stop()
  }

}
