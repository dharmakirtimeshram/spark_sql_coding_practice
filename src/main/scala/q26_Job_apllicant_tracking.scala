import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q26_Job_apllicant_tracking {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb26").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._

    val applicant_tracking = List((1,"Laura Green", "Hired","2024-01-10"),(2,"Mark White","Under Review","2024-01-15"),
                                  (3,"John Doe","Rejected","2024-01-20"),(4,"Emily Brown","Hired","2024-01-25"),
                                  (5,"Sarah Taylor", "Under Review","2024-01-30"),(6,"Chris Black","Rejected", "2024-01-31"))
                              .toDF("Applicant_Id","Applicant_Name","Application_Status","Application_Date")
    applicant_tracking.show()

    /* #Create a new column status_category based on application_status:
     'Accepted' if application_status is 'Hired'
     'Rejected' if application_status is 'Rejected'
     'Pending' if application_status is 'Under Review'
     =======================================================*/

    val status_category = applicant_tracking.withColumn("Status_Category",
                                                    when($"Application_Status" === "Hired" , "Accepted")
                                                     .when($"Application_Status" === "Rejected" , "Rejected")
                                                      .when($"Application_Status" === "Under Review","Pending"))
     status_category.show()


 /* # Filter applicants where application_date is in 'January 2024'.
==================================================================  */
    val filt_df = status_category.filter($"Application_Date".between("2024-01-01","2024-01-31"))

      filt_df.show()

/*# Calculate the total (count) of each status_category.
============================================================*/

    val count_df = status_category.groupBy("Status_Category")
                                  .agg(count(col("Status_Category")).alias("Total Application"))

      count_df.show()

  spark.stop()
  }
}