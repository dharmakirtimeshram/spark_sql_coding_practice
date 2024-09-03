import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q24_conference_Registration {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("qb24").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    import spark.implicits._
    val conference_registration = List((1,"Alice Smith",600,"2024-03-01"),(2,"Bob Johnson",350,"2024-03-05"),
                                       (3,"Charlie Brown",150,"2024-03-10"),(4,"Dave Clark",450,"2024-03-15"),
                                       (5,"Emma Wilson",300,"2024-03-20"),(6,"Frank Miller",700,"2024-03-25"))
                              .toDF("Registration_Id","Attendance_Name","Registration_fee","Registration_date")
    conference_registration.show()

    /* #Create a new column fee_category based on registration_fee:
   'Premium' if registration_fee > 500
   'Standard' if 200 >= registration_fee <= 500
    'Basic' if registration_fee < 200
   ===================================================================================  */
    val fee_category = conference_registration.withColumn("Fee_Category",
                                     when($"Registration_fee" > 500,"Premium")
                                       .when($"Registration_fee" >= 200 && $"Registration_fee" <= 500,"Standard")
                                       .when($"Registration_fee" < 200,"Basic"))
    fee_category.show()

/* #Filter registrations where registration_date is in 'March 2024'.
  =====================================================================*/

    val filt_df = conference_registration.filter($"Registration_date".between("2024-03-01","2024-03-31"))
    filt_df.show()

/* #Calculate the total (sum), average (avg), maximum (max), and minimum (min) registration_fee for each fee_category.
===================================================================================================================*/

   val  calc_df  = fee_category.groupBy("Fee_Category")
                       .agg(sum($"Registration_fee").alias("Total Registration_Fee"),
                         avg($"Registration_fee").alias("Average Registration_Fee"),
                         max($"Registration_fee").alias("Maximum Registration_Fee"),
                         min($"Registration_fee").alias("Minimum Registration_Fee"))
    calc_df.show()

   spark.stop()
  }
}
