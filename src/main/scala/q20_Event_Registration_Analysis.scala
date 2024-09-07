import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q20_Event_Registration_Analysis {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q20").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema = Seq("Registration_Id","Event_Amount","Registration_Amount","Registration_Date")

    val event_registration = Seq((1,"Conference",1200,"2026-12-01"),(2,"WorkShop",800,"2026-12-05"),
                                (3,"Seminar",400,"2026-12-10"),(4,"Conference",1000,"2026-12-15"),
                                (5,"Seminar",1500,"2026-12-20"),(6,"Workshop",300,"2026-12-25"))
    val df = spark.createDataFrame(event_registration).toDF(schema:_*)
    df.show()

    /* Create a new column registration_category based on registration_amount:
           o 'VIP' if registration_amount > 1000
           o 'Standard' if 500 >= registration_amount <= 1000
           o 'Basic' if registration_amount < 500
     ============================================================= */
    val registration_category = df.withColumn("Registration_Category",
                                   when($"Registration_Amount" > 1000,"VIP")
                                     .when($"Registration_Amount" >= 500 && $"Registration_Amount" <= 1000,"Standard")
                                     .when($"Registration_Amount" < 500,"Basic"))
    registration_category.show()

/* Filter records where registration_date is in 'December 2026'.
=======================================================================  */
    val df1 = df.filter($"Registration_Date".between("2026-12-01","2026-12-31"))
    df1.show()

/* Calculate the total (sum), average (avg), maximum (max), and minimum (min) registration_amount for each registration_category.
=======================================================================================================================================*/

    val df2 = registration_category.groupBy("Registration_Category")
                                    .agg(sum($"Registration_Amount").alias("Total_Registration_Amount"),
                                      avg($"Registration_Amount").alias("Average_Registration_Amount"),
                                      max($"Registration_Amount").alias("Maximum_Registration_Amount"),
                                      min($"Registration_Amount").alias("Minimum_Registration_Amount"))
    df2.show()

 spark.stop()
  }
}
