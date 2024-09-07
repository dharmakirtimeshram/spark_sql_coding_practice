import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q9_product_review_scores {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("q9").setMaster("local[4]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._

    val schema= Seq("Review_Id","Product_Name","Review_Score","Review_Date")

    val product_reviews = Seq((1,"Widget X",4.7,"2026-02-01"),(2,"Gadget Y",3.8,"2026-02-05"),
                               (3,"Gadget Z",2.5,"2026-02-10"),(4,"Widget X",3.0,"2026-02-15"),
                                (5,"Gadget Y",4.2,"2026-02-20"),(6,"Widget Z",1.8,"2026-02-25"))
    val df = spark.createDataFrame(product_reviews).toDF(schema:_*)
    df.show()

    /* Create a new column review_category based on review_score:
             'Excellent' if review_score >= 4.5
              'Good' if 3 >= review_score < 4.5
              'Average' if 2 >= review_score < 3
               'Poor' if review_score < 2
       ==========================================================*/
    val review_category = df.withColumn("Review_Category",
                              when($"Review_Score" >= 4.5,"Excellent")
                                .when($"Review_Score" >= 3 && $"Review_Score" < 4.5,"Good")
                                .when($"Review_Score" >=2 && $"Review_Score" < 3,"Average")
                                .when($"Review_Score" < 2 ,"Poor"))
    review_category.show()

/* Filter records where review_date is in 'February 2026'.
===============================================================*/
    val df1 = df.filter($"Review_Date".between("2026-02-01","2026-02-29"))
    df1.show()

/* Calculate the total (count), average (avg), maximum (max), and minimum (min) review_score for each review_category.
====================================================================================================================  */
    val df2 = review_category.groupBy("Review_Category")
                               .agg(count($"Review_Score").alias("Number of Reviews"),
                                 avg($"Review_Score").alias("Average_Review_Score"),
                                 max($"Review_Score").alias("Maximum_Review_Score"),
                                 min($"Review_Score").alias("Minimum_Review_Score"))
    df2.show()

  spark.stop()
  }
}