
import SparkBigData._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
//import org.datanucleus.store.types.backed._
import java.util._

object SparkSQL {

  def main(arg: Array[String]): Unit = {
    val ss = Session_Spark(true)

    val  prop_mysql = new Properties()
    prop_mysql.put("user", "consultant")
    prop_mysql.put("password","pwd#86")

    val df_mysql = ss.read.jdbc("jdbc:mysql://127.0.0.1:3306/jd_bb?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC", "jd_bb.orders",prop_mysql)

    df_mysql.show(15)
  }
}