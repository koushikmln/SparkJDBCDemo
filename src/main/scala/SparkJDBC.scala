import com.typesafe.config._
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import java.util.Properties

object SparkJDBC {

  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("Spark-JDBC").setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val dataframe_mysql = sqlContext.read.format("jdbc").
      option("url", "jdbc:mysql://ms.itversity.com:3306/TetrasoftBigDataHackathon").
      option("driver", "com.mysql.jdbc.Driver").
      option("dbtable", "BEHAVIORAL_RISK_FACTOR").
      option("user", "tetrasoft").
      option("password", "hackathon").load()

    dataframe_mysql.show

  }

}