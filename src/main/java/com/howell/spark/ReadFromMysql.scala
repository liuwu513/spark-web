import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import org.apache.spark.{SparkConf, SparkContext}

object ReadFromMysql {
  //define a function to get an Connection of mysql
  def getConnection ():Connection={
    //you should the database'name not the database and table' name
    DriverManager.getConnection("jdbc:mysql://10.15.51.100:3306/spark","root","wtoip@2015")
  }

  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("ReadFromMysql").setMaster("yarn-cluster")
    val sc = new SparkContext(conf)

    val connection = getConnection()
    val preparedStatement: PreparedStatement = connection.prepareStatement(
      "select * from db_user" )
    val result: ResultSet = preparedStatement.executeQuery()

    println("id\tname")
    while(result.next()){
      print(result.getString("id")+" ")
      print(result.getString("name")+" ")
      println()
    }
  }
}