package org.cts.hbase

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{ Level, LogManager }
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest.{ BeforeAndAfterAll, Suite, SuiteMixin }
//import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster
//import com.github.sakserv.minicluster.impl.HbaseLocalCluster
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseCluster
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.LocalHBaseCluster
import org.apache.hadoop.hbase.spark.HBaseContext

trait MiniCluster extends SuiteMixin with BeforeAndAfterAll { this: Suite =>
 
  var sc: SparkContext = _
  var hc: HBaseContext = _
  val htu: HBaseTestingUtility = new HBaseTestingUtility
  val tableName = "t1"
  val columnFamily = "c"

  override def beforeAll() {
    //System.setProperty("HADOOP_HOME",
    //"C:\\Users\\Dilip Diwakar\\Desktop\\winutils-master\\hadoop-2.7.1") 
    htu.cleanupTestDir()
    println("starting minicluster+++++++++++++++++++++++++")    
    println(" - minicluster started+++++++++++++++++++++++++")
    htu.startMiniCluster()
    try {
      htu.deleteTable(Bytes.toBytes(tableName))
    } catch {
      case e: Exception => {
        println(" - no table +++++++++++" + tableName + " found")
      }

    }
    println(" - creating table+++++++++++++++++++++ " + tableName)
    htu.createTable(Bytes.toBytes(tableName), Bytes.toBytes(columnFamily))
    println(" - created table+++++++++++++++++++++++")

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.broadcast.compress", "false").setAppName("hbase-rdd_spark")
    sc = new SparkContext("local", "test", sparkConfig)
  }

  override def afterAll() {
    htu.deleteTable(Bytes.toBytes(tableName))
    println("shuting down minicluster+++++++++++++++++++++")
    htu.shutdownMiniCluster()
    println(" - minicluster shut down+++++++++++++++++++++")
    htu.cleanupTestDir()
    sc.stop();
  }
}