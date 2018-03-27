package org.cts.hbase

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{ Level, LogManager }
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest.{ BeforeAndAfterAll, Suite, SuiteMixin }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseCluster
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.LocalHBaseCluster
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Logger

trait MiniCluster extends SuiteMixin with BeforeAndAfterAll { this: Suite =>

  @transient var sqlCtx: SQLContext = _
  @transient var sc: SparkContext = _
  var hc: HBaseContext = _
  val htu: HBaseTestingUtility = new HBaseTestingUtility
  Logger.getRootLogger().setLevel(Level.WARN);

  override def beforeAll() {

    System.setProperty("test.build.data.basedirectory", "C:/Temp/hbase")
    htu.cleanupTestDir()
    println("starting minicluster>>>>>>>>>>>>>>>>>>>>>>>")
    htu.startMiniCluster()
    println(" - minicluster started>>>>>>>>>>>>>>>>>>>>>")
    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.broadcast.compress", "false").setAppName("MiniCluster")
    sc = new SparkContext("local", "test", sparkConfig)
    sqlCtx = new SQLContext(sc)

  }

  override def afterAll() {
    println("shuting down minicluster>>>>>>>>>>>>>>>>>>>>>")
    htu.shutdownMiniCluster()
    println(" - minicluster shut down>>>>>>>>>>>>>>>>>>>>>>")
    htu.cleanupTestDir()
    sc.stop();

  }

}
