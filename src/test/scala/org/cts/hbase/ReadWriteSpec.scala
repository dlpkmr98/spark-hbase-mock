package org.cts.hbase

import org.scalatest.BeforeAndAfter
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

class ReadWriteSpec extends UnitSpec with MiniCluster with BeforeAndAfter with Serializable {

  val tableName = "pricetest"
  val columnFamily = "cftest"

  before {
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
  }

  after {
    htu.deleteTable(Bytes.toBytes(tableName))
    println(" - deleted table+++++++++++++++++++++++")
  }

  "A HBaseRDD" should "write to a hbase table" in {

    val config = htu.getConfiguration

    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("a"), Bytes.toBytes("foo1")))),
      (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("b"), Bytes.toBytes("foo2")))),
      (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("c"), Bytes.toBytes("foo3")))),
      (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("d"), Bytes.toBytes("foo")))),
      (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("e"), Bytes.toBytes("bar"))))))

    val hbaseContext = new HBaseContext(sc, config)

    hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](
      rdd,
      TableName.valueOf(tableName),
      (putRecord) => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2, putValue._3))
        put
      })

    val connection = ConnectionFactory.createConnection(config)
    val table = connection.getTable(TableName.valueOf(tableName))
    try {

      val foo1 = Bytes.toString(CellUtil.cloneValue(table.get(new Get(Bytes.toBytes("1"))).
        getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("a"))))
      assert(foo1 == "foo1")
      val foo2 = Bytes.toString(CellUtil.cloneValue(table.get(new Get(Bytes.toBytes("2"))).
        getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("b"))))
      assert(foo2 == "foo2")
      val foo3 = Bytes.toString(CellUtil.cloneValue(table.get(new Get(Bytes.toBytes("3"))).
        getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("c"))))
      assert(foo3 == "foo3")
      val foo4 = Bytes.toString(CellUtil.cloneValue(table.get(new Get(Bytes.toBytes("4"))).
        getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("d"))))
      assert(foo4 == "foo")
      val foo5 = Bytes.toString(CellUtil.cloneValue(table.get(new Get(Bytes.toBytes("5"))).
        getColumnLatestCell(Bytes.toBytes(columnFamily), Bytes.toBytes("e"))))
      assert(foo5 == "bar")

    } finally {

      table.close()

      connection.close()

    }

  }

  "A HBaseRDD" should "write/read to a hbase table" in {

    val config = htu.getConfiguration

    val rdd = sc.parallelize(Array(
      (
        Bytes.toBytes("row_key1"),
        Array(
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c1"), Bytes.toBytes("NA")),
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c2"), Bytes.toBytes("US")),
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c3"), Bytes.toBytes("NYC")),
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c4"), Bytes.toBytes("EWR")),
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c5"), Bytes.toBytes("EU")),
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c6"), Bytes.toBytes("DE")),
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c7"), Bytes.toBytes("FRA")),
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c8"), Bytes.toBytes("FRA")),
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c9"), Bytes.toBytes("false")),
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c10"), Bytes.toBytes("")))),
      (
        Bytes.toBytes("row_key2"),
        Array(
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c1"), Bytes.toBytes("OG")),
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c2"), Bytes.toBytes("OC")),
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c3"), Bytes.toBytes("OC1")),
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c4"), Bytes.toBytes("OA")),
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c5"), Bytes.toBytes("DG")),
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c6"), Bytes.toBytes("DC")),
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c7"), Bytes.toBytes("DC1")),
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c8"), Bytes.toBytes("DA")),
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c9"), Bytes.toBytes("bf")),
          (Bytes.toBytes(columnFamily), Bytes.toBytes("c10"), Bytes.toBytes("no reason"))))))

    val hbaseContext = new HBaseContext(sc, config)
    hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](
      rdd,
      TableName.valueOf(tableName),
      (putRecord) => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2, putValue._3))
        put
      })

    val rdd_key = sc.parallelize(Array((Bytes.toBytes("row_key1")), (Bytes.toBytes("row_key2"))))

    val getRdd = hbaseContext.bulkGet[Array[Byte], scala.collection.mutable.Map[String, String]](
      TableName.valueOf(tableName),
      2,
      rdd_key,
      record => {
        new Get(record)
      },
      (result: Result) => {
        val map = scala.collection.mutable.Map[String, String]()
        if (result.listCells() != null) {
          map += ("row_key" -> Bytes.toString(result.getRow))
          val it = result.listCells().iterator()
          while (it.hasNext) {
            val cell = it.next()
            val q = Bytes.toString(CellUtil.cloneQualifier(cell))
            map += (q -> Bytes.toString(CellUtil.cloneValue(cell)))
          }
        }
        map
      })

    println("==================>>>>>>>>>>>>>>>>>>>>>>>")
    getRdd.collect().foreach(println)

    val zipRdd = getRdd.filter(!_.isEmpty).map(x => x.toList.sortBy(_._1).unzip)
    val schema = StructType(zipRdd.first()._1.map(k => StructField(k, StringType, nullable = false)))
    val rows = zipRdd.map(_._2).map(x => (Row(x: _*)))

    val priceDF = sqlCtx.createDataFrame(rows, schema)
    priceDF.show()
  }
}
