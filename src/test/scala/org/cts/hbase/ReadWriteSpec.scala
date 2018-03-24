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

class ReadWriteSpec extends UnitSpec with MiniCluster {

  "A HBaseRDD" should "read/write to a Table all column families" in {

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
        putRecord._2.foreach((putValue) => put.add(putValue._1, putValue._2, putValue._3))
        put
      })

    val connection = ConnectionFactory.createConnection(config)
    val table = connection.getTable(TableName.valueOf("t1"))
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

}