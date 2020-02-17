package com.cloudera.spark.hbase

import java.security.{PrivilegedAction, PrivilegedExceptionAction}
import java.text.SimpleDateFormat
import java.util.{ArrayList, Date}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{IdentityTableMapper, TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.{InputSplit, Job, JobID, RecordReader}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark._


class HBaserReadDataScanRDDV2(sc: SparkContext,
                              usf: UserSecurityFunction,
                              @transient tableName: String,
                              @transient scan: Scan,
                              configBroadcast: Broadcast[SerializableWritable[Configuration]])
  extends RDD[(ImmutableBytesWritable, Result)](sc, Nil)
    with SparkHadoopMapReduceUtilExtended
    with Logging {

  @transient val jobTransient = new Job(configBroadcast.value.value, "ExampleRead");
  if (usf.isSecurityEnable()) {
    usf.login().doAs(new PrivilegedExceptionAction[Unit] {
      def run: Unit =
        TableMapReduceUtil.initTableMapperJob(
          tableName, // input HBase table name
          scan, // Scan instance to control CF and attribute selection
          classOf[IdentityTableMapper], // mapper
          null, // mapper output key
          null, // mapper output value
          jobTransient)
    })
  } else {
    TableMapReduceUtil.initTableMapperJob(
      tableName, // input HBase table name
      scan, // Scan instance to control CF and attribute selection
      classOf[IdentityTableMapper], // mapper
      null, // mapper output key
      null, // mapper output value
      jobTransient)
  }


  @transient val jobConfigurationTrans = jobTransient.getConfiguration()
  jobConfigurationTrans.set(TableInputFormat.INPUT_TABLE, tableName)
  val jobConfigBroadcast = sc.broadcast(new SerializableWritable(jobConfigurationTrans))

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  @transient protected val jobId = new JobID(jobTrackerId, id)

  override def getPartitions: Array[Partition] = {
    //addCreds
    val tableInputFormat = new TableInputFormat
    tableInputFormat.setConf(jobConfigBroadcast.value.value)
    val jobContext = newJobContext(jobConfigBroadcast.value.value, jobId)
    var rawSplits: Array[Object] = null
    if (usf.isSecurityEnable()) {
      rawSplits = usf.login().doAs(new PrivilegedAction[Array[Object]] {
        def run: Array[Object] = tableInputFormat.getSplits(jobContext).toArray
      })
    } else {
      rawSplits = tableInputFormat.getSplits(jobContext).toArray
    }

    val result = new Array[Partition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) = new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }

    result
  }

  override def compute(theSplit: Partition, context: TaskContext):
  InterruptibleIterator[(ImmutableBytesWritable, Result)] = {
    //addCreds
    val iter = new Iterator[(ImmutableBytesWritable, Result)] {
      //addCreds
      val split = theSplit.asInstanceOf[NewHadoopPartition]
      logInfo("Input split: " + split.serializableHadoopSplit)
      val conf = jobConfigBroadcast.value.value

      val attemptId = newTaskAttemptID(jobTrackerId, id, isMap = true, split.index, 0)
      val hadoopAttemptContext = newTaskAttemptContext(conf, attemptId)
      val format = new TableInputFormat
      format.setConf(conf)

      var reader: RecordReader[ImmutableBytesWritable, Result] = null
      if (usf.isSecurityEnable()) {
        reader = usf.login().doAs(new PrivilegedAction[RecordReader[ImmutableBytesWritable, Result]] {
          def run: RecordReader[ImmutableBytesWritable, Result] = {
            val _reader = format.createRecordReader(
              split.serializableHadoopSplit.value, hadoopAttemptContext)
            _reader.initialize(split.serializableHadoopSplit.value, hadoopAttemptContext)
            _reader
          }
        })
      } else {
        reader = format.createRecordReader(
          split.serializableHadoopSplit.value, hadoopAttemptContext)
        reader.initialize(split.serializableHadoopSplit.value, hadoopAttemptContext)
      }
      // Register an on-task-completion callback to close the input stream.
      context.addOnCompleteCallback(() => close())
      var havePair = false
      var finished = false

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = !reader.nextKeyValue
          havePair = !finished
        }
        !finished
      }

      override def next(): (ImmutableBytesWritable, Result) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        (reader.getCurrentKey, reader.getCurrentValue)
      }

      private def close() {
        try {
          reader.close()
        } catch {
          case e: Exception => logWarning("Exception in RecordReader.close()", e)
        }
      }
    }
    new InterruptibleIterator(context, iter)
  }


  //  override def compute(theSplit: Partition, context: TaskContext):
  //  InterruptibleIterator[(Array[Byte], java.util.List[(Array[Byte], Array[Byte], Array[Byte])])] = {
  //    //addCreds
  //    val iter = new Iterator[(Array[Byte], java.util.List[(Array[Byte], Array[Byte], Array[Byte])])] {
  //      //addCreds
  //      val split = theSplit.asInstanceOf[NewHadoopPartition]
  //      logInfo("Input split: " + split.serializableHadoopSplit)
  //      val conf = jobConfigBroadcast.value.value
  //
  //      val attemptId = newTaskAttemptID(jobTrackerId, id, isMap = true, split.index, 0)
  //      val hadoopAttemptContext = newTaskAttemptContext(conf, attemptId)
  //      val format = new TableInputFormat
  //      format.setConf(conf)
  //
  //      var reader: RecordReader[ImmutableBytesWritable, Result] = null
  //      if (usf.isSecurityEnable()) {
  //        reader = usf.login().doAs(new PrivilegedAction[RecordReader[ImmutableBytesWritable, Result]] {
  //          def run: RecordReader[ImmutableBytesWritable, Result] = {
  //            val _reader = format.createRecordReader(
  //              split.serializableHadoopSplit.value, hadoopAttemptContext)
  //            _reader.initialize(split.serializableHadoopSplit.value, hadoopAttemptContext)
  //            _reader
  //          }
  //        })
  //      } else {
  //        reader = format.createRecordReader(
  //          split.serializableHadoopSplit.value, hadoopAttemptContext)
  //        reader.initialize(split.serializableHadoopSplit.value, hadoopAttemptContext)
  //      }
  //      // Register an on-task-completion callback to close the input stream.
  //      context.addOnCompleteCallback(() => close())
  //      var havePair = false
  //      var finished = false
  //
  //      override def hasNext: Boolean = {
  //        if (!finished && !havePair) {
  //          finished = !reader.nextKeyValue
  //          havePair = !finished
  //        }
  //        !finished
  //      }
  //
  //      override def next(): (Array[Byte], java.util.List[(Array[Byte], Array[Byte], Array[Byte])]) = {
  //        if (!hasNext) {
  //          throw new java.util.NoSuchElementException("End of stream")
  //        }
  //        havePair = false
  //
  //        val it = reader.getCurrentValue.listCells().iterator()
  //
  //        val list = new ArrayList[(Array[Byte], Array[Byte], Array[Byte])]()
  //
  //        while (it.hasNext()) {
  //          val kv = it.next()
  //          list.add((CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv), CellUtil.cloneRow(kv)))
  //          // list.add((kv.getFamily(), kv.getQualifier(), kv.getValue()))
  //        }
  //        (reader.getCurrentKey.copyBytes(), list)
  //      }
  //
  //      private def close() {
  //        try {
  //          reader.close()
  //        } catch {
  //          case e: Exception => logWarning("Exception in RecordReader.close()", e)
  //        }
  //      }
  //    }
  //    new InterruptibleIterator(context, iter)
  //  }

  //  def addCreds {
  //    val creds = SparkHadoopUtil.get.getCurrentUserCredentials()
  //
  //    val ugi = UserGroupInformation.getCurrentUser();
  //    ugi.addCredentials(creds)
  //    // specify that this is a proxy user
  //    ugi.setAuthenticationMethod(AuthenticationMethod.PROXY)
  //  }

  private class NewHadoopPartition(
                                    rddId: Int,
                                    val index: Int,
                                    @transient rawSplit: InputSplit with Writable)
    extends Partition {

    val serializableHadoopSplit = new SerializableWritable(rawSplit)

    override def hashCode(): Int = 41 * (41 + rddId) + index
  }

}


