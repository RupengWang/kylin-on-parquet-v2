/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.storage.parquet.utils

import java.io.IOException

import org.apache.kylin.storage.parquet.NSparkCubingEngine
import org.apache.kylin.storage.parquet.job.NSparkCubingUtil
import org.apache.kylin.metadata.model.NDataModel
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.kylin.common.util.{HadoopUtil, JsonUtil}
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.measure.bitmap.BitmapMeasureType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object BuildUtils extends Logging {

  def findCountDistinctMeasure(layout: LayoutEntity): Boolean =
    layout.getOrderedMeasures.values.asScala.exists((measure: NDataModel.Measure) =>
      measure.getFunction.getReturnType.equalsIgnoreCase(BitmapMeasureType.DATATYPE_BITMAP))

  @throws[IOException]
  def repartitionIfNeed(
                         layout: LayoutEntity,
                         dataCuboid: NDataLayout,
                         storage: NSparkCubingEngine.NSparkCubingStorage,
                         path: String,
                         tempPath: String,
                         kapConfig: KapConfig,
                         sparkSession: SparkSession): Int = {
    val fs = HadoopUtil.getWorkingFileSystem()
    if (fs.exists(new Path(tempPath))) {
      val summary = HadoopUtil.getContentSummary(fs, new Path(tempPath))
      var repartitionThresholdSize = kapConfig.getParquetStorageShardSizeRowCount
      if (findCountDistinctMeasure(layout)) {
        repartitionThresholdSize = kapConfig.getParquetStorageCountDistinctShardSizeRowCount
      }
      val shardByColumns = layout.getShardByColumns
      val repartitioner = new Repartitioner(
        kapConfig.getParquetStorageShardSizeMB,
        kapConfig.getParquetStorageRepartitionThresholdSize,
        dataCuboid.getRows,
        repartitionThresholdSize,
        summary,
        shardByColumns
      )

      val sortCols = NSparkCubingUtil.getColumns(layout.getOrderedDimensions.keySet)

      val extConfig = layout.getIndex.getModel.getProjectInstance.getConfig.getExtendedOverrides
      val configJson = extConfig.get("kylin.engine.shard-num-json")

      val numByFileStorage = repartitioner.getRepartitionNumByStorage
      val repartitionNum = if (configJson != null) {
        try {
          val colToShardsNum = JsonUtil.readValueAsMap(configJson)

          // now we only has one shard by col
          val shardColIdentity = shardByColumns.asScala.map(layout.getIndex.getModel
            .getEffectiveDimenionsMap.get(_).toString).mkString(",")
          val num = colToShardsNum.getOrDefault(shardColIdentity, String.valueOf(numByFileStorage)).toInt
          logInfo(s"Get shard num in config, col identity is:$shardColIdentity, shard num is $num.")
          num
        } catch {
          case th: Throwable =>
            logError("Some thing went wrong when getting shard num in config", th)
            numByFileStorage
        }

      } else {
        logInfo(s"Get shard num by file size, shard num is $numByFileStorage.")
        numByFileStorage
      }

      repartitioner.doRepartition(storage, path, repartitionNum, sortCols, sparkSession)
      repartitionNum
    } else {
      throw new RuntimeException(
        String.format("Temp path does not exist before repartition. Temp path: %s.", tempPath))
    }
  }

  @throws[IOException]
  def fillCuboidInfo(cuboid: NDataLayout): Unit = {
    val strPath = NSparkCubingUtil.getStoragePath(cuboid)
    val fs = HadoopUtil.getWorkingFileSystem
    if (fs.exists(new Path(strPath))) {
      val cs = HadoopUtil.getContentSummary(fs, new Path(strPath))
      cuboid.setFileCount(cs.getFileCount)
      cuboid.setByteSize(cs.getLength)
    } else {
      cuboid.setFileCount(0)
      cuboid.setByteSize(0)
    }
  }

  def updateDataFlow(seg: NDataSegment, dataCuboid: NDataLayout, conf: KylinConfig, project: String): Unit = {
    logInfo(s"Update layout ${dataCuboid.getLayoutId} in dataflow ${seg.getId}, segment ${seg.getId}")
    val update = new NDataflowUpdate(seg.getDataflow.getUuid)
    update.setToAddOrUpdateLayouts(dataCuboid)
    NDataflowManager.getInstance(conf, project).updateDataflow(update)
  }

  def getCurrentYarnConfiguration: YarnConfiguration = {
    val conf = new YarnConfiguration()
    System.getProperties.entrySet()
      .asScala
      .filter(_.getKey.asInstanceOf[String].startsWith("spark.hadoop."))
      .map(entry => (entry.getKey.asInstanceOf[String].substring("spark.hadoop.".length), entry.getValue.asInstanceOf[String]))
      .foreach(tp => conf.set(tp._1, tp._2))
    conf
  }
}
