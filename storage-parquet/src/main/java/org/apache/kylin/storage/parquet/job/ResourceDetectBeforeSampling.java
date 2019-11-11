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

package org.apache.kylin.storage.parquet.job;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.utils.DetectItem;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.apache.spark.util.SizeEstimator;

import com.google.common.collect.Maps;

import org.apache.kylin.storage.parquet.NSparkCubingEngine;
import org.apache.kylin.storage.parquet.application.SparkApplication;
import org.apache.kylin.metadata.model.NBatchConstants;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import scala.collection.JavaConversions;

@Slf4j
public class ResourceDetectBeforeSampling extends SparkApplication {
    @Override
    protected void doExecute() {
        log.info("Start detect resource before table sampling.");
        String tableName = getParam(NBatchConstants.P_TABLE_NAME);
        final TableDesc tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(tableName);
        final Dataset<Row> dataset = SourceFactory
                .createEngineAdapter(tableDesc, NSparkCubingEngine.NSparkCubingSource.class)
                .getSourceData(tableDesc, ss, new HashMap<>());
        final List<Path> paths = JavaConversions
                .seqAsJavaList(ResourceDetectUtils.getPaths(dataset.queryExecution().sparkPlan()));
        List<String> pathList = paths.stream().map(Path::toString).collect(Collectors.toList());
        val sampledData = dataset.limit(SparkJobConstants.DEFAULT_SAMPLED_DATA_LIMIT).collectAsList();
        val estimatedSize = SizeEstimator.estimate(sampledData);
        log.info("estimatedSize is " + estimatedSize);

        Map<String, String> detectedItems = Maps.newLinkedHashMap();
        detectedItems.put(DetectItem.ESTIMATED_LINE_COUNT(), String.valueOf(sampledData.size()));
        detectedItems.put(DetectItem.ESTIMATED_SIZE(), String.valueOf(estimatedSize));

        Map<String, List<String>> resourcePaths = Maps.newHashMap();
        resourcePaths.put(tableName, pathList);

        ResourceDetectUtils.write(
                new Path(config.getJobTmpShareDir(project, jobId), tableName + "_" + ResourceDetectUtils.fileName()),
                resourcePaths);

        ResourceDetectUtils.write(new Path(config.getJobTmpShareDir(project, jobId),
                tableName + "_" + ResourceDetectUtils.samplingDetectItemFileSuffix()), detectedItems);
    }

    public static void main(String[] args) {
        ResourceDetectBeforeSampling detect = new ResourceDetectBeforeSampling();
        detect.execute(args);
    }
}
