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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.kylin.storage.parquet.utils.SparkUtils;
import org.apache.kylin.storage.parquet.application.SparkApplication;
import org.apache.kylin.storage.parquet.builder.NBuildSourceInfo;
import org.apache.kylin.metadata.cuboid.NSpanningTree;
import org.apache.kylin.metadata.cuboid.NSpanningTreeFactory;
import org.apache.kylin.metadata.model.IndexPlan;
import org.apache.kylin.metadata.model.LayoutEntity;
import org.apache.kylin.metadata.model.NBatchConstants;
import org.apache.kylin.metadata.model.NDataSegment;
import org.apache.kylin.metadata.model.NDataflowManager;
import scala.collection.JavaConversions;
import lombok.val;

public class ResourceDetectBeforeCubingJob extends SparkApplication {
    protected volatile NSpanningTree nSpanningTree;
    protected volatile List<NBuildSourceInfo> sources = new ArrayList<>();
    protected static final Logger logger = LoggerFactory.getLogger(ResourceDetectBeforeCubingJob.class);

    @Override
    protected void doExecute() throws Exception {
        logger.info("Start detect resource before cube.");

        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        Set<String> segmentIds = Sets.newHashSet(StringUtils.split(getParam(NBatchConstants.P_SEGMENT_IDS)));
        Set<Long> layoutIds = NSparkCubingUtil.str2Longs(getParam(NBatchConstants.P_LAYOUT_IDS));

        NDataflowManager dfMgr = NDataflowManager.getInstance(config, project);
        IndexPlan indexPlan = dfMgr.getDataflow(dataflowId).getIndexPlan();
        Set<LayoutEntity> cuboids = NSparkCubingUtil.toLayouts(indexPlan, layoutIds).stream().filter(Objects::nonNull)
                .collect(Collectors.toSet());
        nSpanningTree = NSpanningTreeFactory.fromLayouts(cuboids, dataflowId);

        ResourceDetectUtils.write(new Path(config.getJobTmpShareDir(project, jobId), ResourceDetectUtils.countDistinctSuffix()), ResourceDetectUtils.findCountDistinctMeasure(cuboids));
        for (String segId : segmentIds) {
            NDataSegment seg = dfMgr.getDataflow(dataflowId).getSegment(segId);
            DFChooser datasetChooser = new DFChooser(nSpanningTree, seg, jobId, ss, config, false);
            datasetChooser.decideSources();
            NBuildSourceInfo buildFromFlatTable = datasetChooser.flatTableSource();
            if (buildFromFlatTable != null) {
                sources.add(buildFromFlatTable);
            }
            Map<Long, NBuildSourceInfo> buildFromLayouts = datasetChooser.reuseSources();
            sources.addAll(buildFromLayouts.values());

            Map<String, List<String>> resourcePaths = Maps.newHashMap();
            Map<String, Integer> layoutLeafTaskNums = Maps.newHashMap();
            infos.clearSparkPlans();
            for (NBuildSourceInfo source : sources) {
                Dataset<Row> dataset = source.getParentDS();
                val actionRdd = dataset.queryExecution().toRdd();
                logger.info("leaf nodes is: {} ", SparkUtils.leafNodes(actionRdd));
                infos.recordSparkPlan(dataset.queryExecution().sparkPlan());
                List<Path> paths = JavaConversions
                        .seqAsJavaList(ResourceDetectUtils.getPaths(dataset.queryExecution().sparkPlan()));
                List<String> pathList = paths.stream().map(Path::toString).collect(Collectors.toList());
                resourcePaths.put(String.valueOf(source.getLayoutId()), pathList);
                layoutLeafTaskNums.put(String.valueOf(source.getLayoutId()), SparkUtils.leafNodePartitionNums(actionRdd));
            }
            ResourceDetectUtils.write(
                    new Path(config.getJobTmpShareDir(project, jobId), segId + "_" + ResourceDetectUtils.fileName()),
                    resourcePaths);
            ResourceDetectUtils.write(
                    new Path(config.getJobTmpShareDir(project, jobId), segId + "_" + ResourceDetectUtils.cubingDetectItemFileSuffix()),
                    layoutLeafTaskNums);

        }
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.resourceDetectBeforeCubingJobInfo();
    }

    public static void main(String[] args) {
        ResourceDetectBeforeCubingJob resourceDetectJob = new ResourceDetectBeforeCubingJob();
        resourceDetectJob.execute(args);
    }

}
