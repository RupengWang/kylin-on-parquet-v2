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

import java.util.Arrays;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import org.apache.kylin.storage.parquet.merger.MetadataMerger;
import org.apache.kylin.metadata.model.LayoutEntity;
import org.apache.kylin.metadata.model.NDataLayout;
import org.apache.kylin.metadata.model.NDataflow;
import org.apache.kylin.metadata.model.NDataflowManager;
import lombok.NoArgsConstructor;
import lombok.val;

/**
 */
@NoArgsConstructor

public class NSparkCubingStep extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkCubingStep.class);

    public NSparkCubingStep(String sparkSubmitClassName) {
        this.setSparkSubmitClassName(sparkSubmitClassName);
        this.setName(ExecutableConstants.STEP_NAME_BUILD_SPARK_CUBE);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        NDataflow df = NDataflowManager.getInstance(config, getProject()).getDataflow(getDataflowId());
        return df.collectPrecalculationResource();
    }

    public static class Mockup {
        public static void main(String[] args) {
            logger.info(Mockup.class + ".main() invoked, args: " + Arrays.toString(args));
        }
    }

    @Override
    public void mergerMetadata(MetadataMerger merger) {
        merger.merge(this);
    }

    @Override
    public boolean needMergeMetadata() {
        return true;
    }

    @Override
    public Set<String> getDependencies(KylinConfig config) {
        String dataflowId = getDataflowId();
        Set<String> segmentIds = getSegmentIds();

        val dfMgr = NDataflowManager.getInstance(config, getProject());
        val dataflow = dfMgr.getDataflow(dataflowId);
        val indexPlan = dataflow.getIndexPlan();

        Set<String> result = Sets.newHashSet();
        for (String segId : segmentIds) {
            val seg = dfMgr.getDataflow(dataflowId).getSegment(segId);
            for (LayoutEntity layout : indexPlan.getAllLayouts()) {
                NDataLayout dataCuboid = NDataLayout.newDataLayout(seg.getDataflow(), seg.getId(), layout.getId());
                String path = "/" + NSparkCubingUtil.getStoragePathWithoutPrefix(dataCuboid.getSegDetails(),
                        dataCuboid.getLayoutId());
                result.add(path);
                result.add(path + DFBuildJob.TEMP_DIR_SUFFIX);
            }
        }

        val model = indexPlan.getModel();
        model.getJoinTables().forEach(lookupDesc -> {
            val tableDesc = lookupDesc.getTableRef().getTableDesc();
            val isLookupTable = model.isLookupTable(lookupDesc.getTableRef());
            if (isLookupTable) {
                val tablePath = "/" + tableDesc.getProject() + HadoopUtil.SNAPSHOT_STORAGE_ROOT + "/"
                        + tableDesc.getName();
                result.add(tablePath);
            }
        });
        return result;
    }
}
