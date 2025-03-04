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

package org.apache.kylin.engine.spark.job;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.Segments;

public class JobStepFactory {

    private JobStepFactory() {
    }

    public static NSparkExecutable addStep(DefaultChainedExecutable parent, JobStepType type,
            CubeInstance cube) {
        NSparkExecutable step;
        KylinConfig config = cube.getConfig();
        switch (type) {
        case RESOURCE_DETECT:
            step = new NResourceDetectStep(parent);
            break;
        case CUBING:
            step = new NSparkCubingStep(config.getSparkBuildClassName());
            break;
        case MERGING:
            step = new NSparkMergingStep(config.getSparkMergeClassName());
            break;
        case CLEAN_UP_AFTER_MERGE:
            step = new NSparkUpdateMetaAndCleanupAfterMergeStep();
            break;
        default:
            throw new IllegalArgumentException();
        }

        step.setParams(parent.getParams());
        step.setProject(parent.getProject());
        step.setTargetSubject(parent.getTargetSubject());
        if (step instanceof NSparkUpdateMetaAndCleanupAfterMergeStep) {
            CubeSegment mergeSegment = cube.getSegmentById(parent.getTargetSegments().iterator().next());
            final Segments<CubeSegment> mergingSegments = cube.getMergingSegments(mergeSegment);
            step.setParam(MetadataConstants.P_SEGMENT_IDS,
                    String.join(",", NSparkCubingUtil.toSegmentIds(mergingSegments)));
            step.setParam(CubingExecutableUtil.SEGMENT_ID, parent.getParam(CubingExecutableUtil.SEGMENT_ID));
            step.setParam(MetadataConstants.P_JOB_TYPE, parent.getParam(MetadataConstants.P_JOB_TYPE));
            step.setParam(MetadataConstants.P_OUTPUT_META_URL, parent.getParam(MetadataConstants.P_OUTPUT_META_URL));
        }
        parent.addTask(step);
        //after addTask, step's id is changed
        step.setDistMetaUrl(config.getJobTmpMetaStoreUrl(parent.getProject(), step.getId()));
        return step;
    }
}
