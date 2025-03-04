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

package org.apache.kylin.engine.spark.source;

import org.apache.kylin.engine.spark.NSparkCubingEngine.NSparkCubingSource;
import java.io.File;
import java.util.Map;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.metadata.TableDesc;
import org.apache.kylin.engine.spark.metadata.cube.source.ISource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvSource implements ISource {

    @SuppressWarnings("unchecked")
    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
            return (I) new NSparkCubingSource() {

                @Override
                public Dataset<Row> getSourceData(TableDesc table, SparkSession ss, Map<String, String> parameters) {
                    String path = new File(getUtMetaDir(), "../../../examples/test_case_data/localmeta_n//data/" + table.identity() + ".csv").getAbsolutePath();
                    Dataset<Row> delimiter = ss.read()
                            .option("delimiter", ",")
//                            .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS")
                            .schema(table.toSchema()).csv("file:///" + path);
                    return delimiter;
                }
            };
    }

    private String getUtMetaDir() {
        // this is only meant to be used in UT
        final String utMetaDir = System.getProperty(KylinConfig.KYLIN_CONF);
        if (utMetaDir == null || !utMetaDir.startsWith("../example"))
            throw new IllegalStateException();
        return utMetaDir;
    }
}
