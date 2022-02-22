/*
 * Licensed to Cloudera, Inc. under one
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

package com.cloudera;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkTestJavaJob {

    public static void main(String[] args) {
        Options options = new Options();

        Option sinksOption =
                Option.builder("s")
                        .desc(
                                "Comma separated list of Flink sinks where output must be written. Valid values: hdfs,hbase,hive")
                        .required()
                        .longOpt("sinks")
                        .numberOfArgs(Option.UNLIMITED_VALUES)
                        .valueSeparator(',')
                        .build();
        options.addOption(sinksOption);

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine commandLine = parser.parse(options, args);
            String[] sinks = commandLine.getOptionValues(sinksOption);

            EnvironmentSettings settings =
                    EnvironmentSettings.newInstance().inStreamingMode().build();

            TableEnvironment tEnv = TableEnvironment.create(settings);

            tEnv.createTable(
                    "DatagenTable",
                    TableDescriptor.forConnector("datagen")
                            .schema(Schema.newBuilder().column("f0", DataTypes.STRING()).build())
                            .option("fields.f0.kind", "random")
                            .option(DataGenConnectorOptions.FIELD_LENGTH, 8)
                            .option(DataGenConnectorOptions.ROWS_PER_SECOND, 1L)
                            .build());

            Table datagenTable = tEnv.from("DatagenTable").select($("f0"));

            for (String sink : sinks) {
                System.out.println("Creating sink: " + sink);

                switch (sink) {
                    case "hdfs":
                        tEnv.createTable(
                                "HdfsTable",
                                TableDescriptor.forConnector("filesystem")
                                        .schema(
                                                Schema.newBuilder()
                                                        .column("f0", DataTypes.STRING())
                                                        .build())
                                        .option(
                                                FileSystemConnectorOptions.PATH,
                                                "hdfs://hdfs:9000/output")
                                        .build());

                        datagenTable.executeInsert("HdfsTable");
                        break;

                    case "hbase":
                        throw new UnsupportedOperationException("HBase is not yet supported");

                    case "hive":
                        throw new UnsupportedOperationException("Hive is not yet supported");

                    default:
                        throw new ParseException("Invalid sink type");
                }
            }
        } catch (ParseException e) {
            System.err.println("Unable to parse command line options: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(FlinkTestJavaJob.class.getCanonicalName(), options);
        }
    }
}
