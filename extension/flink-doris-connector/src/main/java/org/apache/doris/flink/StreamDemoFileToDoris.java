/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.doris.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.sql.Types;

import static org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType.REPROCESS_WITH_APPENDED;

public class StreamDemoFileToDoris {

    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        final String hostname;
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.getInt("port");
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'StreamDemoFileToDoris " +
                "--hostname <hostname> --port <port>', where hostname (localhost by default) " +
                "and port is the address of the text server");
            System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
                "type the input text into the command line");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(30000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(30000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("file:///D:/workspace/files/flink/checkpoint"));

        DataStream<String> textStream = env.readFileStream("file:///D:/workspace/files/flink/testsourefile/file_for_insert.txt", 1000, REPROCESS_WITH_APPENDED);
        // get input data by connecting to the socket
        // DataStream<String> text = env.socketTextStream(hostname, port, "\n");

        SingleOutputStreamOperator<Row> rowStream = textStream.map((MapFunction<String, Row>) s -> {
            Row row = new Row(4);
            row.setField(0, Integer.parseInt(s));
            row.setField(1, String.format("bb%s", s));
            row.setField(2, String.format("cc%s", s));
            row.setField(3, String.format("dd%s", s));
            return row;
        });
        rowStream.print();
        rowStream.addSink(new DorisTwoPhaseCommitSink("10.81.85.89", 8931, "root", "", "pxy",
                "useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&connectTimeout=10000&socketTimeout=10000&"
                    + "useSSL=false&autoReconnect=true", "insert into TblPxy4 values(?, ?, ?, ?)",
                    new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR}))
            .name("DorisTwoPhaseCommitSink");

        env.execute("File to doris");
    }
}

