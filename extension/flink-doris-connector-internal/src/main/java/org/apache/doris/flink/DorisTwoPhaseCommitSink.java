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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

public class DorisTwoPhaseCommitSink<T> extends TwoPhaseCommitSinkFunction<Row, DorisPreparedStatement, Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DorisTwoPhaseCommitSink.class);

    private static final String FLINK_LABEL_PREFIX = "Flink_";

    private final String ip;
    private final int port;
    private final String user;
    private final String pwd;
    private final String dbName;
    private final String jdbcParam;
    private final String sql;
    private final int[] sqlTypes;

    public DorisTwoPhaseCommitSink(
            final String ip, final int port, final String user, final String pwd,
            String dbName, String jdbcParam, String sql, int[] sqlTypes) {
        super(new KryoSerializer<>(DorisPreparedStatement.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
        this.ip = ip;
        this.port = port;
        this.user = user;
        this.pwd = pwd;
        this.dbName = dbName;
        this.jdbcParam = jdbcParam;
        this.sql = sql;
        this.sqlTypes = sqlTypes;
    }

    /**
     * insert a row into the table, called when a Task is inited.
     * @param dorisPreStmt contains statement which is created in beginTransaction
     * @param row a new row to be inserted into doris
     * @param context
     * @throws Exception when something error happens during this operation
     */
    @Override
    public void invoke(DorisPreparedStatement dorisPreStmt, Row row, Context context) throws Exception {
        if (dorisPreStmt == null || dorisPreStmt.getPreStmt() == null) {
            throw new SQLException("Doris stmt is null");
        }
        LOGGER.info("start invoke...");
        JdbcUtils.setRecordToStatement(dorisPreStmt.getPreStmt(), sqlTypes, row);
        dorisPreStmt.getPreStmt().addBatch();
    }

    /**
     * get a statement to begin a transaction
     * @return DorisPreparedStatement contains a new statement
     * @throws Exception when the connection can't be created.
     */
    @Override
    public DorisPreparedStatement beginTransaction() throws Exception {
        LOGGER.info("call beginTransaction");
        Connection conn = DorisConnectUtil.getConnection(ip, port, user, pwd, dbName, jdbcParam);
        Statement stmt = conn.createStatement();
        String dorisLabel = FLINK_LABEL_PREFIX + UUID.randomUUID().toString().replace("-", "");
        stmt.executeUpdate(String.format("begin with label %s", dorisLabel));
        LOGGER.info(String.format("Doris begin with label %s", dorisLabel));
        stmt.close();
        PreparedStatement preStmt = conn.prepareStatement(sql);
        DorisPreparedStatement dorisPreStmt = new DorisPreparedStatement(dorisLabel, preStmt);
        return dorisPreStmt;
    }

    /**
     * call commit in doris
     * @param dorisPreStmt contains statement which is created in beginTransaction
     * @throws Exception when commit is failed
     */
    @Override
    public void preCommit(DorisPreparedStatement dorisPreStmt) throws Exception {
        LOGGER.info("start preCommit...");
        try {
            DorisConnectUtil.commit(dorisPreStmt);
        } catch (Exception e) {
            DorisConnectUtil.checkTransaction(ip, port, user, pwd, dbName, jdbcParam, dorisPreStmt.getLabel());
        }
    }

    /**
     * nothing to do
     * @param dorisPreStmt contains statement which is created in beginTransaction
     */
    @Override
    public void commit(DorisPreparedStatement dorisPreStmt) {
        LOGGER.info("call commit");
    }

    /**
     * if invoke() throw exception, rollback.
     * @param dorisPreStmt contains statement which is created in beginTransaction
     */
    @Override
    public void abort(DorisPreparedStatement dorisPreStmt) {
        LOGGER.info("call rollback");
        DorisConnectUtil.rollback(dorisPreStmt);
    }
}