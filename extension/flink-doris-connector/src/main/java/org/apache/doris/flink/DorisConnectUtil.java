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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Doris connection class
 */
public class DorisConnectUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(DorisConnectUtil.class);

    private static final int MAX_RETRY_TIMES = 3;

    private static final int MAX_COMMIT_RETRY_TIMES = 3;

    /**
     * get Connection
     *
     * @param ip the ip of doris fe
     * @param port the port of doris fe
     * @param user login user
     * @param password login password
     * @param dbName database name
     * @param jdbcParam jdbc param, such as socketTimeout
     * @return Connection a new connection of jdbc
     * @throws SQLException
     */
    public static Connection getConnection(final String ip, final int port, final String user,
                                           final String password, final String dbName, final String jdbcParam) throws SQLException {
        String url = String.format("jdbc:mysql://%s:%d/%s?rewriteBatchedStatements=true&%s",
                ip, port, dbName, jdbcParam);
        for (int i = 0; i < MAX_RETRY_TIMES; ++i) {
            try {
                Class.forName("com.mysql.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                LOGGER.error("Get mysql.jdbc.Driver failed");
                throw new SQLException(e.getMessage());
            }
            try {
                Connection conn = DriverManager.getConnection(url, user, password);
                LOGGER.info("Get connection successfully.");
                return conn;
            } catch (Exception e) {
                LOGGER.error("Get connection failed: url: " + url + ", user: " + user);
                throw new SQLException(e.getMessage());
            }
        }
        throw new SQLException("Get connection failed");
    }

    /**
     * commit
     *
     * @param dorisPreStmt the statement for this transaction
     */
    public static void commit(DorisPreparedStatement dorisPreStmt) throws SQLException {
        PreparedStatement stmt = dorisPreStmt.getPreStmt();
        if (stmt != null) {
            Connection conn = getConnection(stmt);
            if (conn == null) {
                return;
            }
            stmt.executeBatch();
            stmt.close();
            Statement st = conn.createStatement();
            st.executeUpdate("commit");
            st.close();
            close(conn);
        }
    }

    /**
     * rollback
     *
     * @param dorisPreStmt the statement for this transaction
     */
    public static void rollback(DorisPreparedStatement dorisPreStmt) {
        PreparedStatement stmt = dorisPreStmt.getPreStmt();
        if (stmt != null) {
            Connection conn = getConnection(stmt);
            if (conn == null) {
                return;
            }
            try {
                stmt.close();
                Statement st = conn.createStatement();
                st.executeUpdate("rollback");
                st.close();
                close(conn);
            } catch (Exception e) {
                LOGGER.error("Doris rollback failed.", e);
            }
        }
    }

    /**
     * check the transaction status, committed or not
     *
     * @param ip the ip of doris fe
     * @param port the port of doris fe
     * @param user login user
     * @param password login password
     * @param dbName database name
     * @param jdbcParam jdbc param, such as socketTimeout
     * @param dorisLabel the label of the transaction
     * @throws SQLException if the transaction is not VISIBLE, throw an exception
     */
    public static void checkTransaction(
            final String ip, final int port, final String user, final String password, final String dbName,
            final String jdbcParam, final String dorisLabel) throws SQLException {
        for (int i = 0; i < MAX_COMMIT_RETRY_TIMES; ++i) {
            try {
                Connection conn = DorisConnectUtil.getConnection(
                        ip, port, user, password, dbName, jdbcParam);
                Statement stmt = conn.createStatement();
                stmt.executeQuery(String.format("show transaction where label = '%s'", dorisLabel));
                ResultSet rs = stmt.getResultSet();
                String txnStatus = null;
                while (rs.next()) {
                    txnStatus = rs.getString("TransactionStatus");
                }
                stmt.close();
                close(conn);
                if (txnStatus == null) {
                    LOGGER.error("check transaction return null, label is invalid");
                    throw new SQLException("check transaction return null, label is invalid");
                }
                if (txnStatus.equalsIgnoreCase("ABORTED")) {
                    LOGGER.error("check transaction return abort");
                    throw new SQLException("check transaction return ABORTED");
                }
                if (!txnStatus.equalsIgnoreCase("VISIBLE")) {
                    LOGGER.error("transaction is not completed. waiting");
                    try {
                        Thread.sleep(10000L);
                    } catch (InterruptedException e) {
                        LOGGER.error(e.getMessage());
                    }
                    continue;
                }
                return;
            } catch (SQLException ex) {
                LOGGER.error("check transaction failed: " + ex.getMessage());
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage());
                }
            }
        }
        LOGGER.error("check transaction failed: MAX_COMMIT_RETRY_TIMES");
        throw new SQLException("check transaction failed, exceed upper limit MAX_COMMIT_RETRY_TIMES");
    }

    /**
     * get connection from the statement
     *
     * @param stmt the statement which is connected
     * @return the connection for the statement
     */
    private static Connection getConnection(PreparedStatement stmt) {
        try {
            return stmt.getConnection();
        } catch (Exception e) {
            LOGGER.error("getConnection null.", e);
            return null;
        }
    }
    /**
     * close connection
     *
     * @param conn the connection need to be closed
     */
    public static void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOGGER.error("close doris connection error", e);
            }
        }
    }
}
