// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.qe;

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DdlStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.KillStmt;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.ShowAuthorStmt;
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UseStmt;
import org.apache.doris.analysis.ValueList;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.StreamLoadPlanner;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Status;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPlanFragmentExecParams;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionState;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.glassfish.jersey.internal.guava.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import java_cup.runtime.Symbol;
import mockit.Expectations;
import mockit.Mocked;
import static org.apache.doris.catalog.Table.TableType.OLAP;

public class StmtExecutorTest {
    private ConnectContext ctx;
    private QueryState state;
    private ConnectScheduler scheduler;
    private MysqlChannel channel = null;

    @Mocked
    SocketChannel socketChannel;

    @BeforeClass
    public static void start() {
        MetricRepo.init();
        try {
            FrontendOptions.init();
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() throws IOException {
        state = new QueryState();
        scheduler = new ConnectScheduler(10);
        ctx = new ConnectContext(socketChannel);

        SessionVariable sessionVariable = new SessionVariable();
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        Catalog catalog = AccessTestUtil.fetchAdminCatalog();

        channel = new MysqlChannel(socketChannel);
        new Expectations(channel) {
            {
                channel.sendOnePacket((ByteBuffer) any);
                minTimes = 0;

                channel.reset();
                minTimes = 0;
            }
        };

        new Expectations(ctx) {
            {
                ctx.getMysqlChannel();
                minTimes = 0;
                result = channel;

                ctx.getSerializer();
                minTimes = 0;
                result = serializer;

                ctx.getCatalog();
                minTimes = 0;
                result = catalog;

                ctx.getState();
                minTimes = 0;
                result = state;

                ctx.getConnectScheduler();
                minTimes = 0;
                result = scheduler;

                ctx.getConnectionId();
                minTimes = 0;
                result = 1;

                ctx.getQualifiedUser();
                minTimes = 0;
                result = "testUser";

                ctx.getForwardedStmtId();
                minTimes = 0;
                result = 123L;

                ctx.setKilled();
                minTimes = 0;

                ctx.updateReturnRows(anyInt);
                minTimes = 0;

                ctx.setQueryId((TUniqueId) any);
                minTimes = 0;

                ctx.queryId();
                minTimes = 0;
                result = new TUniqueId();

                ctx.getStartTime();
                minTimes = 0;
                result = 0L;

                ctx.getDatabase();
                minTimes = 0;
                result = "testCluster:testDb";

                ctx.getSessionVariable();
                minTimes = 0;
                result = sessionVariable;

                ctx.setStmtId(anyLong);
                minTimes = 0;

                ctx.getStmtId();
                minTimes = 0;
                result = 1L;
            }
        };
    }

    @Test
    public void testSelect(@Mocked QueryStmt queryStmt,
                           @Mocked SqlParser parser,
                           @Mocked Planner planner,
                           @Mocked Coordinator coordinator) throws Exception {
        Catalog catalog = Catalog.getCurrentCatalog();
        Deencapsulation.setField(catalog, "canRead", new AtomicBoolean(true));

        new Expectations() {
            {
                queryStmt.analyze((Analyzer) any);
                minTimes = 0;

                queryStmt.getColLabels();
                minTimes = 0;
                result = Lists.<String>newArrayList();

                queryStmt.getResultExprs();
                minTimes = 0;
                result = Lists.<Expr>newArrayList();

                queryStmt.isExplain();
                minTimes = 0;
                result = false;

                queryStmt.getTables((Analyzer) any, (SortedMap) any, Sets.newHashSet());
                minTimes = 0;

                queryStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                queryStmt.rewriteExprs((ExprRewriter) any);
                minTimes = 0;

                Symbol symbol = new Symbol(0, Lists.newArrayList(queryStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                planner.plan((QueryStmt) any, (Analyzer) any, (TQueryOptions) any);
                minTimes = 0;

                // mock coordinator
                coordinator.exec();
                minTimes = 0;

                coordinator.endProfile();
                minTimes = 0;

                coordinator.getQueryProfile();
                minTimes = 0;
                result = new RuntimeProfile();

                coordinator.getNext();
                minTimes = 0;
                result = new RowBatch();

                coordinator.getJobId();
                minTimes = 0;
                result = -1L;

                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.EOF, state.getStateType());
    }

    @Test
    public void testShow(@Mocked ShowStmt showStmt, @Mocked SqlParser parser, @Mocked ShowExecutor executor) throws Exception {
        new Expectations() {
            {
                showStmt.analyze((Analyzer) any);
                minTimes = 0;

                showStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                showStmt.toSelectStmt((Analyzer) any);
                minTimes = 0;
                result = null;

                Symbol symbol = new Symbol(0, Lists.newArrayList(showStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                // mock show
                List<List<String>> rows = Lists.newArrayList();
                rows.add(Lists.newArrayList("abc", "bcd"));
                executor.execute();
                minTimes = 0;
                result = new ShowResultSet(new ShowAuthorStmt().getMetaData(), rows);
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.EOF, state.getStateType());
    }

    @Test
    public void testShowNull(@Mocked ShowStmt showStmt, @Mocked SqlParser parser, @Mocked ShowExecutor executor) throws Exception {
        new Expectations() {
            {
                showStmt.analyze((Analyzer) any);
                minTimes = 0;

                showStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                showStmt.toSelectStmt((Analyzer) any);
                minTimes = 0;
                result = null;

                Symbol symbol = new Symbol(0, Lists.newArrayList(showStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                // mock show
                List<List<String>> rows = Lists.newArrayList();
                rows.add(Lists.newArrayList("abc", "bcd"));
                executor.execute();
                minTimes = 0;
                result = null;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testKill(@Mocked KillStmt killStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                killStmt.analyze((Analyzer) any);
                minTimes = 0;

                killStmt.getConnectionId();
                minTimes = 0;
                result = 1L;

                killStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(killStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        new Expectations(scheduler) {
            {
                // suicide
                scheduler.getContext(1L);
                result = ctx;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testKillOtherFail(@Mocked KillStmt killStmt, @Mocked SqlParser parser, @Mocked ConnectContext killCtx) throws Exception {
        Catalog killCatalog = AccessTestUtil.fetchAdminCatalog();

        new Expectations() {
            {
                killStmt.analyze((Analyzer) any);
                minTimes = 0;

                killStmt.getConnectionId();
                minTimes = 0;
                result = 1L;

                killStmt.isConnectionKill();
                minTimes = 0;
                result = true;

                killStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(killStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                killCtx.getCatalog();
                minTimes = 0;
                result = killCatalog;

                killCtx.getQualifiedUser();
                minTimes = 0;
                result = "blockUser";

                killCtx.kill(true);
                minTimes = 0;

                ConnectContext.get();
                minTimes = 0;
                result = ctx;
            }
        };

        new Expectations(scheduler) {
            {
                // suicide
                scheduler.getContext(1L);
                result = killCtx;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testKillOther(@Mocked KillStmt killStmt, @Mocked SqlParser parser, @Mocked ConnectContext killCtx) throws Exception {
        Catalog killCatalog = AccessTestUtil.fetchAdminCatalog();
        new Expectations() {
            {
                killStmt.analyze((Analyzer) any);
                minTimes = 0;

                killStmt.getConnectionId();
                minTimes = 0;
                result = 1L;

                killStmt.isConnectionKill();
                minTimes = 0;
                result = true;

                killStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(killStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                killCtx.getCatalog();
                minTimes = 0;
                result = killCatalog;

                killCtx.getQualifiedUser();
                minTimes = 0;
                result = "killUser";

                killCtx.kill(true);
                minTimes = 0;

                ConnectContext.get();
                minTimes = 0;
                result = ctx;
            }
        };

        new Expectations(scheduler) {
            {
                // suicide
                scheduler.getContext(1L);
                result = killCtx;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testKillNoCtx(@Mocked KillStmt killStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                killStmt.analyze((Analyzer) any);
                minTimes = 0;

                killStmt.getConnectionId();
                minTimes = 0;
                result = 1L;

                killStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(killStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        new Expectations(scheduler) {
            {
                scheduler.getContext(1L);
                result = null;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testSet(@Mocked SetStmt setStmt, @Mocked SqlParser parser, @Mocked SetExecutor executor) throws Exception {
        new Expectations() {
            {
                setStmt.analyze((Analyzer) any);
                minTimes = 0;

                setStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(setStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                // Mock set
                executor.execute();
                minTimes = 0;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testStmtWithUserInfo(@Mocked StatementBase stmt, @Mocked ConnectContext context) throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, stmt);
        Deencapsulation.setField(stmtExecutor, "parsedStmt", null);
        Deencapsulation.setField(stmtExecutor, "originStmt", new OriginStatement("show databases;", 1));
        stmtExecutor.execute();
        StatementBase newstmt = (StatementBase)Deencapsulation.getField(stmtExecutor, "parsedStmt");
        Assert.assertTrue(newstmt.getUserInfo() != null);
    }

    @Test
    public void testSetFail(@Mocked SetStmt setStmt, @Mocked SqlParser parser, @Mocked SetExecutor executor) throws Exception {
        new Expectations() {
            {
                setStmt.analyze((Analyzer) any);
                minTimes = 0;

                setStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(setStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                // Mock set
                executor.execute();
                minTimes = 0;
                result = new DdlException("failed");
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testDdl(@Mocked DdlStmt ddlStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                ddlStmt.analyze((Analyzer) any);
                minTimes = 0;

                ddlStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(ddlStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        DdlExecutor ddlExecutor = new DdlExecutor();
        new Expectations(ddlExecutor) {
            {
                // Mock ddl
                DdlExecutor.execute((Catalog) any, (DdlStmt) any);
                minTimes = 0;
            }
        };

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testDdlFail(@Mocked DdlStmt ddlStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                ddlStmt.analyze((Analyzer) any);
                minTimes = 0;

                ddlStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(ddlStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        DdlExecutor ddlExecutor = new DdlExecutor();
        new Expectations(ddlExecutor) {
            {
                // Mock ddl
                DdlExecutor.execute((Catalog) any, (DdlStmt) any);
                minTimes = 0;
                result = new DdlException("ddl fail");
            }
        };

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testDdlFail2(@Mocked DdlStmt ddlStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                ddlStmt.analyze((Analyzer) any);
                minTimes = 0;

                ddlStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(ddlStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        DdlExecutor ddlExecutor = new DdlExecutor();
        new Expectations(ddlExecutor) {
            {
                // Mock ddl
                DdlExecutor.execute((Catalog) any, (DdlStmt) any);
                minTimes = 0;
                result = new Exception("bug");
            }
        };

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testUse(@Mocked UseStmt useStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                useStmt.analyze((Analyzer) any);
                minTimes = 0;

                useStmt.getDatabase();
                minTimes = 0;
                result = "testCluster:testDb";

                useStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                useStmt.getClusterName();
                minTimes = 0;
                result = "testCluster";

                Symbol symbol = new Symbol(0, Lists.newArrayList(useStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testUseFail(@Mocked UseStmt useStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                useStmt.analyze((Analyzer) any);
                minTimes = 0;

                useStmt.getDatabase();
                minTimes = 0;
                result = "blockDb";

                useStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                useStmt.getClusterName();
                minTimes = 0;
                result = "testCluster";

                Symbol symbol = new Symbol(0, Lists.newArrayList(useStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testInsert(@Mocked InsertStmt insertStmt,
                           @Mocked Catalog catalog,
                           @Mocked SelectStmt queryStmt,
                           @Mocked Expr expr,
                           @Mocked Database db) throws Exception {

        TExecPlanFragmentParams execPlanFragmentParams = new TExecPlanFragmentParams();
        execPlanFragmentParams.params = new TPlanFragmentExecParams();

        ArrayList<Expr> exprs = new ArrayList<>();
        exprs.add(expr);
        ValueList values = new ValueList(exprs);


        ConnectContext localCtx = new ConnectContext();

        TransactionEntry txnEntry = new TransactionEntry();
        localCtx.setTxnEntry(txnEntry);

        TTxnParams txnParams = new TTxnParams();
        txnParams.setFragmentInstanceId(new TUniqueId());
        txnParams.getFragmentInstanceId().setHi(11111).setLo(2222);
        txnParams.setNeedTxn(true);
        txnParams.setTxnId(111);
        txnParams.setDb("db1");
        txnParams.setTbl("tbl1");
        txnEntry.setTxnConf(txnParams);
        txnEntry.setBackend(new Backend());

        Table table = new Table(OLAP);
        List<Column> cols = new ArrayList();
        cols.add(new Column());
        table.setNewFullSchema(cols);
        txnEntry.setTable(table);

        new Expectations(ctx) {
            {
                insertStmt.analyze((Analyzer) any);
                minTimes = 0;

                insertStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                ConnectContext.get();
                minTimes = 0;
                result = localCtx;

                insertStmt.getQueryStmt();
                minTimes = 0;
                result = queryStmt;

                insertStmt.getDb();
                minTimes = 0;
                result = "db1";

                insertStmt.getTbl();
                minTimes = 0;
                result = "tbl1";

                expr.getResultValue();
                minTimes = 0;
                result = "data";

                queryStmt.getValueList();
                minTimes = 0;
                result = values;

            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(localCtx, "insert into Table");
        Deencapsulation.setField(stmtExecutor, "parsedStmt", insertStmt);
        Deencapsulation.setField(localCtx, "mysqlChannel", channel);

        stmtExecutor.execute();
        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());

    }

    @Test
    public void testInsertFail(@Mocked InsertStmt insertStmt,
                               @Mocked Catalog catalog,
                               @Mocked SelectStmt queryStmt,
                               @Mocked Expr expr,
                               @Mocked GlobalTransactionMgr txnMgr,
                               @Mocked MasterTxnExecutor masterTxnExecutor,
                               @Mocked SessionVariable sessionVariable,
                               @Mocked StreamLoadPlanner streamLoadPlanner,
                               @Mocked Database db,
                               @Mocked SystemInfoService systemInfoService,
                               @Mocked Backend backend,
                               @Mocked BackendServiceProxy backendServiceProxy,
                               @Mocked Future<InternalService.PExecPlanFragmentResult> execFuture) throws Exception {

        TExecPlanFragmentParams execPlanFragmentParams = new TExecPlanFragmentParams();
        execPlanFragmentParams.params = new TPlanFragmentExecParams();

        ArrayList<Expr> exprs = new ArrayList<>();
        exprs.add(expr);
        ValueList values = new ValueList(exprs);

        TransactionEntry txnEntry = new TransactionEntry();
        TTxnParams txnParams = new TTxnParams();
        txnParams.setFragmentInstanceId(new TUniqueId());
        txnParams.getFragmentInstanceId().setHi(11111).setLo(2222);
        txnEntry.setTxnConf(txnParams);
        txnEntry.setBackend(new Backend());

        List<Long> beIds = new ArrayList<>();
        beIds.add(1L);

        InternalService.PExecPlanFragmentResult execPlanFragmentResult = InternalService.PExecPlanFragmentResult.newBuilder()
                .setStatus(Status.PStatus.newBuilder().setStatusCode(1).build()).build();

        Map<Long, Backend> map = new HashMap<>();
        map.put(1L, backend);
        ImmutableMap<Long, Backend> idMap = ImmutableMap.copyOf(map);

        new Expectations() {
            {
                insertStmt.analyze((Analyzer) any);
                minTimes = 0;

                insertStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                streamLoadPlanner.plan((TUniqueId) any);
                minTimes = 0;
                result = execPlanFragmentParams;

                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getDb(anyString);
                minTimes = 0;
                result = db;

                db.getTable(anyString);
                minTimes = 0;
                result = new OlapTable();

                catalog.isMaster();
                minTimes = 0;
                result = true;

                Catalog.getCurrentGlobalTransactionMgr();
                minTimes = 0;
                result = txnMgr;

                Catalog.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;

                systemInfoService.seqChooseBackendIds(anyInt, anyBoolean, anyBoolean, anyString);
                minTimes = 0;
                result = beIds;

                BackendServiceProxy.getInstance();
                minTimes = 0;
                result = backendServiceProxy;

                backendServiceProxy.execPlanFragmentAsync((TNetworkAddress) any, (TExecPlanFragmentParams) any);
                minTimes = 0;
                result = execFuture;

                execFuture.get(anyLong, (TimeUnit) any);
                minTimes = 0;
                result = execPlanFragmentResult;

                systemInfoService.getIdToBackend();
                minTimes = 0;
                result = idMap;

                txnMgr.beginTransaction(anyLong, (List<Long>) any, anyString,
                        (TransactionState.TxnCoordinator) any, (TransactionState.LoadJobSourceType) any, anyLong);
                minTimes = 0;
                result = 100L;

                insertStmt.getQueryStmt();
                minTimes = 0;
                result = queryStmt;

                expr.getResultValue();
                minTimes = 0;
                result = "data";

                queryStmt.getValueList();
                minTimes = 0;
                result = values;

                ConnectContext.get();
                minTimes = 0;
                result = ctx;

                ctx.getSessionVariable();
                minTimes = 0;
                result = sessionVariable;

                sessionVariable.getQueryTimeoutS();
                minTimes = 0;
                result = 1000;

                ctx.isTxnModel();
                minTimes = 0;
                result = true;

                ctx.isTxnIniting();
                minTimes = 0;
                result = true;

                ctx.getTxnEntry();
                minTimes = 0;
                result = txnEntry;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        Deencapsulation.setField(stmtExecutor, "parsedStmt", insertStmt);

        stmtExecutor.execute();
        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());

    }

}


