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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.metric.collector.Monitor;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;

import com.clearspring.analytics.util.Lists;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// ADMIN SHOW FRONTEND[BACKEND] METRIC
// ("nodes" = "10001, 10002", "metrics" = "cpu_idle", "time" = "last 30 minutes");
public class AdminShowMetricStmt extends ShowStmt {
    private static final String KEY_NODES = "nodes";
    private static final String KEY_METRICS = "metrics";
    private static final String KEY_TIME = "time";

    public enum NodeType {
        FRONTEND,
        BACKEND
    }

    private NodeType nodeType;
    private Map<String, String> options;

    private List<Pair<String, Integer>> nodeHosts = Lists.newArrayList();
    private List<Monitor.MonitorType> monitorTypes = Lists.newArrayList();
    private Pair<Long, Long> timeRange;

    private static final Pattern TIME_PATTERN_LAST_MIN = Pattern.compile("^[Ll]ast ([1-9][0-9]*) minutes");
    private static final Pattern TIME_PATTERN_LAST_HOUR = Pattern.compile("^[Ll]ast ([1-9][0-9]*) hours");
    private static final Pattern TIME_PATTERN_LAST_DAY = Pattern.compile("^[Ll]ast ([1-9][0-9]*) days");
    private static final Pattern TIME_PATTERN_INTERVAL
            = Pattern.compile("([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}),[ ]*([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})");


    public AdminShowMetricStmt(NodeType nodeType, Map<String, String> options) {
        this.nodeType = nodeType;
        this.options = options;
    }

    public List<Pair<String, Integer>> getNodeHosts() {
        return nodeHosts;
    }

    public List<Monitor.MonitorType> getMonitorTypes() {
        return monitorTypes;
    }

    public Pair<Long, Long> getTimeRange() {
        return timeRange;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        // check auth
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        analyzeNode();
        analyzeMetric();
        analyzeTime();
    }

    private void analyzeNode() throws AnalysisException {
        if (!options.containsKey(KEY_NODES)) {
            // If not specify nodes, add all hosts
            switch (nodeType) {
                case FRONTEND:
                    List<Frontend> fes = Catalog.getCurrentCatalog().getFrontends(null);
                    for (Frontend fe : fes) {
                        nodeHosts.add(Pair.create(fe.getHost(), Config.http_port));
                    }
                    break;
                case BACKEND:
                    List<Backend> bes = Catalog.getCurrentSystemInfo().getClusterBackends(SystemInfoService.DEFAULT_CLUSTER);
                    for (Backend be : bes) {
                        nodeHosts.add(Pair.create(be.getHost(), be.getHttpPort()));
                    }
                    break;
                default:
                    break;
            }
            return;
        }

        String nodeList = options.get(KEY_NODES);
        String[] nodes = nodeList.replaceAll(" ", "").split(",");
        switch (nodeType) {
            case FRONTEND:
                for (String node : nodes) {
                    Frontend fe = Catalog.getCurrentCatalog().getFeByName(node);
                    if (fe == null) {
                        throw new AnalysisException("Unknown frontend name: " + node);
                    }
                    nodeHosts.add(Pair.create(fe.getHost(), Config.http_port));
                }
                break;
            case BACKEND:
                for (String node : nodes) {
                    Backend be = Catalog.getCurrentSystemInfo().getBackend(Long.valueOf(node));
                    if (be == null) {
                        throw new AnalysisException("Unknown backend name: " + node);
                    }
                    nodeHosts.add(Pair.create(be.getHost(), be.getHttpPort()));
                }
                break;
            default:
                break;
        }
    }

    private void analyzeMetric() throws AnalysisException {
        if (!options.containsKey(KEY_METRICS)) {
            throw new AnalysisException("Missing metrics property");
        }

        String metricList = options.get(KEY_METRICS);
        String[] metrics = metricList.replaceAll(" ", "").split(",");

        for (String metric : metrics) {
            try {
                Monitor.MonitorType monitorType = Monitor.MonitorType.valueOf(metric);
                if (!monitorType.nodeType.equalsIgnoreCase(nodeType.name())) {
                    throw new AnalysisException(metric + " is not a " + nodeType.name() + " metric");
                }
                monitorTypes.add(monitorType);
            } catch (IllegalArgumentException e) {
                throw new AnalysisException("Invalid metric name: " + metric);
            }
        }
    }

    private void analyzeTime() throws AnalysisException {
        String timeProp = options.get(KEY_TIME);
        if (timeProp == null) {
            timeProp = "Last 30 minutes";
        }

        // Last xx mintues
        Matcher m = TIME_PATTERN_LAST_MIN.matcher(timeProp);
        if (m.find()) {
            if (m.groupCount() != 1) {
                throw new AnalysisException("Invalid time option: " + timeProp);
            }
            Integer num = Integer.valueOf(m.group(1));
            long endTime = System.currentTimeMillis();
            timeRange = Pair.create(endTime - num * 60 * 1000L, endTime);
            return;
        }

        // Last xx hours
        m = TIME_PATTERN_LAST_HOUR.matcher(timeProp);
        if (m.find()) {
            if (m.groupCount() != 1) {
                throw new AnalysisException("Invalid time option: " + timeProp);
            }
            Integer num = Integer.valueOf(m.group(1));
            long endTime = System.currentTimeMillis();
            timeRange = Pair.create(endTime - num * 60 * 60 * 1000L, endTime);
            return;
        }

        // Last xx days
        m = TIME_PATTERN_LAST_DAY.matcher(timeProp);
        if (m.find()) {
            if (m.groupCount() != 1) {
                throw new AnalysisException("Invalid time option: " + timeProp);
            }
            Integer num = Integer.valueOf(m.group(1));
            long endTime = System.currentTimeMillis();
            timeRange = Pair.create(endTime - num * 24 * 60 * 60 * 1000L, endTime);
            return;
        }

        // 2020-10-11 00:00:00, 2021-10-10 12:44:55
        m = TIME_PATTERN_INTERVAL.matcher(timeProp);
        if (m.find()) {
            if (m.groupCount() != 2) {
                throw new AnalysisException("Invalid time option: " + timeProp);
            }
            String startTime = m.group(1);
            String endTime = m.group(2);
            long start = TimeUtils.timeStringToLong(startTime);
            long end = TimeUtils.timeStringToLong(endTime);
            if (start >= end) {
                throw new AnalysisException("Invalid time option: " + timeProp);
            }
            timeRange = Pair.create(start, end);
        }

        throw new AnalysisException("Invalid time option: " + timeProp);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (Monitor.MonitorType monitorType : monitorTypes) {
            builder.addColumn(new Column(monitorType.name(), ScalarType.createVarchar(65535)));
        }
        return builder.build();
    }
}

