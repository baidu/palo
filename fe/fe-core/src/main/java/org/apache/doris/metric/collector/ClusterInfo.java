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

package org.apache.doris.metric.collector;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.common.proc.StatisticProcDir;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This class provides monitoring data of cluster information.
 */
@Deprecated
public class ClusterInfo {
    private static final Logger LOG = LogManager.getLogger(ClusterInfo.class);

    public enum ClusterInfoType {
        NODE_NUM,
        DISKS_CAPACITY,
        STATISTIC,
        FE_LIST,
        BE_LIST
    }

    public static Object getClusterInfo(ClusterInfoType type) {
        switch (type) {
            case NODE_NUM:
                return nodeNum();
            case DISKS_CAPACITY:
                return clusterCapacity();
            case STATISTIC:
                return fetchStatistic();
            case BE_LIST:
                return beList();
            case FE_LIST:
                return feList();
            default:
                return "";
        }
    }

    /*
     * {
     * 		"fe_node_num_total": value,
     * 		"fe_node_num_alive": value,
     * 		"be_node_num_total": value,
     * 		"be_node_num_alive": value
     * }
     */
    private static Map<String, Integer> nodeNum() {
        Map<String, Integer> result = Maps.newHashMap();
        List<Frontend> frontends = Catalog.getCurrentCatalog().getFrontends(null);
        int feNodeNumAlive = 0;
        for (Frontend frontend : frontends) {
            if (frontend.isAlive()) {
                ++feNodeNumAlive;
            }
        }
        result.put(BDBJEMetricUtils.FE_NODE_NUM_TOTAL, frontends.size());
        result.put(BDBJEMetricUtils.FE_NODE_NUM_ALIVE, feNodeNumAlive);
        result.put(BDBJEMetricUtils.BE_NODE_NUM_TOTAL, Catalog.getCurrentSystemInfo()
                .getBackendIds(false).size());
        result.put(BDBJEMetricUtils.BE_NODE_NUM_ALIVE, Catalog.getCurrentSystemInfo()
                .getBackendIds(true).size());
        return result;
    }

    /*
     * {
     * 		"disks_used": value,
     * 		"disks_total": value
     * }
     */
    private static Map<String, Long> clusterCapacity() {
        Map<String, Long> result = Maps.newHashMap();
        result.put(BDBJEMetricUtils.BE_DISKS_DATA_USED_CAPACITY,
                Catalog.getCurrentCatalog().getBDBJEMetricHandler().readLong(BDBJEMetricUtils.BE_DISKS_DATA_USED_CAPACITY));
        result.put(BDBJEMetricUtils.BE_DISKS_TOTAL_CAPACITY,
                Catalog.getCurrentCatalog().getBDBJEMetricHandler().readLong(BDBJEMetricUtils.BE_DISKS_TOTAL_CAPACITY));
        return result;
    }

    /*
     * {
     *      "unhealthy_tablet_num": value
     * }
     */
    private static Map<String, Long> fetchStatistic() {
        Map<String, Long> statistics = Maps.newHashMap();
        long unhealthyTabletNum;
        String procPath = "/statistic";
        try {
            ProcNodeInterface nodeInterface = ProcService.getInstance().open(procPath);
            ProcResult result = nodeInterface.fetchResult();
            List<List<String>> rows = result.getRows();
            Objects.nonNull(rows);
            if (rows == null || rows.size() <= 0 || !result.getColumnNames().contains(StatisticProcDir.UNHEALTHY_TABLET_NUM)) {
                LOG.warn("failed to fetch UnhealthyTabletNum from proc '\"/statistic\"");
                return null;
            }
            List<String> totalRow = rows.get(rows.size() - 1);
            String unhealthyTablet = totalRow.get(result.getColumnNames().indexOf(StatisticProcDir.UNHEALTHY_TABLET_NUM));
            unhealthyTabletNum = Long.parseLong(unhealthyTablet);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
            return null;
        }
        statistics.put(BDBJEMetricUtils.UNHEALTHY_TABLET_NUM, unhealthyTabletNum);
        return statistics;
    }

    /*
     * [
     *      "fe_host:http_port"
     * ]
     */
    static List<String> feList() {
        List<Frontend> frontends = Catalog.getCurrentCatalog().getFrontends(null);
        List<String> result = Lists.newArrayList();
        for (Frontend frontend : frontends) {
            result.add(NetUtils.getHostnameByIp(frontend.getHost()) + ":" + Config.http_port);
        }
        return result;
    }

    /*
     * [
     *      "be_host:http_port"
     * ]
     */
    static List<String> beList() {
        SystemInfoService infoService = Catalog.getCurrentSystemInfo();
        List<Long> backendIds = infoService.getBackendIds(false);
        List<String> result = Lists.newArrayList();
        for (long backendId : backendIds) {
            Backend backend = infoService.getBackend(backendId);
            result.add(NetUtils.getHostnameByIp(backend.getHost()) + ":" + backend.getHttpPort());
        }
        return result;
    }
}
