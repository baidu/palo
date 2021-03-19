---
{
    "title": "ADMIN SHOW METRIC",
    "language": "zh-CN"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# ADMIN SHOW METRIC
## description

    该语句用于在命令行以 ascii 图标的形式展示系统监控趋势图

    语法：

        ADMIN SHOW FRONTEND[BACKNED] METRIC
        [options];

        options:

        (
            "nodes" = "xx",
            "metrics" = "xx",
            "time" = "xx"
        )

        nodes: 指定节点列表，以逗号分割。对于 BE，指定 BE ID；对于 FE，指定 FE name。如果不指定，默认展示所有节点。
        metrics：监控指标名称。目前支持以下指标：

            FE：
                QUERY_ERR_RATE, QPS, CONN_TOTAL
            BE：
                BE_CPU_IDLE, BE_MEM, BE_DISK_IO, BE_BASE_COMPACTION_SCORE, BE_BASE_COMPACTION_SCORE

        time：指定查看的时间段。如果不指定，默认查询最近30分钟的。支持以下格式：

            last xx mintues
            last xx hours
            last xx days
            2020-10-10 00:00:00, 2020-11-10 00:00:00

## example

    1. 查看最近20分钟，全部 BE 节点的 BE_CPU_IDLE 和 BE_MEM 趋势图：

        ADMIN SHOW BACKEND METRIC (
            "metrics" = "BE_CPU_IDLE, BE_MEM",
            "time" = "last 20 minutes"
        );

    2. 查看指定 BE 节点的 BE_DISK_IO：

        ADMIN SHOW BACKEND METRIC (
            "nodes" = "10002, 10003",
            "metrics" = "BE_DISK_IO",
            "time" = "last 4 hours"
        );
        
    3. 查看指定 FE 节点的 CONN_TOTAL

        ADMIN SHOW FRONTEND METRIC (
            "nodes" = "172.0.0.1_9217_1616598989025",
            "metrics" = "CONN_TOTAL",
            "time" = "last 4 hours"
        );
        
## keyword

    ADMIN,SHOW,METRIC

