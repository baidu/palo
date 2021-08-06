---
{
    "title": "Monotor Action",
    "language": "en"
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

# Monitor Action

## Request

`GET /rest/v2/manager/monitor/value/{type}`

`POST /rest/v2/manager/monitor/timeserial/{type}`

## 获取值类型监控数据

`GET /rest/v2/manager/monitor/value/{type}`

### Description

Used to get cluster value type monitoring.

### Path parameters

* `type`

    Specify the type of monitoring, the currently supported types are:  
    node_num, disks_capacity, statistic, fe_list, be_list  
    For all types see:  `org/apache/doris/metric/collector/ClusterInfo.ClusterInfoType`

### Response

```
node_num:
{
    "msg": "success",
    "code": 0,
    "data": {
        "fe_node_num_total": value,
	    "fe_node_num_alive": value,
	    "be_node_num_total": value,
	    "be_node_num_alive": value
    },
    "count": 0
}
```

```
disks_capacity:
{
    "msg": "success",
    "code": 0,
    "data": {
        "disks_used": value,
	    "disks_total": value
    },
    "count": 0
}
```

```
statistic:
{
    "msg": "success",
    "code": 0,
    "data": {
        "unhealthy_tablet_num": value
    },
    "count": 0
}
```

```
fe_list:
{
    "msg": "success",
    "code": 0,
    "data": [
        "fe_host:http_port"
    ],
    "count": 0
}
```

```
be_list:
{
    "msg": "success",
    "code": 0,
    "data": [
        "be_host:http_port"
    ],
    "count": 0
}
```

### Examples

Get disks_capacity:
```
GET /rest/v2/manager/monitor/value/disks_capacity

{
    "msg": "success",
    "code": 0,
    "data": {
        "be_disks_total": 7750977622016,
        "be_disks_used": 2004
    },
    "count": 0
}
```

## Get time series monitoring information

`POST /rest/v2/manager/monitor/timeserial/{type}`

### Description

Used to obtain fe or be time series monitoring data.
    
### Path parameters

* `type`

    Specify the type of monitoring, currently supported types are:  
    fe monitoring: qps, query_latency, query_err_rate, conn_total, txn_status, scheduled_tablet_num;  
    be monitoring: be_cpu_idle, be_mem, be_disk_io, be_base_compaction_score, be_cumu_compaction_score.  
    For all time series monitor types see: `org/apache/doris/metric/collector/Monitor.MonitorType`

### Query parameters

* `start`

    Specifies the start time to get a period of monitoring data, the value is a timestamp.
    
* `end`

    Specifies the end time to get the monitored data for a period of time, the value is a timestamp.

### Request body

```
{
    "nodes":[
        "host:http_port"
    ],
    "point_num":num,
    "quantile":""
}

If no body is included, all parameters in the body will use the default value.  
nodes for specifying which nodes to return monitoring data, if the monitoring is for fe monitoring, should specify fe_host:http_port, default for all fe nodes, if the monitoring is for be monitoring, should specify fe_host:http_port, default for all be nodes. 
point_num is used to specify the number of monitoring data returned for each node, the default is 100.  
quantile only needs to be specified in query_latency monitoring, the value is one of: "0.75", "0.95", "0.98", "0.99", "0.999".
```

### Response

```
{
    "msg": "success",
    "code": 0,
    "data": {
        "x_value": [
            1627527825000,
            1627527840000,
            ...
        ],
        "y_value": {
            "host:http_port": [
                0.0,
                0.0,
                ...
            ]
        }
    },
    "count": 0
}
x_value indicates the monitoring graph x-axis data, and the value is a list of timestamps.  
y_value indicates the y-axis data of each node of the monitoring graph.

For the txn_status monitor, the response is:
{
    "msg": "success",
    "code": 0,
    "data": {
        "x_value": [
        ],
        "y_value": {
            "host:http_port": {
                "begin":[

                ],
                "success":[

                ],
                "failed":[

                ]
            }
        }
    },
    "count": 0
}
begin indicates the transaction commit rate, success indicates the transaction success rate, and failed indicates the transaction failure rate.
```
    
### Examples

1. Get qps monitoring data:

    ```
    GET /rest/v2/manager/monitor/timeserial/qps?start=1627527795753&end=1627528695763
    
    Response:
    {
    "msg": "success",
    "code": 0,
    "data": {
        "x_value": [
            1627527810000,
            1627527825000,
            ...
        ],
        "y_value": {
            "xxxx:8030": [
                0.0,
                0.0,
                ...
            ]
        }
    },
    "count": 0
    }
    ```

2. Get txn_status monitoring data:
    ```
    GET /rest/v2/manager/monitor/timeserial/txn_status?start=1627527795753&end=1627528695763
    
    Response:
    {
    "msg": "success",
    "code": 0,
    "data": {
        "x_value": [
            1627528095000,
            1627528395000,
            ...
        ],
        "y_value": {
            "xxxx:8030": {
                "success": [
                    0.0,
                    0.0,
                    ...
                ],
                "failed": [
                    0.0,
                    0.0,
                    ...
                ],
                "begin": [
                    0.0,
                    0.0,
                    ...
                ]
            }
        }
    },
    "count": 0
    }
    ```