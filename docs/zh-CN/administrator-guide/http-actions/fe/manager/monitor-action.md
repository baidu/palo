---
{
    "title": "Monotor Action",
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

# Monitor Action

## Request

`GET /rest/v2/manager/monitor/value/{type}`

`POST /rest/v2/manager/monitor/timeserial/{type}`

## 获取值类型监控数据

`GET /rest/v2/manager/monitor/value/{type}`

### Description

用于获取集群值类型监控。

### Path parameters

* `type`

    指定监控类型，目前支持的类型有：  
    node_num, disks_capacity, statistic, fe_list, be_list  
    全部类型见: `org/apache/doris/metric/collector/ClusterInfo.ClusterInfoType`

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

获取磁盘空间：
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

## 获取时间序列监控信息

`POST /rest/v2/manager/monitor/timeserial/{type}`

### Description

用于获取fe或be时间序列监控数据。
    
### Path parameters

* `type`

    指定监控类型，目前支持的类型有：  
    fe监控：qps, query_latency, query_err_rate, conn_total, txn_status, scheduled_tablet_num;  
    be监控：be_cpu_idle, be_mem, be_disk_io, be_base_compaction_score, be_cumu_compaction_score.  
    全部时间序列监控类型见: `org/apache/doris/metric/collector/Monitor.MonitorType`

### Query parameters

* `start`

    指定获取一段时间监控数据的起始时间，值为时间戳。
    
* `end`

    指定获取一段时间监控数据的终止时间，值为时间戳。

### Request body

```
{
    "nodes":[
        "host:http_port"
    ],
    "point_num":num,
    "quantile":""
}

若不带body，body中的参数都使用默认值。  
nodes 用于指定返回哪些节点的监控数据，若该监控为fe的监控，应该指定fe_host:http_port,默认为所有fe节点, 若该监控为be的监控，应该指定fe_host:http_port,默认为所有be节点； 
point_num 用于指定返回每个节点监控数据的个数，默认为100；  
quantile 只有在 query_latency 监控中需要指定，值为："0.75"，"0.95"，"0.98"，"0.99"，"0.999"之一。
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
x_value 表示监控图x轴数据，值为时间戳列表。  
y_value 表示监控图各节点y轴数据。

对于 txn_status 监控，response 为：
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
begin 表示事务提交率，success 表示事务成功率， failed 表示事务失败率。
```
    
### Examples

1. 获取 qps 监控数据：

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

2. 获取 txn_status 监控数据：
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