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

package org.apache.doris.metric.drawer;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.plot.Plotter;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.metric.collector.Monitor;

import com.google.common.collect.Lists;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;

// This is a helper class.
// It will get the metrics from Monitor.monitoring() and draw a plot
public class MetricDrawer {

    public static String draw(long startTime, long endTime, String node, Monitor.MonitorType type) throws UserException {
        // assemble the request json body
        JSONObject object = new JSONObject();
        object.put(Monitor.POINT_NUM, Monitor.DEFAULT_MONITOR_POINTS);
        JSONArray array = new JSONArray();
        array.put(node);
        object.put(Monitor.NODES, array);

        // get metric chart data
        Object o = Monitor.monitoring(startTime, endTime, object.toString(), type);
        if (!(o instanceof Monitor.ChartData)) {
            throw new AnalysisException("Not chart data");
        }
        Monitor.ChartData chartData = (Monitor.ChartData) o;
        List<Long> xValues = chartData.x_value;
        List yValues = (List) chartData.y_value.get(node);
        if (yValues == null) {
            return "Failed to get metrics points for: " + type.name() + ", node: " + node;
        }
        List<Pair<Long, Double>> rawPoints = Lists.newArrayList();
        for (int i = 0; i < xValues.size(); i++) {
            if (yValues.get(i) == null) {
                continue;
            }
            rawPoints.add(Pair.create(xValues.get(i) / 1000, Double.valueOf(yValues.get(i).toString())));
        }
        if (rawPoints.isEmpty()) {
            return "Failed to get metrics points for: " + type.name() + ", node: " + node;
        }

        // draw the plot
        Plotter plotter = new Plotter();
        return "\n" + plotter.draw(rawPoints, buildLabel(node, startTime, endTime));
    }

    private static String buildLabel(String node, long startTime, long endTime) {
        String start = TimeUtils.longToTimeString(startTime);
        String end = TimeUtils.longToTimeString(endTime);
        return node + ": [" + start + " ~ " + end + "]";
    }
}

