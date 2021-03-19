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

package org.apache.doris.common.plot;

import org.apache.doris.common.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.indvd00m.ascii.render.Region;
import com.indvd00m.ascii.render.Render;
import com.indvd00m.ascii.render.api.ICanvas;
import com.indvd00m.ascii.render.api.IContextBuilder;
import com.indvd00m.ascii.render.api.IRender;
import com.indvd00m.ascii.render.elements.Label;
import com.indvd00m.ascii.render.elements.Rectangle;
import com.indvd00m.ascii.render.elements.plot.Axis;
import com.indvd00m.ascii.render.elements.plot.AxisLabels;
import com.indvd00m.ascii.render.elements.plot.Plot;
import com.indvd00m.ascii.render.elements.plot.api.IPlotPoint;
import com.indvd00m.ascii.render.elements.plot.misc.PlotPoint;

import java.util.ArrayList;
import java.util.List;

// This class will draw a plot for the given points list.
public class Plotter {

    private static final int CANVAS_WIDTH = 100;
    private static final int CANVAS_HEIGHT = 25;

    private static final int LAYER_WIDTH = CANVAS_WIDTH - 1;
    private static final int LAYER_HEIGHT = CANVAS_HEIGHT - 1;

    private static final int RENDER_WIDTH = CANVAS_WIDTH - 4;
    private static final int RENDER_HEIGHT = CANVAS_HEIGHT - 4;

    public String draw(List<Pair<Long, Double>> rawPoints, String label) {
        Preconditions.checkState(!rawPoints.isEmpty());

        long baseX = rawPoints.get(0).first;
        List<IPlotPoint> points = new ArrayList<>();
        for (Pair<Long, Double> pair : rawPoints) {
            IPlotPoint plotPoint = new PlotPoint(pair.first - baseX, pair.second);
            points.add(plotPoint);
        }

        IRender render = new Render();
        IContextBuilder builder = render.newBuilder();
        builder.width(CANVAS_WIDTH).height(CANVAS_HEIGHT);
        Region layerRegion = new Region(1, 1, LAYER_WIDTH, LAYER_HEIGHT);
        Region region = new Region(0, 0, RENDER_WIDTH, RENDER_HEIGHT);

        builder.element(new Rectangle(0, 0, CANVAS_WIDTH, CANVAS_HEIGHT));
        builder.layer(layerRegion);
        builder.element(new Axis(points, region));
        builder.element(new AxisLabels(points, region));
        builder.element(new Plot(points, region));
        builder.element(new Label(label, (CANVAS_WIDTH - label.length()) / 2, RENDER_HEIGHT + 1, label.length()));

        ICanvas canvas = render.render(builder.build());
        return canvas.getText();
    }

    public static void main(String[] args) {
        Plotter plotter = new Plotter();
        List<Pair<Long, Double>> rawPoints = Lists.newArrayList();
        long time = System.currentTimeMillis() / 1000;
        for (long degree = 0; degree <= 100; degree++) {
            rawPoints.add(Pair.create(time, Double.valueOf(degree)));
            time += 15;
        }
        System.out.println(plotter.draw(rawPoints, "2020-10-11 00:00:00 ~ 2020-10-11 10:00:00"));
    }

}
