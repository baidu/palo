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

package org.apache.doris.httpv2.restv2;

import lombok.Getter;
import lombok.Setter;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.thrift.TBrokerFileStatus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.Lists;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/rest/v2")
public class ImportAction {

    private static final Logger LOG = LogManager.getLogger(ImportAction.class);

    private static final long MAX_READ_LEN_BYTES = 1024 * 1024; // 1MB

    /**
     * Request body:
     * {
     *  "fileInfo": {
     *      "columnSeparator": ",",//文件的分隔符
     *      "fileUrl": "hdfs://127.0.0.1:50070/file/test/text*",
     *      "format": "TXT",//文件类型，包括TXT,ORC,Parquet,CSV四种
     *  },
     *  "connectInfo": {    // 可选
     *      "brokerName" : "my_broker"
     *      "brokerProps" : {
     *          "username" : "yyy",
     *          "password" : "xxx"
     *      }
     *  }
     * }
     */
    @RequestMapping(path = "/api/import/file_review", method = RequestMethod.POST)
    public Object fileReview(@RequestBody FileReviewRequestVo body, HttpServletRequest request, HttpServletResponse response) {
        FileInfo fileInfo = body.getFileInfo();
        ConnectInfo connectInfo = body.getConnectInfo();
        BrokerDesc brokerDesc = new BrokerDesc(connectInfo.getBrokerName(), connectInfo.getBrokerProps());

        List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
        try {
            // get file status
            BrokerUtil.parseFile(fileInfo.getFileUrl(), brokerDesc, fileStatuses);
            // create response
            FileReviewResponseVo reviewResponseVo = createFileReviewResponse(brokerDesc, fileInfo, fileStatuses);
            return ResponseEntityBuilder.ok(reviewResponseVo);
        } catch (UserException e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }

    private FileReviewResponseVo createFileReviewResponse(BrokerDesc brokerDesc, FileInfo fileInfo,
                                                          List<TBrokerFileStatus> fileStatuses) throws UserException {
        FileReviewResponseVo responseVo = new FileReviewResponseVo();
        // set file review statistic
        FileReviewStatistic statistic = new FileReviewStatistic();
        statistic.setFileNumber(fileStatuses.size());
        long totalFileSize = 0;
        for (TBrokerFileStatus fStatus : fileStatuses) {
            if (fStatus.isDir) {
                throw new UserException("Not all matched paths are files: " + fStatus.path);
            }
            totalFileSize += fStatus.size;
        }
        statistic.setFileSize(totalFileSize);
        responseVo.setReviewStatistic(statistic);

        if (fileStatuses.isEmpty()) {
            return responseVo;
        }

        // Begin to preview first file.
        // Currently we only support csv file format.
        if (!fileInfo.format.equals("CSV")) {
            return responseVo;
        }

        // read some content of sample file
        TBrokerFileStatus sampleFile = fileStatuses.get(0);
        byte[] fileContentBytes = BrokerUtil.readFile(sampleFile.path, brokerDesc, MAX_READ_LEN_BYTES);

        FileSample fileSample = new FileSample();
        fileSample.setSampleFileName(sampleFile.path);
        parseContent(fileInfo.columnSeparator,"\n", fileContentBytes, fileSample);

        responseVo.setFileSample(fileSample);
        return responseVo;
    }

    private void parseContent(String columnSeparator, String lineDelimiter, byte[] fileContentBytes,
                                            FileSample fileSample) {
        List<List<String>> sampleLines = Lists.newArrayList();
        int maxColSize = 0;
        String content = new String(fileContentBytes);
        String[] lines = content.split(lineDelimiter);
        for (String line : lines) {
            String[] cols = line.split(columnSeparator);
            List<String> row = Lists.newArrayList(cols);
            sampleLines.add(row);
            maxColSize = Math.max(maxColSize, row.size());
        }

        fileSample.setFileLineNumber(sampleLines.size());
        fileSample.setMaxColumnSize(maxColSize);
        fileSample.setSampleFileLines(sampleLines);
        return;
    }

    @Getter
    @Setter
    public static class FileReviewRequestVo {
        private FileInfo fileInfo;
        private ConnectInfo connectInfo;
    }

    @Getter
    @Setter
    public static class FileInfo {
        private String columnSeparator;
        private String fileUrl;
        private String format;
    }

    @Getter
    @Setter
    public static class ConnectInfo {
        private String brokerName;
        private Map<String, String> brokerProps;
    }

    @Getter
    @Setter
    public static class FileReviewResponseVo {
        private FileReviewStatistic reviewStatistic;
        private FileSample fileSample;
    }

    @Getter
    @Setter
    public static class FileReviewStatistic {
        private int fileNumber;
        private long fileSize;
    }

    @Getter
    @Setter
    public static class FileSample {
        private String sampleFileName;
        private int fileLineNumber;
        private int maxColumnSize;
        private List<List<String>> sampleFileLines;
    }


    public static void main(String[] args) {
        ImportAction importAction = new ImportAction();
        String str = "1,2,3\n4,5,6\n,7,8,9,中国";
        byte[] fileContentBytes = str.getBytes();
        System.out.println(fileContentBytes.length);
        String newStr = new String(fileContentBytes, 0,fileContentBytes.length - 2);
        System.out.println(newStr);

        FileSample fileSample = new FileSample();
        importAction.parseContent(",", "\n", newStr.getBytes(), fileSample);
        System.out.println(fileSample.sampleFileLines);
    }

}
