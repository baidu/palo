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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.journal.bdbje.BDBDebugger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.sleepycat.je.DatabaseConfig;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

// SHOW PROC "/bdbje"
public class BDBJEProcDir implements ProcDirInterface  {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("DbNames").add("JournalNumber").add("Comment").build();

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String dbName) throws AnalysisException {
        return new BDBJEDatabaseProcDir(dbName);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        TreeMap<String, Long> journalNumMap = getDbNamesWithJournalNumber();
        for (Map.Entry<String, Long> entry : journalNumMap.entrySet()) {
            result.addRow(Lists.newArrayList(entry.getKey(), entry.getValue().toString(), ""));
        }
        return result;
    }

    private TreeMap<String, Long> getDbNamesWithJournalNumber() {
        TreeMap<String, Long> journalNumMap = new TreeMap<>();
        if (Config.enable_bdbje_debug_mode) {
            BDBDebugger.BDBDebugEnv debugEnv = BDBDebugger.get().getEnv();
            List<String> dbNames = debugEnv.listDbNames();
            for (String dbName : dbNames) {
                journalNumMap.put(dbName, debugEnv.getJournalNumber(dbName));
            }
        } else {
            List<String> dbNames = Catalog.getCurrentCatalog().getEditLog().getDatabaseNames(false);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setReadOnly(true);
            dbConfig.setAllowCreate(false);
            for (String dbName : dbNames) {
                journalNumMap.put(dbName, Catalog.getCurrentCatalog().getEditLog().getCountOfDatabase(dbName, dbConfig));
            }
        }
        return journalNumMap;
    }
}
