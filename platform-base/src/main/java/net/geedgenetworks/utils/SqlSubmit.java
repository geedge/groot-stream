/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.geedgenetworks.utils;

import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SqlSubmit {

    private static Logger LOG = LoggerFactory.getLogger(SqlSubmit.class);

    private List<String> sqllist;

    private StatementSet stmtSet;

    private StreamTableEnvironment tEnv = FlinkEnvironmentUtils.tableEnvironment;

    public SqlSubmit(List<String> sqllist) {
        this.sqllist=sqllist;
    }

    public void run() throws Exception {
        stmtSet = tEnv.createStatementSet();
        List<SqlCommandParserUtils.SqlCommandCall> scc = SqlCommandParserUtils.parse(sqllist);

        for(SqlCommandParserUtils.SqlCommandCall sqlCommandCall:scc){
            callCommand(sqlCommandCall);
        }
//        stmtSet.attachAsDataStream();
    }

    private void callCommand(SqlCommandParserUtils.SqlCommandCall cmdCall) {

        LOG.info("sql {}", cmdCall.operands[0]);

        switch (cmdCall.command) {
            case SET:
                callSet(cmdCall);
                break;
            case CREATE_TABLE:
                callCreateTable(cmdCall);
                break;
            case CREATE_VIEW:
                callCreateTable(cmdCall);
                break;
            case CREATE_TEMPORARY_VIEW:
                callCreateTable(cmdCall);
                break;
            case INSERT_INTO:
                callInsertInto(cmdCall);
                break;
            default:
                LOG.error("Unsupported command {} .", "error",cmdCall.command);
                throw new RuntimeException("Unsupported command: " + cmdCall.command);
        }
    }

    private void callSet(SqlCommandParserUtils.SqlCommandCall cmdCall) {
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];
        tEnv.getConfig().getConfiguration().setString(key, value);
    }

    private void callCreateTable(SqlCommandParserUtils.SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];
        try {
            tEnv.executeSql(ddl);
        } catch (SqlParserException e) {
            LOG.error("SQL parse failed {} .", "error", e);
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
    }

    private void callInsertInto(SqlCommandParserUtils.SqlCommandCall cmdCall) {
        String dml = cmdCall.operands[0];
        try {
            stmtSet.addInsertSql(dml);
        } catch (SqlParserException e) {
            LOG.error("SQL parse failed {} .", "error", e);
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
        }
    }
}
