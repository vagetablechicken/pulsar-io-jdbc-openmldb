/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.ecosystem.io.openmldb;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.io.core.SinkContext;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link OpenMLDBJdbcAutoSchemaSink}.
 */
public class OpenMLDBJdbcAutoSchemaSinkTest {

    private final Map<String, Object> goodConfig = new HashMap<>();
    private final Map<String, Object> badConfig = new HashMap<>();

    @Before
    public void setup() {
        // use client to create db & table
        String zk = "localhost:6181", zkPath = "/onebox";
        String jdbcUrl = String.format("jdbc:openmldb:///?zk=%s&zkPath=%s", zk, zkPath);

        String dbName = "pulsar_test";
        String tableName = "connector_test";
        Connection connection = null;
        try {
            Class.forName("com._4paradigm.openmldb.jdbc.SQLDriver");
            connection = DriverManager.getConnection(jdbcUrl);
            Statement stmt = connection.createStatement();
            stmt.execute("create database if not exists " + dbName);
            stmt.execute(String.format("use %s", dbName));
            stmt.execute(String.format("create table if not exists %s(c1 int, c2 string)", tableName));
        } catch (SQLException | ClassNotFoundException e) {
            // TODO: 'create table if not exists' is not supported now
            e.printStackTrace();
        }
        assertNotNull(connection);
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        goodConfig.put("jdbcUrl", jdbcUrl);
        goodConfig.put("tableName", "test");
    }

    /**
     * Test opening the connector with good configuration.
     *
     * @throws Exception when fail to open the connector
     */
    @Test
    public void testOpenConnectorWithGoodConfig() throws Exception {
        OpenMLDBJdbcAutoSchemaSink connector = new OpenMLDBJdbcAutoSchemaSink();
        connector.open(goodConfig, mock(SinkContext.class));

        // goodConfig won't throw exception
    }

    /**
     * Test opening the connector with good configuration.
     *
     * @throws Exception when fail to open the connector
     */
    @Test
    public void testOpenConnectorWithBadConfig() throws Exception {
        OpenMLDBJdbcAutoSchemaSink connector = new OpenMLDBJdbcAutoSchemaSink();
        try {
            connector.open(badConfig, mock(SinkContext.class));
            fail("Should failed to open the connector when using an invalid configuration");
        } catch (NullPointerException npe) {
            // expected
        }

    }

    /**
     * Test opening the connector twice.
     *
     * @throws Exception when fail to open the connector
     */
    @Test
    public void testOpenConnectorTwice() throws Exception {
        OpenMLDBJdbcAutoSchemaSink connector = new OpenMLDBJdbcAutoSchemaSink();
        connector.open(goodConfig, mock(SinkContext.class));

        Map<String, Object> anotherConfig = new HashMap<>(goodConfig);
        // change the maxMessageSize
        anotherConfig.put("maxMessageSize", 2048);
        try {
            connector.open(anotherConfig, mock(SinkContext.class));
            fail("Should fail to open a connector multiple times");
        } catch (IllegalStateException ise) {
            // expected
        }
    }

    /**
     * Test writing before opening.
     *
     * @throws Exception when fail to open the connector
     */
    @Test
    public void testWriteRecordsBeforeOpeningConnector() throws Exception {
        OpenMLDBJdbcAutoSchemaSink connector = new OpenMLDBJdbcAutoSchemaSink();
        try {
            connector.write(() -> null);
            fail("Should fail to write records if a connector is not open");
        } catch (NullPointerException npe) {
            // expected
            // incomingList in sink is null
        }
    }

}
