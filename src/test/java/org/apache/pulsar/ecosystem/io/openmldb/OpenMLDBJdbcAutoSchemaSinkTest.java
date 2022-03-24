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

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

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
        goodConfig.put("randomSeed", System.currentTimeMillis());
        goodConfig.put("maxMessageSize", 1024);
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
//            connector.write();
            fail("Should fail to read records if a connector is not open");
        } catch (IllegalStateException ise) {
            // expected
        }
    }

}
