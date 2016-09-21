/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.lookup.services;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

import org.apache.nifi.lookup.processors.TestProcessor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;

public class TestPropertiesFileLookupTableService {

    @Before
    public void init() {

    }

    @Test
    public void testServiceWithoutReload() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final PropertiesFileLookupTableService service = new PropertiesFileLookupTableService();
        final Map<String, String> properties = new HashMap<>();
        properties.put(PropertiesFileLookupTableService.PROPERTIES_FILE.getName(), "src/test/resources/test.properties");

        runner.addControllerService("PropertiesFileLookupTableService", service, properties);
        runner.enableControllerService(service);
        runner.assertValid(service);

        final String myProperty1 = service.get("myproperty.1");
        assertEquals("this is property 1", myProperty1);

        final String myProperty2 = service.get("myproperty.2");
        assertEquals("2", myProperty2);

        final Map<String, String> actualProperties = service.getAll();
        final Map<String, String> expectedProperties = new HashMap<>();
        expectedProperties.put("myproperty.1", "this is property 1");
        expectedProperties.put("myproperty.2", "2");
        assertEquals(expectedProperties, actualProperties);
    }

    @Test
    public void testServiceWithReload() throws InitializationException, IOException, InterruptedException {
        final Path testPath = Files.createTempFile("test", "properties");
        try {
            Files.copy(new File("src/test/resources/test.properties").toPath(), testPath, StandardCopyOption.REPLACE_EXISTING);

            final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
            final PropertiesFileLookupTableService service = new PropertiesFileLookupTableService();
            runner.addControllerService("PropertiesFileLookupTableService", service);
            runner.setProperty(service, PropertiesFileLookupTableService.PROPERTIES_FILE.getName(), testPath.toString());
            runner.enableControllerService(service);
            runner.assertValid(service);

            final String myProperty11 = service.get("myproperty.1");
            assertEquals("this is property 1", myProperty11);

            final String myProperty21 = service.get("myproperty.2");
            assertEquals("2", myProperty21);

            final String myProperty31 = service.get("myproperty.3");
            assertNull(myProperty31);

            final Map<String, String> actualProperties1 = service.getAll();
            final Map<String, String> expectedProperties1 = new HashMap<>();
            expectedProperties1.put("myproperty.1", "this is property 1");
            expectedProperties1.put("myproperty.2", "2");
            assertEquals(expectedProperties1, actualProperties1);

            Thread.sleep(5000);
            Files.write(testPath, Arrays.asList("myproperty.1=this is property 1 modified", "myproperty.3=this is property 3"), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);

            final String myProperty12 = service.get("myproperty.1");
            assertEquals("this is property 1 modified", myProperty12);

            final String myProperty22 = service.get("myproperty.2");
            assertNull(myProperty22);

            final String myProperty32 = service.get("myproperty.3");
            assertEquals("this is property 3", myProperty32);

            final Map<String, String> actualProperties2 = service.getAll();
            final Map<String, String> expectedProperties2 = new HashMap<>();
            expectedProperties2.put("myproperty.1", "this is property 1 modified");
            expectedProperties2.put("myproperty.3", "this is property 3");
            assertEquals(expectedProperties2, actualProperties2);
        } finally {
            Files.deleteIfExists(testPath);
        }
    }

}
