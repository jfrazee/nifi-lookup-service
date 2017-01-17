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
package org.apache.nifi.lookup.processors;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import org.apache.nifi.lookup.services.PropertiesFileLookupTableService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestLookupAttribute {
    @Test
    public void testLookupAttributeWithInMemoryLookup() {

    }

    @Test
    public void testLookupAttributeWithPropertiesFileLookup() throws InitializationException {
        final PropertiesFileLookupTableService service = new PropertiesFileLookupTableService();
        final TestRunner runner = TestRunners.newTestRunner(new LookupAttribute());
        runner.addControllerService("PropertiesFileLookupTableService", service);
        runner.setProperty(service, PropertiesFileLookupTableService.PROPERTIES_FILE.getName(), "src/test/resources/test.properties");
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.setProperty(LookupAttribute.LOOKUP_TABLE_SERVICE, "PropertiesFileLookupTableService");
        runner.enqueue("some content".getBytes());
        runner.run(1, false);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(LookupAttribute.REL_SUCCESS).get(0);
        assertNotNull(flowFile);
        flowFile.assertAttributeExists("myproperty.1");
        flowFile.assertAttributeExists("myproperty.2");
        flowFile.assertAttributeEquals("myproperty.1", "this is property 1");
        flowFile.assertAttributeEquals("myproperty.2", "2");
    }

    @Test
    public void testLookupAttributeWithPropertiesFileLookupAndDynamicProperties() throws InitializationException {
        final PropertiesFileLookupTableService service = new PropertiesFileLookupTableService();
        final TestRunner runner = TestRunners.newTestRunner(new LookupAttribute());
        runner.addControllerService("PropertiesFileLookupTableService", service);
        runner.setProperty(service, PropertiesFileLookupTableService.PROPERTIES_FILE.getName(), "src/test/resources/test.properties");
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.setProperty(LookupAttribute.LOOKUP_TABLE_SERVICE, "PropertiesFileLookupTableService");
        runner.setProperty("myproperty", "myproperty.1");
        runner.setProperty("doesntexist", "myproperty.3");
        runner.enqueue("some content".getBytes());
        runner.run(1, false);

        runner.assertQueueEmpty();

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(LookupAttribute.REL_SUCCESS).get(0);
        assertNotNull(flowFile);
        flowFile.assertAttributeExists("myproperty");
        flowFile.assertAttributeExists("doesntexist");
        flowFile.assertAttributeEquals("myproperty", "this is property 1");
        flowFile.assertAttributeEquals("doesntexist", "null");
        flowFile.assertAttributeNotExists("myproperty.2");
    }

    @Ignore
    @Test
    public void testLookupAttributeWithPropertiesFileLookupAndReload() throws InitializationException, IOException, InterruptedException {
        // final Path testPath = Files.createTempFile("test", "properties");
        final Path testPath = new File("/tmp/test.properties").toPath();
        try {
            Files.copy(new File("src/test/resources/test.properties").toPath(), testPath, StandardCopyOption.REPLACE_EXISTING);

            final PropertiesFileLookupTableService service = new PropertiesFileLookupTableService();
            final TestRunner runner = TestRunners.newTestRunner(new LookupAttribute());
            runner.addControllerService("PropertiesFileLookupTableService", service);
            runner.setProperty(service, PropertiesFileLookupTableService.PROPERTIES_FILE.getName(), testPath.toString());
            runner.enableControllerService(service);
            runner.assertValid(service);
            runner.setProperty(LookupAttribute.LOOKUP_TABLE_SERVICE, "PropertiesFileLookupTableService");
            runner.enqueue("some content".getBytes());
            runner.run(1, false);

            runner.assertQueueEmpty();

            MockFlowFile flowFile = runner.getFlowFilesForRelationship(LookupAttribute.REL_SUCCESS).get(0);
            assertNotNull(flowFile);
            flowFile.assertAttributeExists("myproperty.1");
            flowFile.assertAttributeExists("myproperty.2");
            flowFile.assertAttributeEquals("myproperty.1", "this is property 1");
            flowFile.assertAttributeEquals("myproperty.2", "2");

            Thread.sleep(5000);
            Files.write(testPath, Arrays.asList("myproperty.1=this is property 1 modified", "myproperty.3=this is property 3"), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);

            runner.enqueue("more content".getBytes());
            runner.run(10, false, false);

            runner.assertQueueEmpty();

            flowFile = runner.getFlowFilesForRelationship(LookupAttribute.REL_SUCCESS).get(0);
            assertNotNull(flowFile);
            flowFile.assertAttributeExists("myproperty.1");
            // flowFile.assertAttributeExists("myproperty.3");
            flowFile.assertAttributeEquals("myproperty.1", "this is property 1 modified");
            // flowFile.assertAttributeEquals("myproperty.3", "this is property 3");
            // flowFile.assertAttributeNotExists("myproperty.2");
        } finally {
            //Files.deleteIfExists(testPath);
        }
    }
}
