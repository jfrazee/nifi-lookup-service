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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

@Tags({"lookup", "csv", "cache"})
@CapabilityDescription("A CSV file-based lookup table")
public class CsvFileLookupTableService extends AbstractControllerService implements LookupTableService {

    public static final PropertyDescriptor CSV_FILE =
        new PropertyDescriptor.Builder()
            .name("csv-file")
            .displayName("CSV File")
            .description("A character-delimited file (e.g., CSV)")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    private List<PropertyDescriptor> properties;

    private Map<String, String> lookupTable;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected void init(final ControllerServiceInitializationContext context) throws InitializationException {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CSV_FILE);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException, IOException, FileNotFoundException {
        final Map<String, String> lookupTable = new ConcurrentHashMap<>();
        final String path = context.getProperty(CSV_FILE).getValue();
        final FileReader reader = new FileReader(path);
        final Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(reader);
        for (CSVRecord record : records) {
            final String prefix = record.get(0);
            for (int i = 1; i < record.size(); i++) {
                final String key = prefix + "." + i;
                final String value = record.get(i);
                lookupTable.put(key, value);
            }
        }
        this.lookupTable = lookupTable;
    }

    @OnDisabled
    public void onDisabled() {
        this.lookupTable = null;
    }

    @Override
    public String get(String key) {
        if (lookupTable != null) {
            return lookupTable.get(key);
        }
        return null;
    }

    @Override
    public String put(String key, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String putIfAbsent(String key, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> getAll() {
        return lookupTable;
    }

    @Override
    public Map<String, String> putAll(Map<String, String> values) {
        throw new UnsupportedOperationException();
    }

}
