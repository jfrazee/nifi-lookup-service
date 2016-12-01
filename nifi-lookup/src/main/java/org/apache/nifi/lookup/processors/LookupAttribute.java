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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.lookup.services.LookupTableService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.cache.LoadingCache;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"attributes", "lookup", "properties", "cache", "Attribute Expression Language"})
@CapabilityDescription("Lookup attributes from a lookup table")
@DynamicProperty(name = "The name of the attribute to add to the FlowFile", value = "The name of the key or property to lookup from the lookup table", supportsExpressionLanguage = true,
        description = "Adds a FlowFile attribute specified by the Dynamic Property's key with the value found in the lookup table using the the Dynamic Property's value")
@WritesAttribute(attribute = "See additional details", description = "This processor may write zero or more attributes as described in additional details")
public class LookupAttribute extends AbstractProcessor {

    public static final PropertyDescriptor LOOKUP_TABLE_SERVICE =
        new PropertyDescriptor.Builder()
            .name("lookup-table-service")
            .displayName("Lookup Service")
            .description("The lookup service to use for attribute lookups")
            .identifiesControllerService(LookupTableService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor INCLUDE_NULL_VALUES =
        new PropertyDescriptor.Builder()
            .name("Include Null Values")
            .description("Include null values for keys that aren't present int he lookup table")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor CACHE_SIZE =
        new PropertyDescriptor.Builder()
            .name("cache-size")
            .displayName("Cache size")
            .description("Maximum number of keys to cache. Zero disables the cache.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE_TTL_AFTER_WRITE =
        new PropertyDescriptor.Builder()
            .name("cache-ttl-after-write")
            .displayName("Cache TTL after write")
            .description("The cache TTL (time-to-live) or how long to keep keys in the cache after they're added.")
            .required(true)
            .defaultValue("60 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .description("FlowFiles with matching lookups are routed to this relationship")
            .name("success")
            .build();

    public static final Relationship REL_UNMATCHED = new Relationship.Builder()
            .description("FlowFiles with missing lookups are routed to this relationship")
            .name("unmatched")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private Map<PropertyDescriptor, PropertyValue> dynamicProperties;

    private LoadingCache<String, String> cache;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .required(false)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .expressionLanguageSupported(true)
            .dynamic(true)
            .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(LOOKUP_TABLE_SERVICE);
        descriptors.add(INCLUDE_NULL_VALUES);
        descriptors.add(CACHE_SIZE);
        descriptors.add(CACHE_TTL_AFTER_WRITE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_UNMATCHED);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        // Load up all the dynamic properties once for use later in onTrigger
        final Map<PropertyDescriptor, PropertyValue> properties = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> e : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = e.getKey();
            if (descriptor.isDynamic()) {
                final PropertyValue value = context.getProperty(descriptor);
                properties.put(descriptor, value);
            }
        }
        dynamicProperties = Collections.unmodifiableMap(properties);

        // Setup the loading cache if the cache size is > 0
        final LookupTableService lookupTableService = context.getProperty(LOOKUP_TABLE_SERVICE).asControllerService(LookupTableService.class);
        final Integer cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        final Long cacheTTL = context.getProperty(CACHE_TTL_AFTER_WRITE).asTimePeriod(TimeUnit.SECONDS);
        if (cacheSize > 0) {
            CacheBuilder cacheBuilder = CacheBuilder.newBuilder().maximumSize(cacheSize);
            if (cacheTTL > 0) {
                cacheBuilder = cacheBuilder.expireAfterWrite(cacheTTL, TimeUnit.SECONDS);
            }

            cache = cacheBuilder.build(
               new CacheLoader<String, String>() {
                   public String load(String key) {
                       return lookupTableService.get(key);
                   }
               });
        } else {
            cache = null;
            getLogger().warn("Cache disabled because cache size is set to 0");
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final LookupTableService lookupTableService = context.getProperty(LOOKUP_TABLE_SERVICE).asControllerService(LookupTableService.class);
        final Boolean includeNullValues = context.getProperty(INCLUDE_NULL_VALUES).asBoolean();
        final Map<String, String> attributes = new HashMap<>(flowFile.getAttributes());

        boolean notMatched = false;
        if (dynamicProperties.isEmpty()) {
            // If there aren't any dynamic properties, load the entire lookup table
            final Map<String, String> lookupTable = lookupTableService.getAll();
            if (lookupTable.isEmpty()) {
                logger.warn("No dynamic properties provided and lookup table is empty");
            }
            for (final Map.Entry<String, String> e : lookupTable.entrySet()) {
                final String attributeName = e.getKey();
                final String attributeValue = e.getValue();
                attributes.putIfAbsent(attributeName, attributeValue);
            }
        } else {
            // Otherwise, load the keys corresponding to the property values
            for (final Map.Entry<PropertyDescriptor, PropertyValue> e : dynamicProperties.entrySet()) {
                final PropertyValue lookupKeyExpression = e.getValue();
                final String lookupKey = lookupKeyExpression.evaluateAttributeExpressions(flowFile).getValue();
                final String attributeName = e.getKey().getName();
                final String attributeValue = get(lookupTableService, lookupKey);
                if (attributeValue != null) {
                    attributes.put(attributeName, attributeValue);
                } else if (includeNullValues) {
                    attributes.put(attributeName, "null");
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("No such value for key: {}", new Object[]{lookupKey});
                    }
                    notMatched = true;
                }
            }
        }

        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(flowFile, notMatched ? REL_UNMATCHED : REL_SUCCESS);
    }

    private String get(final LookupTableService lookupTableService, final String key) {
        final ComponentLog logger = getLogger();
        if (cache != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Looking up value from cache");
            }
            try {
                return cache.get(key);
            } catch (final ExecutionException | InvalidCacheLoadException e) {
                logger.warn(e.getMessage(), e);
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Looking up value from service because cache is disabled");
            }
            return lookupTableService.get(key);
        }
        return null;
    }

}
