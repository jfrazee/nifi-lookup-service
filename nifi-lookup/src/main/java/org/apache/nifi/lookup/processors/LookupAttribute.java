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

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.lookup.services.LookupTableService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@EventDriven
@SideEffectFree
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

    public static final PropertyDescriptor INCLUDE_NULL_VALUES = new PropertyDescriptor.Builder()
        .name("Include Null Values")
        .description("Include null values for keys that aren't present int he lookup table")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .description("All FlowFiles are routed to this relationship")
            .name("success")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

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
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final LookupTableService lookupTableService = context.getProperty(LOOKUP_TABLE_SERVICE).asControllerService(LookupTableService.class);
        final Boolean includeNullValues = context.getProperty(INCLUDE_NULL_VALUES).asBoolean();

        // Load up all the dynamic properties so we can see if there are any
        final Map<PropertyDescriptor, PropertyValue> dynamicProperties = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> e : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = e.getKey();
            if (descriptor.isDynamic()) {
                final PropertyValue value = context.getProperty(descriptor);
                dynamicProperties.put(descriptor, value);
            }
        }

        final Map<String, String> attributes = new HashMap<>(flowFile.getAttributes());
        if (dynamicProperties.isEmpty()) {
            // If there aren't any dynamic properties, load the entire lookup table
            final Map<String, String> lookupTable = lookupTableService.getAll();
            if (lookupTable.isEmpty()) {
                getLogger().warn("No dynamic properties provided and lookup table is empty");
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
                final String attributeValue = lookupTableService.get(lookupKey);
                if (attributeValue != null) {
                    attributes.put(attributeName, attributeValue);
                } else if (includeNullValues) {
                    attributes.put(attributeName, "null");
                } else {
                    getLogger().warn("No such value for key: {}", new Object[]{lookupKey});
                }
            }
        }

        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(flowFile, REL_SUCCESS);
    }
}
