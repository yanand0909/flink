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

package org.apache.flink.table.api;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.SensitiveConnection;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ConnectionDescriptor}. */
class ConnectionDescriptorTest {

    private static final ConfigOption<Boolean> OPTION_A =
            ConfigOptions.key("a").booleanType().noDefaultValue();

    private static final ConfigOption<Integer> OPTION_B =
            ConfigOptions.key("b").intType().noDefaultValue();

    private static final ConfigOption<String> ENDPOINT_OPTION =
            ConfigOptions.key("endpoint").stringType().noDefaultValue();

    @Test
    void testBasic() {
        final ConnectionDescriptor descriptor =
                ConnectionDescriptor.forType("basic")
                        .option("endpoint", "https://example.com")
                        .comment("Test Connection Comment")
                        .build();

        assertThat(descriptor.getOptions()).hasSize(2);
        assertThat(descriptor.getOptions()).containsEntry("type", "basic");
        assertThat(descriptor.getOptions()).containsEntry("endpoint", "https://example.com");

        assertThat(descriptor.getComment().orElse(null)).isEqualTo("Test Connection Comment");
    }

    @Test
    void testOptions() {
        final ConnectionDescriptor descriptor =
                ConnectionDescriptor.forType("basic")
                        .option(OPTION_A, false)
                        .option(OPTION_B, 42)
                        .option("endpoint", "https://example.com")
                        .build();

        assertThat(descriptor.getOptions()).hasSize(4);
        assertThat(descriptor.getOptions()).containsEntry("type", "basic");
        assertThat(descriptor.getOptions()).containsEntry("a", "false");
        assertThat(descriptor.getOptions()).containsEntry("b", "42");
        assertThat(descriptor.getOptions()).containsEntry("endpoint", "https://example.com");
    }

    @Test
    void testToSensitiveConnection() {
        final ConnectionDescriptor descriptor =
                ConnectionDescriptor.forType("basic")
                        .option("endpoint", "https://example.com")
                        .option("password", "s3cr3t")
                        .comment("Test Connection")
                        .build();

        final SensitiveConnection connection = descriptor.toSensitiveConnection();

        assertThat(connection.getOptions())
                .containsEntry("type", "basic")
                .containsEntry("endpoint", "https://example.com")
                .containsEntry("password", "s3cr3t");
        assertThat(connection.getComment()).isEqualTo("Test Connection");
        assertThat(connection)
                .isEqualTo(
                        SensitiveConnection.of(
                                Map.of(
                                        "type",
                                        "basic",
                                        "endpoint",
                                        "https://example.com",
                                        "password",
                                        "s3cr3t"),
                                "Test Connection"));

        assertThatThrownBy(() -> connection.getOptions().put("other", "value"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testGetOptionsIsImmutable() {
        final ConnectionDescriptor descriptor = ConnectionDescriptor.forType("basic").build();

        assertThatThrownBy(() -> descriptor.getOptions().put("endpoint", "https://example.com"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testToStringMasksAllOptionValues() {
        final ConnectionDescriptor descriptor =
                ConnectionDescriptor.forType("basic")
                        .option("bootstrap.servers", "localhost:9092")
                        .option("password", "s3cr3t")
                        .comment("Test Connection Comment")
                        .build();

        assertThat(descriptor.toString())
                .isEqualTo(
                        String.format(
                                "COMMENT 'Test Connection Comment'%n"
                                        + "WITH (%n"
                                        + "  'type' = '****',%n"
                                        + "  'bootstrap.servers' = '****',%n"
                                        + "  'password' = '****'%n"
                                        + ")"))
                .doesNotContain("s3cr3t", "localhost:9092");
    }

    @Test
    void testToBuilder() {
        final ConnectionDescriptor original =
                ConnectionDescriptor.forType("basic")
                        .option("endpoint", "https://example.com")
                        .comment("Original Comment")
                        .build();

        final ConnectionDescriptor modified =
                original.toBuilder()
                        .option("new-option", "new-value")
                        .comment("Modified Comment")
                        .build();

        assertThat(original.getComment().orElse(null)).isEqualTo("Original Comment");
        assertThat(original.getOptions()).doesNotContainKey("new-option");
        assertThat(modified.getComment().orElse(null)).isEqualTo("Modified Comment");
        assertThat(modified.getOptions()).containsEntry("new-option", "new-value");
        assertThat(modified.getOptions()).containsEntry("endpoint", "https://example.com");
        assertThat(modified.getOptions()).containsEntry("type", "basic");
    }

    @Test
    void testEqualsAndHashCode() {
        final ConnectionDescriptor descriptor1 =
                ConnectionDescriptor.forType("basic")
                        .option("endpoint", "https://example.com")
                        .comment("Test Comment")
                        .build();

        final ConnectionDescriptor descriptor2 =
                ConnectionDescriptor.forType("basic")
                        .option("endpoint", "https://example.com")
                        .comment("Test Comment")
                        .build();

        final ConnectionDescriptor descriptor3 =
                ConnectionDescriptor.forType("bearer")
                        .option("endpoint", "https://example.com")
                        .comment("Test Comment")
                        .build();

        assertThat(descriptor1).isEqualTo(descriptor2);
        assertThat(descriptor1).isNotEqualTo(descriptor3);
        assertThat(descriptor1).isNotEqualTo(null);
        assertThat(descriptor1).isNotEqualTo("not a connection descriptor");

        assertThat(descriptor1.hashCode()).isEqualTo(descriptor2.hashCode());
    }

    @Test
    void testBuilderExceptions() {
        assertThatThrownBy(() -> ConnectionDescriptor.forType(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Connection descriptors require a type value.");

        final ConnectionDescriptor.Builder builder = ConnectionDescriptor.forType("basic");

        assertThatThrownBy(() -> builder.option((ConfigOption<String>) null, "value"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Config option must not be null.");

        assertThatThrownBy(() -> builder.option(ENDPOINT_OPTION, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Value must not be null.");

        assertThatThrownBy(() -> builder.option((String) null, "value"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Key must not be null.");

        assertThatThrownBy(() -> builder.option("key", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Value must not be null.");
    }

    @Test
    void testNullComment() {
        final ConnectionDescriptor descriptor =
                ConnectionDescriptor.forType("basic").comment(null).build();

        assertThat(descriptor.getComment()).isNotPresent();
    }
}
