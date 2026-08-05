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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.table.catalog.SensitiveConnection;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Describes a {@link SensitiveConnection} representing a connection.
 *
 * <p>A {@link ConnectionDescriptor} is a template for creating a {@link SensitiveConnection}
 * instance. It closely resembles the "CREATE CONNECTION" SQL DDL statement, containing the
 * connection options and other characteristics.
 *
 * <p>This can be used to register a Connection in the Table API.
 *
 * <p>Connection options may contain plain secrets, which are extracted into a secret store when the
 * connection is registered.
 */
@PublicEvolving
public class ConnectionDescriptor {

    private static final String MASKED_VALUE = "****";

    private final Map<String, String> connectionOptions;
    private final @Nullable String comment;

    protected ConnectionDescriptor(
            Map<String, String> connectionOptions, @Nullable String comment) {
        this.connectionOptions = connectionOptions;
        this.comment = comment;
    }

    /** Converts this descriptor into a {@link SensitiveConnection}. */
    public SensitiveConnection toSensitiveConnection() {
        return SensitiveConnection.of(getOptions(), comment);
    }

    /** Converts this immutable instance into a mutable {@link Builder}. */
    public Builder toBuilder() {
        return new Builder(this);
    }

    // ---------------------------------------------------------------------------------------------

    /** Returns a map of string-based connection options. */
    Map<String, String> getOptions() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(connectionOptions));
    }

    /** Get comment of the connection. */
    Optional<String> getComment() {
        return Optional.ofNullable(comment);
    }

    // ---------------------------------------------------------------------------------------------

    /**
     * Creates a new {@link Builder} for the connection with the given type option.
     *
     * @param type string value of type for the connection.
     */
    public static Builder forType(String type) {
        Preconditions.checkNotNull(type, "Connection descriptors require a type value.");
        final Builder descriptorBuilder = new Builder();
        descriptorBuilder.option(FactoryUtil.CONNECTION_TYPE, type);
        return descriptorBuilder;
    }

    /** Option values are masked because connection options may contain plain secrets. */
    @Override
    public String toString() {
        final String serializedOptions =
                connectionOptions.keySet().stream()
                        .map(
                                key ->
                                        String.format(
                                                "  '%s' = '%s'",
                                                EncodingUtils.escapeSingleQuotes(key),
                                                MASKED_VALUE))
                        .collect(Collectors.joining(String.format(",%n")));

        return String.format(
                "COMMENT '%s'%nWITH (%n%s%n)", comment != null ? comment : "", serializedOptions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConnectionDescriptor that = (ConnectionDescriptor) o;
        return connectionOptions.equals(that.connectionOptions)
                && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectionOptions, comment);
    }

    // ---------------------------------------------------------------------------------------------

    /** Builder for {@link ConnectionDescriptor}. */
    @PublicEvolving
    public static class Builder {
        private final Map<String, String> connectionOptions;
        private @Nullable String comment;

        private Builder() {
            this.connectionOptions = new LinkedHashMap<>();
        }

        private Builder(ConnectionDescriptor descriptor) {
            this.connectionOptions = new LinkedHashMap<>(descriptor.getOptions());
            this.comment = descriptor.getComment().orElse(null);
        }

        /** Sets the given option on the connection. */
        public <T> Builder option(ConfigOption<T> configOption, T value) {
            Preconditions.checkNotNull(configOption, "Config option must not be null.");
            Preconditions.checkNotNull(value, "Value must not be null.");
            connectionOptions.put(
                    configOption.key(), ConfigurationUtils.convertValue(value, String.class));
            return this;
        }

        /**
         * Sets the given option on the connection.
         *
         * <p>Option keys must be fully specified.
         *
         * <p>Example:
         *
         * <pre>{@code
         * ConnectionDescriptor.forType("basic")
         *   .option("endpoint", "https://example.com")
         *   .option("password", "my-password")
         *   .build();
         * }</pre>
         */
        public Builder option(String key, String value) {
            Preconditions.checkNotNull(key, "Key must not be null.");
            Preconditions.checkNotNull(value, "Value must not be null.");
            this.connectionOptions.put(key, value);
            return this;
        }

        /** Define the comment for this connection. */
        public Builder comment(@Nullable String comment) {
            this.comment = comment;
            return this;
        }

        /** Returns an immutable instance of {@link ConnectionDescriptor}. */
        public ConnectionDescriptor build() {
            return new ConnectionDescriptor(connectionOptions, comment);
        }
    }
}
