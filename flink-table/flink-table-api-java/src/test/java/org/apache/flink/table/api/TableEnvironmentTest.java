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

import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ContextResolvedModel;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.ModelNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.operations.SourceQueryOperation;
import org.apache.flink.table.utils.TableEnvironmentMock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

/** Tests for {@link TableEnvironment}. */
class TableEnvironmentTest {
    private static final Schema TEST_SCHEMA =
            Schema.newBuilder().column("f0", DataTypes.INT()).build();
    private static final TableDescriptor TEST_DESCRIPTOR =
            TableDescriptor.forConnector("fake").schema(TEST_SCHEMA).option("a", "Test").build();
    private static final ModelDescriptor TEST_MODEL_DESCRIPTOR =
            ModelDescriptor.forProvider("TestProvider")
                    .option("a", "Test")
                    .inputSchema(TEST_SCHEMA)
                    .outputSchema(TEST_SCHEMA)
                    .build();

    @Test
    void testCreateTemporaryTableFromDescriptor() {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();

        assertTemporaryCreateTableFromDescriptor(tEnv, TEST_SCHEMA, false);
    }

    @Test
    void testCreateTemporaryTableIfNotExistsFromDescriptor() {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();

        assertTemporaryCreateTableFromDescriptor(tEnv, TEST_SCHEMA, true);
        assertThatNoException()
                .isThrownBy(() -> tEnv.createTemporaryTable("T", TEST_DESCRIPTOR, true));

        assertThatThrownBy(() -> tEnv.createTemporaryTable("T", TEST_DESCRIPTOR, false))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Temporary table '`default_catalog`.`default_database`.`T`' already exists");
    }

    @Test
    void testCreateTableFromDescriptor() throws Exception {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();

        assertCreateTableFromDescriptor(tEnv, TEST_SCHEMA, false);
    }

    @ParameterizedTest(name = "{index}: ignoreIfExists ({0})")
    @ValueSource(booleans = {true, false})
    void testCreateViewFromTable(final boolean ignoreIfExists) throws Exception {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();

        final String catalog = tEnv.getCurrentCatalog();
        final String database = tEnv.getCurrentDatabase();

        tEnv.createTable("T", TEST_DESCRIPTOR);

        assertThat(tEnv.createView("V", tEnv.from("T"), ignoreIfExists)).isTrue();

        final ObjectPath objectPath = new ObjectPath(database, "V");

        final CatalogBaseTable catalogView =
                tEnv.getCatalog(catalog).orElseThrow(AssertionError::new).getTable(objectPath);
        assertThat(catalogView).isInstanceOf(CatalogView.class);
        assertThat(catalogView.getUnresolvedSchema()).isEqualTo(TEST_SCHEMA);
    }

    @Test
    void testCreateViewWithSameNameIgnoreIfExists() {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();

        tEnv.createTable("T", TEST_DESCRIPTOR);
        tEnv.createView("V", tEnv.from("T"));

        assertThat(tEnv.createView("V", tEnv.from("T"), true)).isFalse();
    }

    @Test
    void testCreateViewWithSameName() {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();

        tEnv.createTable("T", TEST_DESCRIPTOR);
        tEnv.createView("V", tEnv.from("T"));

        assertThatThrownBy(() -> tEnv.createView("V", tEnv.from("T"), false))
                .hasCauseInstanceOf(TableAlreadyExistException.class)
                .hasMessageContaining(
                        "Could not execute CreateTable in path `default_catalog`.`default_database`.`V`");

        assertThatThrownBy(() -> tEnv.createView("V", tEnv.from("T")))
                .hasCauseInstanceOf(TableAlreadyExistException.class)
                .hasMessageContaining(
                        "Could not execute CreateTable in path `default_catalog`.`default_database`.`V`");
    }

    @Test
    void testCreateTableIfNotExistsFromDescriptor() throws Exception {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();

        assertCreateTableFromDescriptor(tEnv, TEST_SCHEMA, true);
        assertThatNoException().isThrownBy(() -> tEnv.createTable("T", TEST_DESCRIPTOR, true));

        assertThatThrownBy(() -> tEnv.createTable("T", TEST_DESCRIPTOR, false))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Could not execute CreateTable in path `default_catalog`.`default_database`.`T`");
    }

    @Test
    void testTableFromDescriptor() {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();
        final Table table = tEnv.from(TEST_DESCRIPTOR);

        assertThat(Schema.newBuilder().fromResolvedSchema(table.getResolvedSchema()).build())
                .isEqualTo(TEST_SCHEMA);

        assertThat(table.getQueryOperation())
                .asInstanceOf(type(SourceQueryOperation.class))
                .extracting(SourceQueryOperation::getContextResolvedTable)
                .satisfies(
                        crs -> {
                            assertThat(crs.isAnonymous()).isTrue();
                            assertThat(crs.getIdentifier().toList()).hasSize(1);
                            assertThat(crs.getResolvedTable().getOptions())
                                    .containsEntry("connector", "fake");
                        });

        assertThat(tEnv.getCatalogManager().listTables()).isEmpty();
    }

    @Test
    void testCreateModelFromDescriptor() throws Exception {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();

        assertCreateModelFromDescriptor(tEnv, false);
    }

    @Test
    void testCreateModelIgnoreIfExistsFromDescriptor() throws Exception {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();

        assertCreateModelFromDescriptor(tEnv, true);
        assertThatNoException()
                .isThrownBy(() -> tEnv.createModel("M", TEST_MODEL_DESCRIPTOR, true));

        assertThatThrownBy(() -> tEnv.createModel("M", TEST_MODEL_DESCRIPTOR, false))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Could not execute CreateModel in path `default_catalog`.`default_database`.`M`");
    }

    @Test
    void testCreateModelWithSameName() {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();

        tEnv.createModel("M", TEST_MODEL_DESCRIPTOR);

        tEnv.createModel("M", TEST_MODEL_DESCRIPTOR, true);

        assertThatThrownBy(() -> tEnv.createModel("M", TEST_MODEL_DESCRIPTOR, false))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Could not execute CreateModel in path `default_catalog`.`default_database`.`M`");

        assertThatThrownBy(() -> tEnv.createModel("M", TEST_MODEL_DESCRIPTOR))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Could not execute CreateModel in path `default_catalog`.`default_database`.`M`");
    }

    @Test
    void testDropModel() throws Exception {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();

        tEnv.createModel("M", TEST_MODEL_DESCRIPTOR);

        final String catalog = tEnv.getCurrentCatalog();
        final String database = tEnv.getCurrentDatabase();
        final ObjectPath objectPath = new ObjectPath(database, "M");
        CatalogModel catalogModel =
                tEnv.getCatalog(catalog).orElseThrow(AssertionError::new).getModel(objectPath);
        assertThat(catalogModel).isInstanceOf(CatalogModel.class);
        assertThat(tEnv.dropModel("M", true)).isTrue();
        assertThatThrownBy(
                        () ->
                                tEnv.getCatalog(catalog)
                                        .orElseThrow(AssertionError::new)
                                        .getModel(objectPath))
                .isInstanceOf(ModelNotExistException.class)
                .hasMessage("Model '`default_catalog`.`default_database`.`M`' does not exist.");
    }

    @Test
    void testNonExistingDropModel() throws Exception {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();

        assertThat(tEnv.dropModel("M", true)).isFalse();

        assertThatThrownBy(() -> tEnv.dropModel("M", false))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Model with identifier 'default_catalog.default_database.M' does not exist.");
    }

    @Test
    void testCreateTemporaryModelFromDescriptor() {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();
        assertTemporaryCreateModelFromDescriptor(tEnv, false);
    }

    @Test
    void testCreateTemporaryModelIfNotExistsFromDescriptor() {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();

        assertTemporaryCreateModelFromDescriptor(tEnv, true);
        assertThatNoException()
                .isThrownBy(() -> tEnv.createTemporaryModel("M", TEST_MODEL_DESCRIPTOR, true));

        assertThatThrownBy(() -> tEnv.createTemporaryModel("M", TEST_MODEL_DESCRIPTOR, false))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Temporary model '`default_catalog`.`default_database`.`M`' already exists");
    }

    @Test
    void testCreateModelWithSameNameIgnoreIfExists() {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();

        tEnv.createModel("M", TEST_MODEL_DESCRIPTOR);
        tEnv.createModel("M", TEST_MODEL_DESCRIPTOR, true);
    }

    @Test
    void testListModels() {
        final TableEnvironmentMock tEnv = TableEnvironmentMock.getStreamingInstance();

        tEnv.createModel("M1", TEST_MODEL_DESCRIPTOR);
        tEnv.createModel("M2", TEST_MODEL_DESCRIPTOR);

        String[] models = tEnv.listModels();
        assertThat(models).isNotEmpty();
        assertThat(models[0]).isEqualTo("M1");
        assertThat(models[1]).isEqualTo("M2");
    }

    private static void assertCreateTableFromDescriptor(
            TableEnvironmentMock tEnv, Schema schema, boolean ignoreIfExists)
            throws org.apache.flink.table.catalog.exceptions.TableNotExistException {
        final String catalog = tEnv.getCurrentCatalog();
        final String database = tEnv.getCurrentDatabase();

        if (ignoreIfExists) {
            assertThat(tEnv.createTable("T", TEST_DESCRIPTOR, true)).isTrue();
        } else {
            tEnv.createTable("T", TEST_DESCRIPTOR);
        }

        final ObjectPath objectPath = new ObjectPath(database, "T");
        assertThat(
                        tEnv.getCatalog(catalog)
                                .orElseThrow(AssertionError::new)
                                .tableExists(objectPath))
                .isTrue();

        final CatalogBaseTable catalogTable =
                tEnv.getCatalog(catalog).orElseThrow(AssertionError::new).getTable(objectPath);
        assertThat(catalogTable).isInstanceOf(CatalogTable.class);
        assertThat(catalogTable.getUnresolvedSchema()).isEqualTo(schema);
        assertThat(catalogTable.getOptions())
                .contains(entry("connector", "fake"), entry("a", "Test"));
    }

    private static void assertTemporaryCreateTableFromDescriptor(
            TableEnvironmentMock tEnv, Schema schema, boolean ignoreIfExists) {
        final String catalog = tEnv.getCurrentCatalog();
        final String database = tEnv.getCurrentDatabase();

        if (ignoreIfExists) {
            tEnv.createTemporaryTable("T", TEST_DESCRIPTOR, true);
        } else {
            tEnv.createTemporaryTable("T", TEST_DESCRIPTOR);
        }

        assertThat(
                        tEnv.getCatalog(catalog)
                                .orElseThrow(AssertionError::new)
                                .tableExists(new ObjectPath(database, "T")))
                .isFalse();

        final Optional<ContextResolvedTable> lookupResult =
                tEnv.getCatalogManager().getTable(ObjectIdentifier.of(catalog, database, "T"));
        assertThat(lookupResult.isPresent()).isTrue();

        final CatalogBaseTable catalogTable = lookupResult.get().getResolvedTable();
        assertThat(catalogTable instanceof CatalogTable).isTrue();
        assertThat(catalogTable.getUnresolvedSchema()).isEqualTo(schema);
        assertThat(catalogTable.getOptions().get("connector")).isEqualTo("fake");
        assertThat(catalogTable.getOptions().get("a")).isEqualTo("Test");
    }

    private static void assertCreateModelFromDescriptor(
            TableEnvironmentMock tEnv, boolean ignoreIfExists) throws ModelNotExistException {
        final String catalog = tEnv.getCurrentCatalog();
        final String database = tEnv.getCurrentDatabase();

        if (ignoreIfExists) {
            tEnv.createModel("M", TEST_MODEL_DESCRIPTOR, true);
        } else {
            tEnv.createModel("M", TEST_MODEL_DESCRIPTOR);
        }

        final ObjectPath objectPath = new ObjectPath(database, "M");
        CatalogModel catalogModel =
                tEnv.getCatalog(catalog).orElseThrow(AssertionError::new).getModel(objectPath);
        assertThat(catalogModel).isInstanceOf(CatalogModel.class);
        assertThat(catalogModel.getInputSchema()).isEqualTo(TEST_SCHEMA);
        assertThat(catalogModel.getOptions().get("a")).isEqualTo("Test");
    }

    private static void assertTemporaryCreateModelFromDescriptor(
            TableEnvironmentMock tEnv, boolean ignoreIfExists) {
        final String catalog = tEnv.getCurrentCatalog();
        final String database = tEnv.getCurrentDatabase();

        tEnv.createTemporaryModel("M", TEST_MODEL_DESCRIPTOR, ignoreIfExists);
        final Optional<ContextResolvedModel> lookupResult =
                tEnv.getCatalogManager().getModel(ObjectIdentifier.of(catalog, database, "M"));
        assertThat(lookupResult.isPresent()).isTrue();
        CatalogModel catalogModel = lookupResult.get().getResolvedModel();
        assertThat(catalogModel != null).isTrue();
        assertThat(catalogModel.getInputSchema()).isEqualTo(TEST_SCHEMA);
        assertThat(catalogModel.getOutputSchema()).isEqualTo(TEST_SCHEMA);
        assertThat(catalogModel.getOptions().get("a")).isEqualTo("Test");
    }
}
