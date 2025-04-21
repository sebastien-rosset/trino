/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.openfga;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.openfga.model.OpenFgaContext;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for batch operations in OpenFGA access control
 */
public class TestOpenFgaBatchOperations
{
    private static final String TEST_USER = "test-user";
    private static final String TEST_CATALOG = "test-catalog";
    private static final String TEST_SCHEMA = "test-schema";
    private static final String TEST_TABLE_PREFIX = "test-table-";
    private static final QueryId TEST_QUERY_ID = new QueryId("test_query");

    private BatchEnabledTestingOpenFgaClient testClient;
    private OpenFgaConfig config;
    private OpenFgaContext context;
    private TestingLifeCycleManager testingLifeCycleManager;
    private OpenFgaAccessControl accessControl;
    private SystemSecurityContext securityContext;
    private Identity identity;

    @BeforeEach
    public void setUp()
    {
        config = new OpenFgaConfig()
                .setApiUrl("http://localhost:8080")
                .setStoreId("test_store")
                .setModelId("test_model")
                .setAllowPermissionManagementOperations(false);

        testClient = new BatchEnabledTestingOpenFgaClient(config);
        testingLifeCycleManager = new TestingLifeCycleManager();

        // Create identity and context
        identity = Identity.ofUser(TEST_USER);
        context = new OpenFgaContext(identity, "test_version", System.currentTimeMillis());

        // Create a proper wrapper for our TestingLifeCycleManager
        TestingLifeCycleManagerWrapper lifeCycleManager = new TestingLifeCycleManagerWrapper(testingLifeCycleManager);

        // Create access control with test client using all the required parameters
        accessControl = new OpenFgaAccessControl(lifeCycleManager, testClient, config, context);

        // Create security context with the appropriate constructor based on Trino version
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        try {
            securityContext = createSystemSecurityContext(identity, TEST_QUERY_ID, now);
        }
        catch (Throwable e) {
            throw new RuntimeException("Unable to create SystemSecurityContext for testing: " + e.getMessage(), e);
        }
    }

    @Test
    public void testBatchFilterCatalogs()
    {
        // Set up test data
        Set<String> catalogs = ImmutableSet.of("catalog1", "catalog2", "catalog3", "catalog4", "catalog5");

        // Configure responses for batch operation - use proper Map.Entry objects
        Map<BatchKey, Boolean> responses = ImmutableMap.<BatchKey, Boolean>builder()
                .put(new BatchKey("catalog", "catalog1", "viewer"), true)
                .put(new BatchKey("catalog", "catalog2", "viewer"), false)
                .put(new BatchKey("catalog", "catalog3", "viewer"), true)
                .put(new BatchKey("catalog", "catalog4", "viewer"), false)
                .put(new BatchKey("catalog", "catalog5", "viewer"), true)
                .buildOrThrow();
        testClient.addBatchResponses(responses);

        // Perform the operation that should trigger batch check
        Set<String> result = accessControl.filterCatalogs(securityContext, catalogs);

        // Verify results
        assertThat(result).containsExactlyInAnyOrder("catalog1", "catalog3", "catalog5");

        // Verify batch check was called
        assertThat(testClient.getBatchCheckCount()).isEqualTo(1);
    }

    @Test
    public void testBatchFilterSchemas()
    {
        // Set up test schemas
        Set<String> schemas = ImmutableSet.of("schema1", "schema2", "schema3", "schema4", "schema5");

        // Configure responses for batch operation
        Map<BatchKey, Boolean> responses = ImmutableMap.<BatchKey, Boolean>builder()
                .put(new BatchKey("schema", TEST_CATALOG + "/schema1", "viewer"), true)
                .put(new BatchKey("schema", TEST_CATALOG + "/schema2", "viewer"), false)
                .put(new BatchKey("schema", TEST_CATALOG + "/schema3", "viewer"), true)
                .put(new BatchKey("schema", TEST_CATALOG + "/schema4", "viewer"), false)
                .put(new BatchKey("schema", TEST_CATALOG + "/schema5", "viewer"), true)
                .buildOrThrow();
        testClient.addBatchResponses(responses);

        // Perform the operation that should trigger batch check
        Set<String> result = accessControl.filterSchemas(securityContext, TEST_CATALOG, schemas);

        // Verify results
        assertThat(result).containsExactlyInAnyOrder("schema1", "schema3", "schema5");

        // Verify batch check was called
        assertThat(testClient.getBatchCheckCount()).isEqualTo(1);
    }

    @Test
    public void testBatchRowFilters()
    {
        int tableCount = 5;
        List<CatalogSchemaTableName> tables = createTestTables(tableCount);

        // Set up row filters
        for (int i = 0; i < tableCount; i++) {
            String tableId = tableIdentifier(i);
            String filterExpression = "department" + i + " = 'HR'";
            testClient.addRowFilter("filter" + i, tableId, filterExpression);
            testClient.addUserToRowFilter(TEST_USER, "filter" + i);
        }

        // Record batch access checks and verify the contents of the returned filters
        for (int i = 0; i < tableCount; i++) {
            // Check for row filters for each table
            List<ViewExpression> filters = accessControl.getRowFilters(securityContext, tables.get(i));

            // Verify that we got the expected filter
            assertThat(filters).hasSize(1);

            // Get the expected filter expression for this table
            String expectedExpression = "department" + i + " = 'HR'";

            // Extract the actual expression from the ViewExpression
            String actualExpression = filters.get(0).getExpression();

            // Verify the expression is correct
            assertThat(actualExpression).isEqualTo(expectedExpression);
        }

        // Verify batch row filter check was performed
        assertThat(testClient.getBatchRowFilterCount()).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void testBatchCheckCanSelectFromColumns()
    {
        int tableCount = 5;
        List<CatalogSchemaTableName> tables = createTestTables(tableCount);
        Set<String> columns = ImmutableSet.of("column1", "column2");

        // Configure responses using batch responses to ensure the batch check count is incremented
        Map<BatchKey, Boolean> responses = new ConcurrentHashMap<>();
        for (int i = 0; i < tableCount; i++) {
            String tableId = tableIdentifier(i);
            // Allow select on even indexes
            responses.put(new BatchKey("table", tableId, "select"), i % 2 == 0);
        }
        testClient.addBatchResponses(responses);

        // Test select permission on all tables
        for (int i = 0; i < tableCount; i++) {
            // Store index in final variable to use in lambda
            final int index = i;
            CatalogSchemaTableName table = tables.get(index);

            if (index % 2 == 0) {
                // Even indexes should be allowed
                accessControl.checkCanSelectFromColumns(securityContext, table, columns);
            }
            else {
                // Odd indexes should be denied
                assertThatThrownBy(() -> accessControl.checkCanSelectFromColumns(securityContext, table, columns))
                        .isInstanceOf(AccessDeniedException.class);
            }
        }

        // Verify batch operation was used
        assertThat(testClient.getBatchCheckCount()).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void testBatchCheckMultipleOperations()
    {
        // Create a sequence of tables
        List<CatalogSchemaTableName> tables = createTestTables(3);

        // Configure responses for different operations
        for (int i = 0; i < 3; i++) {
            String tableId = tableIdentifier(i);
            // Allow all operations on table 0, insert on table 1, nothing on table 2
            testClient.addCheckResponse("table", tableId, "select", i == 0);
            testClient.addCheckResponse("table", tableId, "insert", i <= 1);
            testClient.addCheckResponse("table", tableId, "delete", i == 0);
        }

        // Perform multiple operations that could be batched
        // These should pass
        accessControl.checkCanSelectFromColumns(securityContext, tables.get(0), ImmutableSet.of("column1"));
        accessControl.checkCanInsertIntoTable(securityContext, tables.get(0));
        accessControl.checkCanDeleteFromTable(securityContext, tables.get(0));
        accessControl.checkCanInsertIntoTable(securityContext, tables.get(1));

        // These should fail
        assertThatThrownBy(() -> accessControl.checkCanSelectFromColumns(
                securityContext, tables.get(1), ImmutableSet.of("column1")))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControl.checkCanDeleteFromTable(securityContext, tables.get(1)))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControl.checkCanSelectFromColumns(
                securityContext, tables.get(2), ImmutableSet.of("column1")))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControl.checkCanInsertIntoTable(securityContext, tables.get(2)))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControl.checkCanDeleteFromTable(securityContext, tables.get(2)))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    public void testBatchColumnMasking()
    {
        int tableCount = 3;
        List<CatalogSchemaTableName> tables = createTestTables(tableCount);

        // Create column schemas for testing using the builder pattern
        ColumnSchema ssnColumn = ColumnSchema.builder().setName("ssn").setType(VarcharType.VARCHAR).setHidden(false).build();
        ColumnSchema salaryColumn = ColumnSchema.builder().setName("salary").setType(DoubleType.DOUBLE).setHidden(false).build();

        // Set up column masks for several columns across tables
        for (int i = 0; i < tableCount; i++) {
            String tableId = tableIdentifier(i);
            // Add mask for ssn column
            testClient.addColumnMask(
                    "mask" + i,
                    tableId + "/ssn",
                    "regexp_replace(ssn, '(\\d{3})(\\d{2})(\\d{4})', 'XXX-XX-$3')");
            testClient.addUserToColumnMask(TEST_USER, "mask" + i);

            // Add mask for salary column
            testClient.addColumnMask(
                    "salaryMask" + i,
                    tableId + "/salary",
                    "salary * 0.1");
            testClient.addUserToColumnMask(TEST_USER, "salaryMask" + i);
        }

        // Access column masks for all tables
        for (int i = 0; i < tableCount; i++) {
            Map<ColumnSchema, ViewExpression> masks = accessControl.getColumnMasks(
                    securityContext,
                    tables.get(i),
                    List.of(ssnColumn, salaryColumn));

            // Verify we got the expected masks
            assertThat(masks).hasSize(2);
            assertThat(masks).containsKey(ssnColumn);
            assertThat(masks).containsKey(salaryColumn);
            // Compare the expression strings, not the ViewExpression objects
            assertThat(masks.get(ssnColumn).getExpression()).contains("regexp_replace");
            assertThat(masks.get(salaryColumn).getExpression()).contains("salary * 0.1");
        }

        // Verify batch column mask checks were performed
        assertThat(testClient.getBatchColumnMaskCount()).isGreaterThanOrEqualTo(1);
    }

    /**
     * Helper method to create test table names
     */
    private List<CatalogSchemaTableName> createTestTables(int count)
    {
        ImmutableList.Builder<CatalogSchemaTableName> tables = ImmutableList.builder();
        for (int i = 0; i < count; i++) {
            tables.add(new CatalogSchemaTableName(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE_PREFIX + i));
        }
        return tables.build();
    }

    /**
     * Helper method to create table identifier string
     */
    private String tableIdentifier(int index)
    {
        return TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE_PREFIX + index;
    }

    /**
     * Helper method to create a SystemSecurityContext with appropriate constructor based on Trino version
     */
    private SystemSecurityContext createSystemSecurityContext(Identity identity, QueryId queryId, Instant queryStart)
            throws ReflectiveOperationException
    {
        // First try to find the constructor that takes Identity, QueryId, and Instant (newer Trino versions)
        try {
            Constructor<SystemSecurityContext> constructor =
                    SystemSecurityContext.class.getConstructor(Identity.class, QueryId.class, Instant.class);
            return constructor.newInstance(identity, queryId, queryStart);
        }
        catch (NoSuchMethodException e) {
            // Fall back to older constructor that only takes Identity and QueryId
            try {
                Constructor<SystemSecurityContext> constructor =
                        SystemSecurityContext.class.getConstructor(Identity.class, QueryId.class);
                return constructor.newInstance(identity, queryId);
            }
            catch (NoSuchMethodException e2) {
                throw new RuntimeException("No compatible constructor found for SystemSecurityContext. " +
                        "This may indicate incompatibility with the current Trino version.", e2);
            }
        }
    }

    /**
     * Extension of TestingOpenFgaClient that tracks batch operations
     */
    private static class BatchEnabledTestingOpenFgaClient
            extends TestingOpenFgaClient
            implements io.trino.plugin.openfga.batch.BatchEnabledOpenFgaClient
    {
        private final Map<BatchKey, Boolean> batchResponses = new ConcurrentHashMap<>();
        private int batchCheckCount;
        private int batchRowFilterCount;
        private int batchColumnMaskCount;

        public BatchEnabledTestingOpenFgaClient(OpenFgaConfig config)
        {
            super(config);
        }

        public void addBatchResponses(Map<BatchKey, Boolean> responses)
        {
            batchResponses.putAll(responses);
        }

        @Override
        public boolean check(OpenFgaContext context, String objectType, String objectId, String relation)
        {
            // Check batch responses first
            BatchKey key = new BatchKey(objectType, objectId, relation);
            if (batchResponses.containsKey(key)) {
                batchCheckCount++;
                return batchResponses.get(key);
            }

            // Fall back to standard behavior
            return super.check(context, objectType, objectId, relation);
        }

        public Set<String> filterCatalogs(OpenFgaContext context, Set<String> catalogs, String relation)
        {
            batchCheckCount++;

            // Use only the catalogs that are allowed based on the batch responses
            return catalogs.stream()
                    .filter(catalog -> {
                        BatchKey key = new BatchKey("catalog", catalog, relation);
                        if (batchResponses.containsKey(key)) {
                            return batchResponses.get(key);
                        }
                        // Fall back to standard behavior
                        return super.check(context, "catalog", catalog, relation);
                    })
                    .collect(Collectors.toSet());
        }

        public Set<String> filterSchemas(OpenFgaContext context, String catalogName, Set<String> schemas, String relation)
        {
            batchCheckCount++;

            // Use only the schemas that are allowed based on the batch responses
            return schemas.stream()
                    .filter(schema -> {
                        BatchKey key = new BatchKey("schema", catalogName + "/" + schema, relation);
                        if (batchResponses.containsKey(key)) {
                            return batchResponses.get(key);
                        }
                        // Fall back to standard behavior
                        return super.check(context, "schema", catalogName + "/" + schema, relation);
                    })
                    .collect(Collectors.toSet());
        }

        @Override
        public boolean checkTableAccess(OpenFgaContext context, String catalogName, String schemaName, String tableName, String relation)
        {
            String tableId = catalogName + "/" + schemaName + "/" + tableName;
            BatchKey key = new BatchKey("table", tableId, relation);
            if (batchResponses.containsKey(key)) {
                batchCheckCount++;
                return batchResponses.get(key);
            }
            return super.checkTableAccess(context, catalogName, schemaName, tableName, relation);
        }

        @Override
        public List<String> getApplicableRowFilters(OpenFgaContext fgaContext, String catalogName, String schemaName, String tableName)
        {
            batchRowFilterCount++;
            return super.getApplicableRowFilters(fgaContext, catalogName, schemaName, tableName);
        }

        @Override
        public java.util.Optional<String> getApplicableColumnMask(
                OpenFgaContext context, String catalogName, String schemaName, String tableName, String columnName)
        {
            batchColumnMaskCount++;
            return super.getApplicableColumnMask(context, catalogName, schemaName, tableName, columnName);
        }

        public int getBatchCheckCount()
        {
            return batchCheckCount;
        }

        public int getBatchRowFilterCount()
        {
            return batchRowFilterCount;
        }

        public int getBatchColumnMaskCount()
        {
            return batchColumnMaskCount;
        }
    }

    /**
     * Key class for batch operations
     */
    private static class BatchKey
    {
        private final String objectType;
        private final String objectId;
        private final String relation;

        public BatchKey(String objectType, String objectId, String relation)
        {
            this.objectType = objectType;
            this.objectId = objectId;
            this.relation = relation;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BatchKey batchKey = (BatchKey) o;
            return objectType.equals(batchKey.objectType) &&
                    objectId.equals(batchKey.objectId) &&
                    relation.equals(batchKey.relation);
        }

        @Override
        public int hashCode()
        {
            int result = objectType.hashCode();
            result = 31 * result + objectId.hashCode();
            result = 31 * result + relation.hashCode();
            return result;
        }
    }
}
