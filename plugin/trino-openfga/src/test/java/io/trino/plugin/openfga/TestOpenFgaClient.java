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

import dev.openfga.sdk.api.client.model.ClientReadRequest;
import io.trino.plugin.openfga.model.OpenFgaContext;
import io.trino.spi.security.Identity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenFgaClient
{
    private static final String TEST_USER = "test-user";
    private static final String TEST_CATALOG = "test-catalog";
    private static final String TEST_SCHEMA = "test-schema";
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_VERSION = "test-version";

    private TestingOpenFgaClient testClient;
    private OpenFgaContext context;
    private OpenFgaConfig config;
    private Identity identity;
    private TestingLifeCycleManager testingLifeCycleManager;

    @BeforeEach
    public void setUp()
    {
        config = new OpenFgaConfig()
                .setApiUrl("http://localhost:8080")
                .setStoreId("test_store")
                .setModelId("test_model")
                .setAllowPermissionManagementOperations(false);

        // Use the new constructor to pass the config directly to the test client
        testClient = new TestingOpenFgaClient(config);
        testingLifeCycleManager = new TestingLifeCycleManager();

        // Create identity and context
        identity = Identity.ofUser(TEST_USER);
        context = new OpenFgaContext(identity, TEST_VERSION, System.currentTimeMillis());
    }

    @Test
    public void testCheckCatalogAccess()
    {
        // Setup the test client to return true for catalog viewer permission
        testClient.addCheckResponse("catalog", "test_catalog", "show_schemas", true);
        assertThat(testClient.check(new OpenFgaContext(Identity.ofUser(TEST_USER), TEST_VERSION, System.currentTimeMillis()), "catalog", "test_catalog", "show_schemas")).isTrue();

        testClient.addCheckResponse("table", "catalog/schema/table", "select", true);
        assertThat(testClient.checkTableAccess(new OpenFgaContext(Identity.ofUser(TEST_USER), TEST_VERSION, System.currentTimeMillis()), "catalog", "schema", "table", "select")).isTrue();

        testClient.addCheckResponse("catalog", "test_catalog", "show_schemas", false);
        assertThat(testClient.check(new OpenFgaContext(Identity.ofUser(TEST_USER), TEST_VERSION, System.currentTimeMillis()), "catalog", "test_catalog", "show_schemas")).isFalse();

        testClient.addCheckResponse("catalog", "test_catalog", "show_schemas", true);
        assertThat(testClient.check(new OpenFgaContext(Identity.ofUser(TEST_USER), TEST_VERSION, System.currentTimeMillis()), "catalog", "test_catalog", "show_schemas")).isTrue();
        testClient.addCheckResponse("catalog", "test_catalog", "show_schemas", false);
        assertThat(testClient.check(new OpenFgaContext(Identity.ofUser(TEST_USER), TEST_VERSION, System.currentTimeMillis()), "catalog", "test_catalog", "show_schemas")).isFalse();
        testClient.addCheckResponse("catalog", "test_catalog", "show_schemas", true);
        assertThat(testClient.check(new OpenFgaContext(Identity.ofUser(TEST_USER), TEST_VERSION, System.currentTimeMillis()), "catalog", "test_catalog", "show_schemas")).isTrue();
    }

    @Test
    public void testCheckSchemaAccess()
    {
        // The schema objectId format should be "catalogName/schemaName"
        String expectedObjectId = TEST_CATALOG + "/" + TEST_SCHEMA;

        // Setup the test client to return true for schema viewer permission
        testClient.addCheckResponse("schema", expectedObjectId, "viewer", true);

        boolean result = testClient.checkSchemaAccess(context, TEST_CATALOG, TEST_SCHEMA, "viewer");

        assertThat(result).isTrue();
        assertThat(testClient.getCheckCallCount("schema", expectedObjectId, "viewer")).isEqualTo(1);
    }

    @Test
    public void testCheckTableAccess()
    {
        // The table objectId format should be "catalogName/schemaName/tableName"
        String expectedObjectId = TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE;

        // Setup the test client to return true for table select permission
        testClient.addCheckResponse("table", expectedObjectId, "select", true);

        boolean result = testClient.checkTableAccess(context, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, "select");

        assertThat(result).isTrue();
        assertThat(testClient.getCheckCallCount("table", expectedObjectId, "select")).isEqualTo(1);
    }

    @Test
    public void testCheckTableAccessDenied()
    {
        // The table objectId format should be "catalogName/schemaName/tableName"
        String expectedObjectId = TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE;

        // Setup the test client to return false for table insert permission
        testClient.addCheckResponse("table", expectedObjectId, "insert", false);

        boolean result = testClient.checkTableAccess(context, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, "insert");

        assertThat(result).isFalse();
        assertThat(testClient.getCheckCallCount("table", expectedObjectId, "insert")).isEqualTo(1);
    }

    @Test
    public void testGetApplicableRowFilters()
    {
        // Configure the test client to return row filters with the actual expression directly
        // The ID format in the ClientReadResponse should have the "filter:" prefix
        // This matches how the OpenFgaClient.getApplicableRowFilters() method processes the response
        String filterExpression = "department = 'SALES'";
        testClient.addRowFilter("dept_filter", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, filterExpression);
        testClient.addUserToRowFilter(TEST_USER, "dept_filter");

        // Create and configure the expected read request
        ClientReadRequest expectedRequest = new ClientReadRequest();
        expectedRequest.user("user:" + TEST_USER);
        expectedRequest.relation("assigned");
        expectedRequest._object(null);

        // The filter ID format in the response needs to be "filter:expression"
        testClient.whenRead(expectedRequest).thenReturn(List.of("filter:" + filterExpression));

        List<String> filters = testClient.getApplicableRowFilters(context, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);

        assertThat(filters).hasSize(1);
        assertThat(filters.get(0)).isEqualTo(filterExpression);
    }

    @Test
    public void testGetApplicableRowFiltersViaRole()
    {
        // Configure the test client to return filters via role membership
        String tableId = TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE;
        String filterExpression = "role = 'ANALYST'";
        String roleId = "analyst_role";

        testClient.addRowFilter("role_filter", tableId, filterExpression);
        testClient.addRoleToRowFilter("analyst_role", "role_filter");
        testClient.addUserToRole(TEST_USER, "analyst_role");

        // Create and configure the expected read request
        ClientReadRequest expectedRequest = new ClientReadRequest();
        expectedRequest.user("user:" + TEST_USER);
        expectedRequest.relation("assigned");
        expectedRequest._object(null);

        // The filter ID format in the response now includes the table identifier
        testClient.whenRead(expectedRequest).thenReturn(List.of("filter:" + tableId + ":" + filterExpression));

        List<String> filters = testClient.getApplicableRowFilters(context, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);

        assertThat(filters).hasSize(1);
        assertThat(filters.get(0)).isEqualTo(filterExpression);
    }

    @Test
    public void testGetMultipleApplicableRowFilters()
    {
        // Setup the expected read request that will be made by getApplicableRowFilters
        ClientReadRequest expectedRequest = new ClientReadRequest();
        expectedRequest.user("user:" + TEST_USER);
        expectedRequest.relation("assigned");
        expectedRequest._object(null);

        String tableId = TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE;

        // Create a list of filter IDs to return for the filters - include table identifiers
        List<String> filterIds = List.of(
                "filter:" + tableId + ":department = 'SALES'",
                "filter:" + tableId + ":region = 'NORTH'");

        // Configure the mock client with our expected request and response
        testClient.whenRead(expectedRequest).thenReturn(filterIds);

        // Call the method under test
        List<String> filters = testClient.getApplicableRowFilters(context, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);

        // Verify the results
        assertThat(filters).hasSize(2);
        assertThat(filters).contains("department = 'SALES'", "region = 'NORTH'");
    }

    @Test
    public void testNoApplicableRowFilters()
    {
        // Setup the expected read request that will be made by getApplicableRowFilters
        ClientReadRequest expectedRequest = new ClientReadRequest();
        expectedRequest.user("user:" + TEST_USER);
        expectedRequest.relation("assigned");
        expectedRequest._object(null);

        // Create an empty list since we're testing the case where no filters apply
        List<String> filterIds = List.of();

        // Configure the mock client with our expected request and response using the new pattern
        testClient.whenRead(expectedRequest).thenReturn(filterIds);

        // Call the method under test
        List<String> filters = testClient.getApplicableRowFilters(context, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);

        // Should return empty list since no filters are configured
        assertThat(filters).isEmpty();
    }

    @Test
    public void testRowFilterForDifferentTable()
    {
        // Setup the expected read request that will be made by getApplicableRowFilters
        ClientReadRequest expectedRequest = new ClientReadRequest();
        expectedRequest.user("user:" + TEST_USER);
        expectedRequest.relation("assigned");
        expectedRequest._object(null);

        // Return a filter that's for a different table
        String differentTableId = TEST_CATALOG + "/" + TEST_SCHEMA + "/different_table";
        List<String> filterIds = List.of("filter:" + differentTableId + ":department = 'SALES'");

        // Configure the mock client with our expected request and response
        testClient.whenRead(expectedRequest).thenReturn(filterIds);

        // Call the method under test with our target table (which is different from the filter's table)
        List<String> filters = testClient.getApplicableRowFilters(context, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);

        // Should return an empty list since no filters apply for this table
        assertThat(filters).isEmpty();
    }

    @Test
    public void testWildcardRowFilters()
    {
        // Setup the expected read request
        ClientReadRequest expectedRequest = new ClientReadRequest();
        expectedRequest.user("user:" + TEST_USER);
        expectedRequest.relation("assigned");
        expectedRequest._object(null);

        // Return filters with wildcard patterns
        String schemaWildcard = TEST_CATALOG + "/" + TEST_SCHEMA + "/*";
        String globalWildcard = "*";

        List<String> filterIds = List.of(
                "filter:" + schemaWildcard + ":schema_level_filter = true",
                "filter:" + globalWildcard + ":global_filter = true");

        // Configure the mock client
        testClient.whenRead(expectedRequest).thenReturn(filterIds);

        // Call the method under test
        List<String> filters = testClient.getApplicableRowFilters(context, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);

        // Both filters should apply - schema-level and global
        assertThat(filters).hasSize(2);
        assertThat(filters).contains("schema_level_filter = true", "global_filter = true");
    }

    @Test
    public void testCheckColumnAccess()
    {
        // The column objectId format should be "catalogName/schemaName/tableName/columnName"
        String columnName = "user_id";
        String expectedObjectId = TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE + "/" + columnName;

        // Create the expected check request
        dev.openfga.sdk.api.client.model.ClientCheckRequest expectedRequest = new dev.openfga.sdk.api.client.model.ClientCheckRequest();
        expectedRequest.user("user:" + TEST_USER);
        expectedRequest.relation("select");
        expectedRequest._object("column:" + expectedObjectId);

        // Configure the mock client to return true for this request
        testClient.whenCheck(expectedRequest).thenReturn(true);

        // Call the method under test
        boolean result = testClient.checkColumnAccess(context, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, columnName, "select");

        // Verify the results
        assertThat(result).isTrue();
    }

    @Test
    public void testCheckColumnAccessDenied()
    {
        // The column objectId format should be "catalogName/schemaName/tableName/columnName"
        String columnName = "salary";
        String expectedObjectId = TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE + "/" + columnName;

        // Create the expected check request
        dev.openfga.sdk.api.client.model.ClientCheckRequest expectedRequest = new dev.openfga.sdk.api.client.model.ClientCheckRequest();
        expectedRequest.user("user:" + TEST_USER);
        expectedRequest.relation("select");
        expectedRequest._object("column:" + expectedObjectId);

        // Configure the mock client to return false for this request
        testClient.whenCheck(expectedRequest).thenReturn(false);

        // Call the method under test
        boolean result = testClient.checkColumnAccess(context, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, columnName, "select");

        // Verify the results
        assertThat(result).isFalse();
    }

    @Test
    public void testNullIdentity()
    {
        // Create a context with null identity
        OpenFgaContext nullContext = new OpenFgaContext(null, TEST_VERSION, 0);

        // Test check methods with null identity - all should return false
        assertThat(testClient.check(nullContext, "table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "select")).isFalse();
        assertThat(testClient.checkCatalogAccess(nullContext, TEST_CATALOG, "viewer")).isFalse();
        assertThat(testClient.checkSchemaAccess(nullContext, TEST_CATALOG, TEST_SCHEMA, "viewer")).isFalse();
        assertThat(testClient.checkTableAccess(nullContext, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, "select")).isFalse();

        // Test row filters with null identity - should return empty list
        List<String> filters = testClient.getApplicableRowFilters(nullContext, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);
        assertThat(filters).isEmpty();
    }

    @Test
    public void testGetApplicableColumnMask()
    {
        // We need to update TestingOpenFgaClient to handle column masks
        // For now, we can verify with what we have - testing that identity is checked

        // Create a context with null identity
        OpenFgaContext nullContext = new OpenFgaContext(null, TEST_VERSION, 0);

        // For null identity, column mask should be empty
        assertThat(testClient.getApplicableColumnMask(nullContext, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, "salary"))
                .isEmpty();
    }

    @Test
    public void testGetApplicableColumnMaskForUser()
    {
        // Setup test data
        String columnName = "salary";
        String columnId = TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE + "/" + columnName;
        String maskId = "salary_mask";
        String maskExpression = "CAST(substring(salary, 1, 2) || '***' AS VARCHAR)";

        // Add column mask for user
        testClient.addColumnMask(maskId, columnId, maskExpression);
        testClient.addUserToColumnMask(TEST_USER, maskId);

        // Create and configure the expected read request
        ClientReadRequest expectedRequest = new ClientReadRequest();
        expectedRequest.user("user:" + TEST_USER);
        expectedRequest.relation("assigned");
        expectedRequest._object(null);

        // The mask ID format in the response should include the column identifier
        testClient.whenRead(expectedRequest).thenReturn(List.of("mask:" + columnId + ":" + maskExpression));

        // Test mask retrieval
        Optional<String> mask = testClient.getApplicableColumnMask(context, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, columnName);

        assertThat(mask).isPresent();
        assertThat(mask.get()).isEqualTo(maskExpression);
        assertThat(testClient.getColumnMaskCallCount(TEST_USER, columnId)).isEqualTo(1);
    }

    @Test
    public void testGetApplicableColumnMaskViaRole()
    {
        // Setup test data
        String columnName = "phone";
        String columnId = TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE + "/" + columnName;
        String maskId = "phone_mask";
        String maskExpression = "CONCAT('XXX-XXX-', SUBSTRING(phone, 8, 4))";
        String roleId = "analyst_role";

        // Add column mask via role
        testClient.addColumnMask(maskId, columnId, maskExpression);
        testClient.addRoleToColumnMask(roleId, maskId);
        testClient.addUserToRole(TEST_USER, roleId);

        // Create and configure the expected read request
        ClientReadRequest expectedRequest = new ClientReadRequest();
        expectedRequest.user("user:" + TEST_USER);
        expectedRequest.relation("assigned");
        expectedRequest._object(null);

        // The mask ID format in the response now includes the column identifier
        // This matches the updated OpenFgaClient.getApplicableColumnMask() implementation
        testClient.whenRead(expectedRequest).thenReturn(List.of("mask:" + columnId + ":" + maskExpression));

        // Test mask retrieval
        Optional<String> mask = testClient.getApplicableColumnMask(context, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, columnName);

        assertThat(mask).isPresent();
        assertThat(mask.get()).isEqualTo(maskExpression);
    }

    @Test
    public void testGetApplicableColumnMaskWithNoMatchingMasks()
    {
        // Setup test data for a different column
        String otherColumnName = "address";
        String otherColumnId = TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE + "/" + otherColumnName;
        String maskId = "address_mask";
        String maskExpression = "CONCAT('** Hidden Address **')";

        // Add column mask for a different column
        testClient.addColumnMask(maskId, otherColumnId, maskExpression);
        testClient.addUserToColumnMask(TEST_USER, maskId);

        // Test for a column with no mask
        String targetColumnName = "salary";
        String targetColumnId = TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE + "/" + targetColumnName;

        Optional<String> mask = testClient.getApplicableColumnMask(context, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, targetColumnName);

        // Should return empty since no mask applies to this column
        assertThat(mask).isEmpty();
        assertThat(testClient.getColumnMaskCallCount(TEST_USER, targetColumnId)).isEqualTo(1);
    }

    @Test
    public void testPrecedenceWhenMultipleMasksApply()
    {
        // Setup test data with multiple masks for same column - direct and via role
        String columnName = "email";
        String columnId = TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE + "/" + columnName;

        String directMaskId = "direct_email_mask";
        String directMaskExpression = "CONCAT('***@', SUBSTRING(email, POSITION('@' IN email) + 1))";

        String roleMaskId = "role_email_mask";
        String roleMaskExpression = "CONCAT('***@example.com')";
        String roleId = "masked_role";

        // Add both direct and role masks
        testClient.addColumnMask(directMaskId, columnId, directMaskExpression);
        testClient.addUserToColumnMask(TEST_USER, directMaskId);

        testClient.addColumnMask(roleMaskId, columnId, roleMaskExpression);
        testClient.addRoleToColumnMask(roleId, roleMaskId);
        testClient.addUserToRole(TEST_USER, roleId);

        // Test mask retrieval - should get the direct user mask first
        Optional<String> mask = testClient.getApplicableColumnMask(context, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, columnName);

        assertThat(mask).isPresent();
        // Direct user mask should take precedence
        assertThat(mask.get()).isEqualTo(directMaskExpression);
    }

    @Test
    public void testWithConfiguration()
    {
        // Test creating a client with configuration
        OpenFgaConfig config = new OpenFgaConfig()
                .setApiUrl("http://fga-server:8080")
                .setStoreId("test-store")
                .setModelId("test-model");

        // The constructor should not throw exceptions
        OpenFgaClient client = new OpenFgaClient(config);

        // Since we can't easily test the real client without mocking,
        // just verify the constructor completes without error
        assertThat(client).isNotNull();
    }
}
