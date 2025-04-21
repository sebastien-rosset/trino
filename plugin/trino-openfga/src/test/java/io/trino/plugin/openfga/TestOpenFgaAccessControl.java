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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.openfga.model.OpenFgaContext;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.SchemaTableName;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOpenFgaAccessControl
{
    private static final String TEST_USER = "test-user";
    private static final String TEST_CATALOG = "test-catalog";
    private static final String TEST_SCHEMA = "test-schema";
    private static final String TEST_TABLE = "test-table";
    private static final QueryId TEST_QUERY_ID = new QueryId("test_query");

    private TestingOpenFgaClient testClient;
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

        // Use the new constructor to pass the config directly to the test client
        testClient = new TestingOpenFgaClient(config);
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
            // First try the constructor with Instant parameter (newer Trino versions)
            securityContext = createSystemSecurityContext(identity, TEST_QUERY_ID, now);
        }
        catch (Throwable e) {
            throw new RuntimeException("Unable to create SystemSecurityContext for testing: " + e.getMessage(), e);
        }
    }

    @Test
    public void testCanAccessCatalog()
    {
        testClient.addCheckResponse("catalog", "test_catalog", "viewer", true);
        assertThat(accessControl.canAccessCatalog(securityContext, "test_catalog")).isTrue();
    }

    @Test
    public void testFilterCatalogs()
    {
        Set<String> catalogs = ImmutableSet.of(TEST_CATALOG, "other_catalog");

        testClient.addCheckResponse("catalog", TEST_CATALOG, "viewer", true);
        testClient.addCheckResponse("catalog", "other_catalog", "viewer", false);

        Set<String> result = accessControl.filterCatalogs(securityContext, catalogs);

        assertThat(result).isEqualTo(ImmutableSet.of(TEST_CATALOG));
    }

    @Test
    public void testFilterSchemas()
    {
        Set<String> schemas = ImmutableSet.of(TEST_SCHEMA, "other_schema");

        testClient.addCheckResponse("schema", TEST_CATALOG + "/" + TEST_SCHEMA, "viewer", true);
        testClient.addCheckResponse("schema", TEST_CATALOG + "/other_schema", "viewer", false);

        Set<String> result = accessControl.filterSchemas(securityContext, TEST_CATALOG, schemas);

        assertThat(result).isEqualTo(ImmutableSet.of(TEST_SCHEMA));
    }

    @Test
    public void testFilterTables()
    {
        SchemaTableName testTable = new SchemaTableName(TEST_SCHEMA, TEST_TABLE);
        SchemaTableName otherTable = new SchemaTableName(TEST_SCHEMA, "other_table");
        Set<SchemaTableName> tables = ImmutableSet.of(testTable, otherTable);

        testClient.addCheckResponse("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "viewer", true);
        testClient.addCheckResponse("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/other_table", "viewer", false);

        Set<SchemaTableName> result = accessControl.filterTables(securityContext, TEST_CATALOG, tables);

        assertThat(result).isEqualTo(ImmutableSet.of(testTable));
    }

    @Test
    public void testCheckCanExecuteQuery()
    {
        testClient.addCheckResponse("system", "system", "execute_query", true);

        // Should not throw an exception
        accessControl.checkCanExecuteQuery(identity, TEST_QUERY_ID);

        assertThat(testClient.getCheckCallCount("system", "system", "execute_query")).isEqualTo(1);
    }

    @Test
    public void testCheckCanExecuteQueryDenied()
    {
        testClient.addCheckResponse("system", "system", "execute_query", false);

        // Should throw an exception
        assertThatThrownBy(() -> accessControl.checkCanExecuteQuery(identity, TEST_QUERY_ID))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    public void testCheckCanDropTable()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);

        testClient.addCheckResponse("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "admin", true);

        // Should not throw an exception
        accessControl.checkCanDropTable(securityContext, table);

        assertThat(testClient.getCheckCallCount("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "admin")).isEqualTo(1);
    }

    @Test
    public void testCheckCanDropTableDenied()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);

        testClient.addCheckResponse("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "admin", false);

        // Should throw an exception
        assertThatThrownBy(() -> accessControl.checkCanDropTable(securityContext, table))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    public void testCheckCanSelectFromColumns()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);
        Set<String> columns = ImmutableSet.of("column1", "column2");

        testClient.addCheckResponse("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "select", true);

        // Should not throw an exception
        accessControl.checkCanSelectFromColumns(securityContext, table, columns);

        // Check that the method was actually called
        assertThat(testClient.getCheckCallCount("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "select")).isEqualTo(1);
    }

    @Test
    public void testCheckCanSelectFromColumnsDenied()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);
        Set<String> columns = ImmutableSet.of("column1", "column2");

        testClient.addCheckResponse("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "select", false);

        // Should throw an exception
        assertThatThrownBy(() -> accessControl.checkCanSelectFromColumns(securityContext, table, columns))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    public void testCheckCanShowSchemas()
    {
        // Note: In OpenFgaAccessControl.checkCanShowSchemas, we call fgaClient.check with relation "show_schemas"
        // However, in our test we're adding a response for relation "viewer"
        testClient.addCheckResponse("catalog", TEST_CATALOG, "show_schemas", true);

        // Should not throw an exception
        accessControl.checkCanShowSchemas(securityContext, TEST_CATALOG);

        assertThat(testClient.getCheckCallCount("catalog", TEST_CATALOG, "show_schemas")).isEqualTo(1);
    }

    @Test
    public void testCheckCanShowSchemasDenied()
    {
        testClient.addCheckResponse("catalog", TEST_CATALOG, "show_schemas", false);

        // Should throw an exception
        assertThatThrownBy(() -> accessControl.checkCanShowSchemas(securityContext, TEST_CATALOG))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    public void testCheckCanShowTables()
    {
        CatalogSchemaName schema = new CatalogSchemaName(TEST_CATALOG, TEST_SCHEMA);

        testClient.addCheckResponse("schema", TEST_CATALOG + "/" + TEST_SCHEMA, "show_tables", true);

        // Should not throw an exception
        accessControl.checkCanShowTables(securityContext, schema);

        assertThat(testClient.getCheckCallCount("schema", TEST_CATALOG + "/" + TEST_SCHEMA, "show_tables")).isEqualTo(1);
    }

    @Test
    public void testCheckCanShowTablesDenied()
    {
        CatalogSchemaName schema = new CatalogSchemaName(TEST_CATALOG, TEST_SCHEMA);

        testClient.addCheckResponse("schema", TEST_CATALOG + "/" + TEST_SCHEMA, "show_tables", false);

        // Should throw an exception
        assertThatThrownBy(() -> accessControl.checkCanShowTables(securityContext, schema))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    public void testGetRowFilters()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);

        // Set up row filters in the test client
        testClient.addRowFilter("filter1", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "department = 'HR'");
        testClient.addRowFilter("filter2", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "region = 'EMEA'");

        // Assign filters directly to user
        testClient.addUserToRowFilter(TEST_USER, "filter1");

        // Assign filter via role
        testClient.addUserToRole(TEST_USER, "analysts");
        testClient.addRoleToRowFilter("analysts", "filter2");

        // Test row filters are returned
        List<ViewExpression> filters = accessControl.getRowFilters(securityContext, table);

        assertThat(filters).hasSize(2);

        // Extract expressions for comparison - ViewExpression toString() isn't reliable for comparison
        Set<String> expressions = filters.stream()
                .map(ViewExpression::getExpression)
                .collect(java.util.stream.Collectors.toSet());

        assertThat(expressions).contains("department = 'HR'", "region = 'EMEA'");
        assertThat(testClient.getRowFilterCallCount(TEST_USER, TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE)).isEqualTo(1);
    }

    @Test
    public void testEmptyRowFilters()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);

        // No filters set up for this table/user

        // Test empty filter set is returned
        List<ViewExpression> filters = accessControl.getRowFilters(securityContext, table);

        assertThat(filters).isEmpty();
    }

    @Test
    public void testGetColumnMasks()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);

        // Create column schemas for testing using the builder pattern
        ColumnSchema ssnColumn = ColumnSchema.builder().setName("ssn").setType(VarcharType.VARCHAR).setHidden(false).build();
        ColumnSchema salaryColumn = ColumnSchema.builder().setName("salary").setType(DoubleType.DOUBLE).setHidden(false).build();
        ColumnSchema departmentColumn = ColumnSchema.builder().setName("department").setType(VarcharType.VARCHAR).setHidden(false).build();
        ColumnSchema creditCardColumn = ColumnSchema.builder().setName("credit_card").setType(VarcharType.VARCHAR).setHidden(false).build();

        List<ColumnSchema> columns = List.of(ssnColumn, salaryColumn, departmentColumn, creditCardColumn);

        // Set up column masks in the test client
        testClient.addColumnMask("mask1",
                TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE + "/" + ssnColumn.getName(),
                "regexp_replace(ssn, '(\\d{3})(\\d{2})(\\d{4})', 'XXX-XX-$3')");
        testClient.addUserToColumnMask(TEST_USER, "mask1");

        // Set up salary mask via role inheritance
        testClient.addColumnMask("mask2",
                TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE + "/" + salaryColumn.getName(),
                "salary * 0.1");
        testClient.addUserToRole(TEST_USER, "analysts");
        testClient.addRoleToColumnMask("analysts", "mask2");

        // Set up multiple masks for credit card column to test prioritization
        testClient.addColumnMask("complete_mask",
                TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE + "/" + creditCardColumn.getName(),
                "'XXXX-XXXX-XXXX-XXXX'");
        testClient.addColumnMask("partial_mask",
                TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE + "/" + creditCardColumn.getName(),
                "regexp_replace(credit_card, '(\\d{4})(\\d{4})(\\d{4})(\\d{4})', '$1-XXXX-XXXX-$4')");
        testClient.addUserToRole(TEST_USER, "pci_restricted");
        testClient.addRoleToColumnMask("pci_restricted", "partial_mask");
        testClient.addUserToColumnMask(TEST_USER, "complete_mask");

        // Test column masks are returned
        Map<ColumnSchema, ViewExpression> masks = accessControl.getColumnMasks(securityContext, table, columns);

        // SSN column should have a mask
        assertThat(masks).containsKey(ssnColumn);
        assertThat(masks.get(ssnColumn).getExpression())
                .isEqualTo("regexp_replace(ssn, '(\\d{3})(\\d{2})(\\d{4})', 'XXX-XX-$3')");

        // Salary column should have a mask via role inheritance
        assertThat(masks).containsKey(salaryColumn);
        assertThat(masks.get(salaryColumn).getExpression()).isEqualTo("salary * 0.1");

        // Department column should not have a mask
        assertThat(masks).doesNotContainKey(departmentColumn);

        // Credit card column should have a mask, but we don't assert which one since the
        // implementation doesn't guarantee which mask will be selected when multiple match
        assertThat(masks).containsKey(creditCardColumn);
        String creditCardMaskExpression = masks.get(creditCardColumn).getExpression();
        assertThat(creditCardMaskExpression).matches(s ->
                s.equals("'XXXX-XXXX-XXXX-XXXX'") ||
                        s.equals("regexp_replace(credit_card, '(\\d{4})(\\d{4})(\\d{4})(\\d{4})', '$1-XXXX-XXXX-$4')"));
    }

    @Test
    public void testNoColumnMaskAccess()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);

        // Create column schemas for testing using the builder pattern
        ColumnSchema secretColumn = ColumnSchema.builder().setName("secret").setType(VarcharType.VARCHAR).setHidden(false).build();

        // Set up access denial in the test client
        testClient.addColumnAccess(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, "secret", "viewer", false);

        // Test that column with no access returns a null mask
        Map<ColumnSchema, ViewExpression> masks = accessControl.getColumnMasks(securityContext, table, List.of(secretColumn));

        assertThat(masks).containsKey(secretColumn);
        assertThat(masks.get(secretColumn).getExpression()).isEqualTo("null");
    }

    @Test
    public void testCheckCanCreateTable()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName(TEST_CATALOG, TEST_SCHEMA, "new_table");

        testClient.addCheckResponse("schema", TEST_CATALOG + "/" + TEST_SCHEMA, "admin", true);

        // Should not throw an exception
        accessControl.checkCanCreateTable(securityContext, table, Map.of());

        assertThat(testClient.getCheckCallCount("schema", TEST_CATALOG + "/" + TEST_SCHEMA, "admin")).isEqualTo(1);
    }

    @Test
    public void testCheckCanCreateTableDenied()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName(TEST_CATALOG, TEST_SCHEMA, "new_table");

        testClient.addCheckResponse("schema", TEST_CATALOG + "/" + TEST_SCHEMA, "admin", false);

        // Should throw an exception
        assertThatThrownBy(() -> accessControl.checkCanCreateTable(securityContext, table, Map.of()))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    public void testCheckCanInsertIntoTable()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);

        testClient.addCheckResponse("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "insert", true);

        // Should not throw an exception
        accessControl.checkCanInsertIntoTable(securityContext, table);

        assertThat(testClient.getCheckCallCount("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "insert")).isEqualTo(1);
    }

    @Test
    public void testCheckCanInsertIntoTableDenied()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);

        testClient.addCheckResponse("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "insert", false);

        // Should throw an exception
        assertThatThrownBy(() -> accessControl.checkCanInsertIntoTable(securityContext, table))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    public void testCheckCanDeleteFromTable()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);

        testClient.addCheckResponse("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "delete", true);

        // Should not throw an exception
        accessControl.checkCanDeleteFromTable(securityContext, table);

        assertThat(testClient.getCheckCallCount("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "delete")).isEqualTo(1);
    }

    @Test
    public void testCheckCanDeleteFromTableDenied()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);

        testClient.addCheckResponse("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "delete", false);

        // Should throw an exception
        assertThatThrownBy(() -> accessControl.checkCanDeleteFromTable(securityContext, table))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    public void testCheckCanCreateView()
    {
        CatalogSchemaTableName view = new CatalogSchemaTableName(TEST_CATALOG, TEST_SCHEMA, "new_view");

        testClient.addCheckResponse("schema", TEST_CATALOG + "/" + TEST_SCHEMA, "admin", true);

        // Should not throw an exception
        accessControl.checkCanCreateView(securityContext, view);

        assertThat(testClient.getCheckCallCount("schema", TEST_CATALOG + "/" + TEST_SCHEMA, "admin")).isEqualTo(1);
    }

    @Test
    public void testCheckCanCreateViewDenied()
    {
        CatalogSchemaTableName view = new CatalogSchemaTableName(TEST_CATALOG, TEST_SCHEMA, "new_view");

        testClient.addCheckResponse("schema", TEST_CATALOG + "/" + TEST_SCHEMA, "admin", false);

        // Should throw an exception
        assertThatThrownBy(() -> accessControl.checkCanCreateView(securityContext, view))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    public void testComplexRowFilterScenario()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName(TEST_CATALOG, TEST_SCHEMA, "employees");

        // Set up multiple row filters through different paths
        testClient.addRowFilter("dept_filter", TEST_CATALOG + "/" + TEST_SCHEMA + "/employees", "department = 'SALES'");
        testClient.addRowFilter("region_filter", TEST_CATALOG + "/" + TEST_SCHEMA + "/employees", "region = 'NORTH'");
        testClient.addRowFilter("date_filter", TEST_CATALOG + "/" + TEST_SCHEMA + "/employees", "hire_date > DATE '2020-01-01'");

        // Create complex role hierarchy
        testClient.addUserToRole(TEST_USER, "junior_analyst");
        testClient.addUserToRole(TEST_USER, "us_employee");
        testClient.addRoleToRowFilter("junior_analyst", "dept_filter");
        testClient.addRoleToRowFilter("us_employee", "region_filter");
        testClient.addUserToRowFilter(TEST_USER, "date_filter");

        // Test that all filters are returned
        List<ViewExpression> filters = accessControl.getRowFilters(securityContext, table);

        assertThat(filters).hasSize(3);
        // Extract expressions for comparison
        Set<String> expressions = filters.stream()
                .map(ViewExpression::getExpression)
                .collect(java.util.stream.Collectors.toSet());
        assertThat(expressions).contains(
                "department = 'SALES'",
                "region = 'NORTH'",
                "hire_date > DATE '2020-01-01'");
    }

    @Test
    public void testCheckCanSetUser()
    {
        // Set up the test client to approve impersonation of "bob" by our test user
        testClient.addCheckResponse("user", "bob", "impersonate", true);

        // Should not throw an exception when the permission is granted
        accessControl.checkCanSetUser(identity.getPrincipal(), "bob");

        // Verify the impersonation check was performed
        assertThat(testClient.getCheckCallCount("user", "bob", "impersonate")).isEqualTo(1);
    }

    @Test
    public void testCheckCanSetUserDenied()
    {
        // Set up the test client to deny impersonation of "alice" by our test user
        testClient.addCheckResponse("user", "alice", "impersonate", false);

        // Should throw an exception when the permission is denied
        assertThatThrownBy(() -> accessControl.checkCanSetUser(identity.getPrincipal(), "alice"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("Cannot set user to alice");

        // Verify the impersonation check was performed
        assertThat(testClient.getCheckCallCount("user", "alice", "impersonate")).isEqualTo(1);
    }

    @Test
    public void testCheckCanImpersonateUser()
    {
        // Set up the test client to approve impersonation of "bob" by our test user
        testClient.addCheckResponse("user", "bob", "impersonate", true);

        // Should not throw an exception when the permission is granted
        accessControl.checkCanImpersonateUser(identity, "bob");

        // Verify the impersonation check was performed
        assertThat(testClient.getCheckCallCount("user", "bob", "impersonate")).isEqualTo(1);
    }

    @Test
    public void testCheckCanImpersonateUserDenied()
    {
        // Set up the test client to deny impersonation of "alice" by our test user
        testClient.addCheckResponse("user", "alice", "impersonate", false);

        // Should throw an exception when the permission is denied
        assertThatThrownBy(() -> accessControl.checkCanImpersonateUser(identity, "alice"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("User test-user cannot impersonate user alice");

        // Verify the impersonation check was performed
        assertThat(testClient.getCheckCallCount("user", "alice", "impersonate")).isEqualTo(1);
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
}
