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

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.openfga.model.OpenFgaContext;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test class validates row-level security and column masking functionality
 * using SQL queries with a simulated OpenFGA access control environment.
 */
public class TestOpenFgaSqlSecurity
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestOpenFgaSqlSecurity.class);
    private static final String TEST_CATALOG = "memory";
    private static final String TEST_SCHEMA = "default";
    private static final String TEST_TABLE = "employee";

    private Session adminSession;
    private Session hrUserSession;
    private Session financeUserSession;
    private Session regularUserSession;
    private TestingOpenFgaClient testingOpenFgaClient;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        log.info("Creating query runner with OpenFGA access control");
        // Create sessions for different user types using proper Identity objects
        adminSession = testSessionBuilder()
                .setCatalog(TEST_CATALOG)
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.ofUser("admin"))
                .build();

        hrUserSession = testSessionBuilder()
                .setCatalog(TEST_CATALOG)
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.ofUser("hr_user"))
                .build();

        financeUserSession = testSessionBuilder()
                .setCatalog(TEST_CATALOG)
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.ofUser("finance_user"))
                .build();

        regularUserSession = testSessionBuilder()
                .setCatalog(TEST_CATALOG)
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.ofUser("regular_user"))
                .build();

        // Create testing OpenFGA client with predefined permissions
        OpenFgaConfig config = new OpenFgaConfig()
                .setApiUrl("http://localhost:8080") // Not used with TestingOpenFgaClient
                .setStoreId("test_store")
                .setModelId("test_model")
                .setAllowPermissionManagementOperations(false);
        testingOpenFgaClient = new TestingOpenFgaClient(config);
        configureTestPermissions(testingOpenFgaClient);

        // Configure OpenFGA access control
        OpenFgaContext context = new OpenFgaContext(
                Identity.ofUser("admin"),
                "test_version",
                System.currentTimeMillis());

        TestingLifeCycleManagerWrapper lifeCycleManager = new TestingLifeCycleManagerWrapper(new TestingLifeCycleManager());

        // Create the access control
        OpenFgaAccessControl accessControl = new OpenFgaAccessControl(
                lifeCycleManager,
                testingOpenFgaClient,
                config,
                context);

        // Instead of trying to add the access control to an existing system, create a query runner
        // with the access control already configured
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(adminSession)
                .setSystemAccessControl(accessControl)
                .build();

        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog(TEST_CATALOG, "memory");
        log.info("Query runner created with OpenFGA access control");

        return queryRunner;
    }

    private void configureTestPermissions(TestingOpenFgaClient client)
    {
        // Grant general permissions for all users
        for (String user : List.of("admin", "hr_user", "finance_user", "regular_user")) {
            // System permissions
            client.addCheckResponse("system", "system", "execute_query", true);
            client.addCheckResponse("system", "system", "read", true);

            // Catalog permissions
            client.addCheckResponse("catalog", TEST_CATALOG, "viewer", true);
            client.addCheckResponse("catalog", TEST_CATALOG, "show_schemas", true);

            // Schema permissions
            client.addCheckResponse("schema", TEST_CATALOG + "/" + TEST_SCHEMA, "viewer", true);
            client.addCheckResponse("schema", TEST_CATALOG + "/" + TEST_SCHEMA, "show_tables", true);
        }

        // Admin has all permissions
        client.addCheckResponse("schema", TEST_CATALOG + "/" + TEST_SCHEMA, "admin", true);
        client.addCheckResponse("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "admin", true);
        client.addCheckResponse("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "select", true);
        client.addCheckResponse("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "insert", true);
        client.addCheckResponse("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "delete", true);

        // HR user can see HR department data only
        client.addCheckResponse("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "select", true);
        String hrFilterId = "hr_filter_" + UUID.randomUUID().toString().replace("-", "");
        client.addRowFilter(hrFilterId, TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "department = 'HR'");
        client.addUserToRowFilter("hr_user", hrFilterId);

        // Finance user can see Finance department data only with salary masked
        client.addCheckResponse("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "select", true);
        String financeFilterId = "finance_filter_" + UUID.randomUUID().toString().replace("-", "");
        client.addRowFilter(financeFilterId, TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "department = 'Finance'");
        client.addUserToRowFilter("finance_user", financeFilterId);

        // SSN masking for HR user (partial)
        String ssnHrMaskId = "ssn_hr_mask_" + UUID.randomUUID().toString().replace("-", "");
        client.addColumnMask(ssnHrMaskId,
                TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE + "/ssn",
                "regexp_replace(ssn, '(\\d{3})-(\\d{2})-(\\d{4})', 'XXX-XX-$3')");
        client.addUserToColumnMask("hr_user", ssnHrMaskId);

        // Salary masking for finance user (show actual values)
        // No mask, they see actual salaries

        // Regular user can see all departments but with masked sensitive data
        client.addCheckResponse("table", TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE, "select", true);

        // SSN complete masking for regular user
        String ssnRegularMaskId = "ssn_regular_mask_" + UUID.randomUUID().toString().replace("-", "");
        client.addColumnMask(ssnRegularMaskId,
                TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE + "/ssn",
                "'XXX-XX-XXXX'");
        client.addUserToColumnMask("regular_user", ssnRegularMaskId);

        // Salary masking for regular user
        String salaryMaskId = "salary_mask_" + UUID.randomUUID().toString().replace("-", "");
        client.addColumnMask(salaryMaskId,
                TEST_CATALOG + "/" + TEST_SCHEMA + "/" + TEST_TABLE + "/salary",
                "'Confidential'");
        client.addUserToColumnMask("regular_user", salaryMaskId);
    }

    @BeforeEach
    public void setUp()
            throws Exception
    {
        // Ensure queryRunner is initialized before using it
        log.info("Setting up test environment");

        // Create test table
        assertUpdate(adminSession, "CREATE TABLE " + TEST_TABLE + " (" +
                "id INTEGER, " +
                "name VARCHAR, " +
                "department VARCHAR, " +
                "salary INTEGER, " +
                "ssn VARCHAR)");

        // Insert test data
        assertUpdate(adminSession, "INSERT INTO " + TEST_TABLE + " VALUES" +
                "(1, 'Alice', 'HR', 100000, '123-45-6789')," +
                "(2, 'Bob', 'Engineering', 120000, '234-56-7890')," +
                "(3, 'Carol', 'HR', 90000, '345-67-8901')," +
                "(4, 'Dave', 'Finance', 130000, '456-78-9012')," +
                "(5, 'Eve', 'Engineering', 110000, '567-89-0123')," +
                "(6, 'Frank', 'Finance', 95000, '678-90-1234')", 6);
    }

    @AfterEach
    public void tearDown()
    {
        // Drop test table
        assertUpdate(adminSession, "DROP TABLE IF EXISTS " + TEST_TABLE);
    }

    @Test
    public void testAdminUserCanSeeAllData()
    {
        // Admin should see all rows with unmasked data
        MaterializedResult result = computeActual(adminSession, "SELECT * FROM " + TEST_TABLE + " ORDER BY id");
        assertThat(result.getRowCount()).as("Admin should see all rows").isEqualTo(6);

        // Verify all departments are visible
        List<String> departments = getMaterializedColumn(result, 2);
        assertThat(departments).containsExactly("HR", "Engineering", "HR", "Finance", "Engineering", "Finance");

        // Verify salary and SSN are not masked
        List<Object> salaries = getMaterializedColumn(result, 3);
        assertThat(salaries).contains(100000L, 120000L, 90000L, 130000L, 110000L, 95000L);

        List<String> ssns = getMaterializedColumn(result, 4);
        assertThat(ssns).contains("123-45-6789", "234-56-7890", "345-67-8901", "456-78-9012", "567-89-0123", "678-90-1234");
    }

    @Test
    public void testHrUserCanSeeOnlyHrData()
    {
        // HR user should only see rows from HR department
        MaterializedResult result = computeActual(hrUserSession, "SELECT * FROM " + TEST_TABLE + " ORDER BY id");
        assertThat(result.getRowCount()).as("HR user should only see HR department rows").isEqualTo(2);

        // Verify only HR department is visible
        List<String> departments = getMaterializedColumn(result, 2);
        assertThat(departments).containsOnly("HR");

        // Verify SSN is partially masked (last 4 digits visible)
        List<String> ssns = getMaterializedColumn(result, 4);
        for (String ssn : ssns) {
            assertThat(ssn).as("SSN should be partially masked as XXX-XX-####").matches("XXX-XX-\\d{4}");
        }
    }

    @Test
    public void testFinanceUserCanSeeOnlyFinanceData()
    {
        // Finance user should only see rows from Finance department
        MaterializedResult result = computeActual(financeUserSession, "SELECT * FROM " + TEST_TABLE + " ORDER BY id");
        assertThat(result.getRowCount()).as("Finance user should only see Finance department rows").isEqualTo(2);

        // Verify only Finance department is visible
        List<String> departments = getMaterializedColumn(result, 2);
        assertThat(departments).containsOnly("Finance");

        // Verify salary is not masked for finance users
        List<Object> salaries = getMaterializedColumn(result, 3);
        assertThat(salaries).containsExactly(130000L, 95000L);
    }

    @Test
    public void testRegularUserCanSeeAllDepartmentsWithMaskedData()
    {
        // Regular user should see all rows but with masked sensitive data
        MaterializedResult result = computeActual(regularUserSession, "SELECT * FROM " + TEST_TABLE + " ORDER BY id");
        assertThat(result.getRowCount()).as("Regular user should see all rows").isEqualTo(6);

        // Verify all departments are visible
        List<String> departments = getMaterializedColumn(result, 2);
        assertThat(departments).containsExactly("HR", "Engineering", "HR", "Finance", "Engineering", "Finance");

        // Verify salary is masked
        List<Object> salaries = getMaterializedColumn(result, 3);
        assertThat(salaries).containsOnly("Confidential");

        // Verify SSN is completely masked
        List<String> ssns = getMaterializedColumn(result, 4);
        assertThat(ssns).containsOnly("XXX-XX-XXXX");
    }

    @Test
    public void testFilterConditions()
    {
        // Test that row filters work with WHERE clauses

        // HR user with department filter
        MaterializedResult hrResult = computeActual(hrUserSession,
                "SELECT * FROM " + TEST_TABLE + " WHERE salary > 95000");
        assertThat(hrResult.getRowCount()).as("HR user should only see 1 row after filtering").isEqualTo(1);

        // Should only see Alice since Carol's salary is <= 95000
        List<String> hrNames = getMaterializedColumn(hrResult, 1);
        assertThat(hrNames).containsExactly("Alice");

        // Finance user with department filter
        MaterializedResult financeResult = computeActual(financeUserSession,
                "SELECT * FROM " + TEST_TABLE + " WHERE salary > 100000");
        assertThat(financeResult.getRowCount()).as("Finance user should only see 1 row after filtering").isEqualTo(1);

        // Should only see Dave since Frank's salary is <= 100000
        List<String> financeNames = getMaterializedColumn(financeResult, 1);
        assertThat(financeNames).containsExactly("Dave");
    }

    @Test
    public void testAggregatesWithRowFiltering()
    {
        // Test aggregates with row-level security

        // Admin sees sum of all salaries
        MaterializedResult adminResult = computeActual(adminSession,
                "SELECT sum(salary) FROM " + TEST_TABLE);
        Object adminSum = getMaterializedColumn(adminResult, 0).get(0);
        assertThat(adminSum).as("Admin should see sum of all salaries").isEqualTo(645000L);

        // HR user sees sum of only HR department salaries
        MaterializedResult hrResult = computeActual(hrUserSession,
                "SELECT sum(salary) FROM " + TEST_TABLE);
        Object hrSum = getMaterializedColumn(hrResult, 0).get(0);
        assertThat(hrSum).as("HR user should see sum of only HR salaries").isEqualTo(190000L);

        // Finance user sees sum of only Finance department salaries
        MaterializedResult financeResult = computeActual(financeUserSession,
                "SELECT sum(salary) FROM " + TEST_TABLE);
        Object financeSum = getMaterializedColumn(financeResult, 0).get(0);
        assertThat(financeSum).as("Finance user should see sum of only Finance salaries").isEqualTo(225000L);
    }

    @Test
    public void testAggregatesWithColumnMasking()
    {
        // Test that column masking affects aggregates correctly

        // Regular user has masked salary as 'Confidential' which can't be aggregated
        assertQueryFails(regularUserSession,
                "SELECT sum(salary) FROM " + TEST_TABLE,
                ".*Cannot apply 'sum' to arguments of type 'sum\\(varchar\\)'.*");

        // However, count should still work
        MaterializedResult regularCountResult = computeActual(regularUserSession,
                "SELECT count(*) FROM " + TEST_TABLE);
        Object regularCount = getMaterializedColumn(regularCountResult, 0).get(0);
        assertThat(regularCount).as("Regular user should be able to count all rows").isEqualTo(6L);
    }

    @Test
    public void testJoinsWithRowFiltering()
    {
        // Create a department table for join testing
        assertUpdate(adminSession, "CREATE TABLE department_info (" +
                "department VARCHAR, " +
                "location VARCHAR, " +
                "manager VARCHAR)");

        assertUpdate(adminSession, "INSERT INTO department_info VALUES" +
                "('HR', 'New York', 'John Smith')," +
                "('Engineering', 'San Francisco', 'Jane Doe')," +
                "('Finance', 'Chicago', 'Bob Johnson')", 3);

        try {
            // Add permissions
            for (String user : List.of("admin", "hr_user", "finance_user", "regular_user")) {
                testingOpenFgaClient.addCheckResponse("table",
                        TEST_CATALOG + "/" + TEST_SCHEMA + "/department_info",
                        "select",
                        true);
            }

            // Test join with row filtering active

            // HR user should only join with HR department
            MaterializedResult hrJoinResult = computeActual(hrUserSession,
                    "SELECT e.name, e.department, d.location, d.manager " +
                            "FROM " + TEST_TABLE + " e " +
                            "JOIN department_info d ON e.department = d.department " +
                            "ORDER BY e.id");

            assertThat(hrJoinResult.getRowCount()).as("HR user should only see joined rows for HR department").isEqualTo(2);

            List<String> hrDepartments = getMaterializedColumn(hrJoinResult, 1);
            assertThat(hrDepartments).containsOnly("HR");

            List<String> hrLocations = getMaterializedColumn(hrJoinResult, 2);
            assertThat(hrLocations).containsOnly("New York");

            // Finance user should only join with Finance department
            MaterializedResult financeJoinResult = computeActual(financeUserSession,
                    "SELECT e.name, e.department, d.location, d.manager " +
                            "FROM " + TEST_TABLE + " e " +
                            "JOIN department_info d ON e.department = d.department " +
                            "ORDER BY e.id");

            assertThat(financeJoinResult.getRowCount()).as("Finance user should only see joined rows for Finance department").isEqualTo(2);

            List<String> financeDepartments = getMaterializedColumn(financeJoinResult, 1);
            assertThat(financeDepartments).containsOnly("Finance");

            List<String> financeLocations = getMaterializedColumn(financeJoinResult, 2);
            assertThat(financeLocations).containsOnly("Chicago");

            // Regular user should see all departments joined
            MaterializedResult regularJoinResult = computeActual(regularUserSession,
                    "SELECT e.name, e.department, d.location, d.manager " +
                            "FROM " + TEST_TABLE + " e " +
                            "JOIN department_info d ON e.department = d.department " +
                            "ORDER BY e.id");

            assertThat(regularJoinResult.getRowCount()).as("Regular user should see all joined rows").isEqualTo(6);

            List<String> regularDepartments = getMaterializedColumn(regularJoinResult, 1);
            assertThat(regularDepartments).containsExactly("HR", "Engineering", "HR", "Finance", "Engineering", "Finance");
        }
        finally {
            // Clean up
            assertUpdate(adminSession, "DROP TABLE IF EXISTS department_info");
        }
    }

    /**
     * Helper method to get a specific column from a MaterializedResult as a list
     */
    private <T> List<T> getMaterializedColumn(MaterializedResult result, int columnIndex)
    {
        return result.getMaterializedRows().stream()
                .map(row -> (T) row.getField(columnIndex))
                .collect(Collectors.toList());
    }
}
