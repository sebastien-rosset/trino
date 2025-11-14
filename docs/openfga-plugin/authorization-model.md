# Authorization Model Design

## Notation and Examples

This document uses multiple notation systems to explain concepts. All examples use placeholder names and are **per-project configuration examples**, not hardcoded values in the upstream Trino plugin.

### Notation Systems Used

#### OpenFGA Tuple Format

The Trino plugin sends authorization check requests to an OpenFGA server using these tuple structures. The OpenFGA server evaluates the checks against stored relationship tuples and returns authorization decisions that the plugin enforces within Trino.

**TupleKey Structure** (Reference: [OpenFGA API Protobuf](https://github.com/openfga/api/blob/main/openfga/v1/openfga.proto)):

```json
{
  "user": "user:alice",
  "relation": "select",
  "object": "table:sales/customer/orders"
}
```

**TupleKey Fields**:
- `user`: Subject (format: "type:id" e.g., "user:alice", "group:engineering#member")
- `relation`: Permission/relationship (e.g., "select", "discover", "admin")
- `object`: Resource (format: "type:id" e.g., "table:sales/customer/orders", "catalog:sales")

#### Hierarchical Permission Diagrams (Conceptual Visualization)

```text
catalog:example_catalog ← Object type:instance name
├── schema:schema1#permission1,permission2 ← Permissions after #
│   └── table:table1#permission3
```

#### Configuration Examples (Per-Project YAML/JSON)

Configuration files that administrators create for their specific deployments.

### Upstream vs Per-Project Boundaries

**Upstream Trino Plugin** (What gets merged into Trino repository):

- Authorization model framework and interfaces
- OpenFGA integration capabilities
- Permission types and inheritance rules
- Configuration parsing and validation
- Authorization request generation and OpenFGA client integration
- Integration with SystemAccessControl and StatementRewrite SPIs

**Per-Project Configuration** (What each organization configures):

- Specific catalog, schema, and table names
- OpenFGA authorization model definitions
- User identities and role assignments
- Attribute sources and mappings
- Organization-specific permission hierarchies
- Business-specific object types and relationships

**Key Distinction**: The plugin provides the **framework and capabilities** for fine-grained access control. Each organization then **configures** the plugin for their specific catalogs, schemas, users, and business requirements.

All examples in this document (sales, customer, orders, alice, bob) are **configuration examples** that administrators would customize for their environment.

## Hybrid Authorization Model

The plugin implements a **hybrid authorization model** that combines hierarchical metadata permissions with flat data-level access control for flexibility and performance. This model integrates with the **Trusted Attribute Injection framework** as the foundational component for attribute-aware policy evaluation.

## Model Architecture

The authorization model operates within the **dual-interface architecture** where SystemAccessControl handles authorization decisions and data filtering, while StatementRewrite (via Trusted Attribute Injection framework) handles query modification and attribute injection.

### Metadata Layer (Hierarchical)

**Purpose**: Controls discovery, visibility, and administrative operations on SQL objects (tables, views, materialized views, functions, procedures, roles, catalogs, schemas).

**Conceptual Hierarchy Example** (Per-project configuration):

```text
catalog:sales ← Example catalog name (configured per deployment)
├── schema:customer#discover,show_create,admin ← Example schema with permissions
│   ├── table:orders#discover,show_create ← Example table
│   ├── view:order_summary#discover,show_create ← Example view
│   ├── materialized_view:daily_orders#discover,show_create,refresh ← Example materialized view
│   ├── function:calculate_discount#discover,execute ← Example function
│   ├── procedure:cleanup_orders#discover,execute ← Example procedure
│   └── role:customer_analyst#discover,grant,revoke ← Example role
└── schema:products#discover,show_create ← Second example schema
    ├── table:inventory#discover,show_create ← Example tables in products schema
    └── table:catalog#discover,show_create
```

**Notation Explanation**:

- `object_type:instance_name` - Object type (catalog/schema/table) and specific name
- `#permission1,permission2` - List of permissions applicable to that object
- Tree structure shows containment hierarchy (catalog contains schemas, schemas contain tables)

**Key Point**: `sales`, `customer`, `orders` etc. are **example names**. In actual deployments, administrators configure their own catalog/schema/table names.

**Inheritance Rules**:

- Schema permissions inherit to contained objects (tables, views, functions)
- Users need `discover` on schema to see any contained objects
- Administrative permissions (`admin`) propagate down hierarchy
- Exception: Data access permissions do NOT inherit (handled by data layer)

### Data Layer (Flat + Policy-Driven)

**Purpose**: Controls actual data access with fine-grained row/column level policies using attributes injected by the Trusted Attribute Injection framework.

```json
// Direct table access
{
  "user": "user:alice",
  "relation": "select",
  "object": "dataset:sales.customer.orders"
}
{
  "user": "user:alice",
  "relation": "insert",
  "object": "dataset:sales.customer.orders"
}
{
  "user": "user:alice",
  "relation": "mask",
  "object": "data_field:sales.customer.orders.email"
}

// Policy-derived access (computed at runtime using configured trusted attributes)
{
  "user": "user:alice",
  "relation": "conditional_select",
  "object": "dataset:sales.customer.orders",
  "condition": {
    "name": "attribute_filter",
    "context": {
      "configured_attr_1": "resolved_value_1"
    }
  }
}
{
  "user": "user:alice",
  "relation": "conditional_select",
  "object": "dataset:sales.customer.daily_orders",
  "condition": {
    "name": "inherited_filter",
    "context": {
      "inherited_from_sources": true
    }
  }
}

// Example with multi-tenancy configuration:
{
  "user": "user:alice",
  "relation": "conditional_select",
  "object": "dataset:sales.customer.orders",
  "condition": {
    "name": "tenant_filter",
    "context": {
      "tenant_id": "tenant_123"
    }
  }
}
```

## SQL Construct Mapping

### Object Type Definitions

#### Metadata Objects

**Complete SQL Object Coverage**: Based on Trino SQL specification, the plugin provides access control for all SQL objects and constructs.

```yaml
catalog:
  permissions: [discover, show_create, create_schema, drop, admin]
  inheritance: grants discover to all schemas
  trino_operations: [CREATE CATALOG, DROP CATALOG, SHOW CATALOGS]

schema:
  permissions: [discover, show_create, create_table, create_view, create_materialized_view, create_function, alter, drop, admin]
  inheritance: grants discover to all contained objects
  trino_operations: [CREATE SCHEMA, ALTER SCHEMA, DROP SCHEMA, SHOW SCHEMAS, SHOW CREATE SCHEMA]

table:
  permissions: [discover, show_create, show_stats, show_columns, alter, drop, truncate]
  inheritance: none (data access handled separately by data layer)
  trino_operations: [CREATE TABLE, CREATE TABLE AS, ALTER TABLE, DROP TABLE, TRUNCATE, SHOW TABLES, SHOW CREATE TABLE, SHOW COLUMNS, SHOW STATS, DESCRIBE]

view:
  permissions: [discover, show_create, show_columns, alter, drop]
  inheritance: inherits data access policies from source tables
  trino_operations: [CREATE VIEW, ALTER VIEW, DROP VIEW, SHOW CREATE VIEW, SHOW COLUMNS, DESCRIBE]

materialized_view:
  permissions: [discover, show_create, show_columns, refresh, alter, drop]
  inheritance: inherits data access policies from source tables
  trino_operations: [CREATE MATERIALIZED VIEW, ALTER MATERIALIZED VIEW, DROP MATERIALIZED VIEW, REFRESH MATERIALIZED VIEW, SHOW CREATE MATERIALIZED VIEW]

function:
  permissions: [discover, show_create, execute, alter, drop]
  inheritance: none
  trino_operations: [CREATE FUNCTION, DROP FUNCTION, SHOW FUNCTIONS, SHOW CREATE FUNCTION]

procedure:
  permissions: [discover, show_create, execute, alter, drop]
  inheritance: none
  trino_operations: [CALL procedure]

role:
  permissions: [discover, grant, revoke, drop]
  inheritance: none
  trino_operations: [CREATE ROLE, DROP ROLE, GRANT role, REVOKE role, SHOW ROLE GRANTS]
```

#### Data Objects

```yaml
dataset:
  description: "Unified data access for table-like objects"
  covers: [table, view, materialized_view]
  permissions: [select, insert, update, delete, merge, truncate]
  integration: "Uses Trusted Attribute Injection framework for policy evaluation"

data_field:
  description: "Column-level access with masking"
  covers: [column in any table-like object]
  permissions: [select, mask, encrypt]
  integration: "Column access controlled using injected trusted attributes"

data_row:
  description: "Row-level access with filtering using configured trusted attributes"
  permissions: [conditional_access]
  attributes: [filter_expression, configured_trusted_attributes]
  integration: "Row filtering based on attributes injected by Trusted Attribute Injection framework"
```

## OpenFGA Tuple Structure

**Important**: These are examples of OpenFGA tuples that would be stored in each organization's OpenFGA deployment. The plugin provides the framework to generate authorization check requests using these tuple patterns, but the specific names and relationships are configured per deployment. The OpenFGA server stores and evaluates these tuples.

### OpenFGA Tuple Format Specification

**Official OpenFGA TupleKey Structure** (Reference: [OpenFGA API Protobuf](https://github.com/openfga/api/blob/main/openfga/v1/openfga.proto)):

```json
{
  "user": "user:alice",
  "relation": "admin",
  "object": "catalog:sales"
}
```

**API Field Specifications**:
- `user`: string (max 512 bytes, example: "user:anne")
- `relation`: string (pattern: `^[^:#@\\s]{1,50}$`, example: "reader")
- `object`: string (pattern: `^[^\\s]{2,256}$`, example: "document:2021-budget")

### Metadata Tuple Examples (Per-Project Configuration)

**Upstream Plugin Capability**: Framework to generate these tuples based on configuration
**Per-Project**: Administrators define their own catalog/schema/table names and user identities

```json
// Catalog access - Example configuration
{
  "user": "user:alice",
  "relation": "admin",
  "object": "catalog:sales"
}
// Meaning: User "alice" has "admin" permission on catalog "sales"

// Schema access with inheritance - Example configuration
{
  "user": "user:alice",
  "relation": "discover",
  "object": "schema:sales/customer"
}
{
  "user": "user:alice",
  "relation": "create_table",
  "object": "schema:sales/customer"
}
// Meaning: User "alice" can discover and create tables in schema "customer"

// Table metadata access - Example configuration
{
  "user": "user:alice",
  "relation": "show_create",
  "object": "table:sales/customer/orders"
}
{
  "user": "user:bob",
  "relation": "alter",
  "object": "table:sales/customer/orders"
}
// Meaning: Users have different permissions on the "orders" table

// Function access - Example configuration
{
  "user": "user:charlie",
  "relation": "execute",
  "object": "function:sales/analytics/calculate_discount"
}
// Meaning: User "charlie" can execute the "calculate_discount" function

// Role management - Example configuration
{
  "user": "user:admin",
  "relation": "grant",
  "object": "role:customer_analyst"
}
{
  "user": "user:admin",
  "relation": "revoke",
  "object": "role:customer_analyst"
}
// Meaning: User "admin" can grant/revoke the "customer_analyst" role
```

### Data Access Tuple Examples (Per-Project Configuration)

**Note**: Data access uses different object types (dataset, data_field) to distinguish from metadata access.

```json
// Basic data access - Example configuration
{
  "user": "user:alice",
  "relation": "select",
  "object": "dataset:sales.customer.orders"
}
{
  "user": "user:alice",
  "relation": "insert",
  "object": "dataset:sales.customer.orders"
}
{
  "user": "user:alice",
  "relation": "merge",
  "object": "dataset:sales.customer.orders"
}
// Meaning: User "alice" has data access permissions on the orders dataset

// Column-level access - Example configuration
{
  "user": "user:alice",
  "relation": "select",
  "object": "data_field:sales.customer.orders.customer_id"
}
{
  "user": "user:alice",
  "relation": "mask",
  "object": "data_field:sales.customer.orders.email"
}
{
  "user": "user:alice",
  "relation": "encrypt",
  "object": "data_field:sales.customer.orders.ssn"
}
// Meaning: User "alice" has different access levels to specific columns

// Row-level access (policy-driven using configured trusted attributes)
{
  "user": "user:alice",
  "relation": "conditional_select",
  "object": "dataset:sales.customer.orders",
  "condition": {
    "name": "tenant_filter",
    "context": {
      "configured_attr_1": "resolved_value_1",
      "configured_attr_2": "resolved_value_2"
    }
  }
}
// Meaning: User "alice" has filtered access based on configured attributes

// Real-world example with specific multi-tenancy configuration:
{
  "user": "user:alice",
  "relation": "conditional_select",
  "object": "dataset:sales.customer.orders",
  "condition": {
    "name": "multi_tenant_filter",
    "context": {
      "tenant_id": "tenant_123",
      "region": "US-WEST"
    }
  }
}
// Meaning: User "alice" can only see data for tenant_123 in US-WEST region
```

## Permission Types

### Metadata Permissions

| Permission | Scope | Description |
|------------|-------|-------------|
| `discover` | All objects | Can see object exists, basic metadata |
| `show_create` | Tables, views, functions, catalogs, schemas | Can view object definition/DDL |
| `show_stats` | Tables, materialized views | Can view object statistics |
| `show_columns` | Tables, views, materialized views | Can view column information |
| `admin` | All objects | Full administrative control |
| `create_schema` | Catalogs | Can create schemas in catalog |
| `create_table` | Schemas | Can create tables in schema |
| `create_view` | Schemas | Can create views in schema |
| `create_materialized_view` | Schemas | Can create materialized views in schema |
| `create_function` | Schemas | Can create functions in schema |
| `alter` | Tables, views, materialized views, functions, schemas | Can modify object structure |
| `drop` | All objects | Can delete objects |
| `execute` | Functions, procedures | Can execute callable objects |
| `refresh` | Materialized views | Can refresh materialized view data |
| `grant` | Roles | Can grant role to users |
| `revoke` | Roles | Can revoke role from users |
| `truncate` | Tables | Can truncate table (metadata operation) |

### Data Permissions

**Integration**: All data permissions use attributes provided by the Trusted Attribute Injection framework for policy evaluation.

| Permission | Scope | Description |
|------------|-------|-------------|
| `select` | Datasets, data_fields | Read access to data |
| `insert` | Datasets | Write new data |
| `update` | Datasets | Modify existing data |
| `delete` | Datasets | Remove data |
| `merge` | Datasets | MERGE operation (insert/update/delete combination) |
| `truncate` | Datasets | Remove all data (data operation) |
| `mask` | Data_fields | See masked version of column |
| `encrypt` | Data_fields | Access to encrypted column data |
| `conditional_select` | Datasets | Row-level filtered access using configured trusted attributes |

## Authorization Resolution Flow

### Metadata Authorization

**Architecture**: Plugin queries OpenFGA server for hierarchical permission checks. OpenFGA evaluates stored tuples and returns authorization decisions.

```java
public boolean canShowTable(Identity user, CatalogSchemaTableName table) {
    // Send authorization check requests to OpenFGA server for metadata visibility
    return checkPermission(user, "discover", "catalog:" + table.getCatalogName())
        && checkPermission(user, "discover", "schema:" + table.getCatalogName() + "/" + table.getSchemaTableName().getSchemaName())
        && checkPermission(user, "discover", "table:" + formatTableName(table));
}

private boolean checkPermission(Identity user, String relation, String object) {
    // Make HTTP/gRPC call to OpenFGA server: "Can user perform relation on object?"
    CheckRequest request = CheckRequest.builder()
        .user(formatUser(user))
        .relation(relation)
        .object(object)
        .build();
    return openFGAClient.check(request).isAllowed();
}
```

### Data Authorization with Row Filtering

**Architecture**: Plugin queries OpenFGA server for data access permissions, then applies returned authorization decisions as row filters within Trino.

```java
public List<ViewExpression> getRowFilters(SystemSecurityContext context, CatalogSchemaTableName table) {
    String datasetName = formatDatasetName(table);

    // Query OpenFGA server for basic select permission
    if (!checkPermission(context.getIdentity(), "select", "dataset:" + datasetName)) {
        return List.of(ViewExpression.builder()
            .expression("FALSE")  // Deny all access based on OpenFGA decision
            .build());
    }

    // Get applicable policies for conditional access using configured trusted attributes
    List<DataAccessPolicy> policies = getPoliciesForDataset(datasetName);
    Map<String, Object> trustedAttributes = getTrustedAttributes(context);

    return policies.stream()
        .map(policy -> applyPolicyAsFilter(policy, context, trustedAttributes))
        .filter(Objects::nonNull)
        .map(this::convertToViewExpression)
        .collect(toList());
}

private ViewExpression applyPolicyAsFilter(DataAccessPolicy policy,
                                          SystemSecurityContext context,
                                          Map<String, Object> trustedAttributes) {
    // Query OpenFGA server with contextual tuple for conditional access
    CheckRequest request = CheckRequest.builder()
        .user(formatUser(context.getIdentity()))
        .relation("conditional_select")
        .object(policy.getDatasetName())
        .context(trustedAttributes)
        .build();

    if (openFGAClient.check(request).isAllowed()) {
        return policy.toSQLFilter(); // Apply policy as WHERE clause
    }
    return null; // OpenFGA denied access
}
```

### Column Masking Authorization

**Architecture**: Plugin queries OpenFGA server for column-level permissions, then applies returned authorization decisions as column masks or access denial within Trino.

```java
public Map<ColumnSchema, ViewExpression> getColumnMasks(SystemSecurityContext context,
                                                       CatalogSchemaTableName table,
                                                       List<ColumnSchema> columns) {
    Map<String, Object> trustedAttributes = getTrustedAttributes(context);

    return columns.stream()
        .collect(toMap(
            identity(),
            column -> determineColumnAccess(context, table, column, trustedAttributes)
        ));
}

private ViewExpression determineColumnAccess(SystemSecurityContext context,
                                           CatalogSchemaTableName table,
                                           ColumnSchema column,
                                           Map<String, Object> trustedAttributes) {
    String fieldName = formatDataFieldName(table, column.getName());

    // Query OpenFGA server for column permissions in order of restrictiveness
    if (checkPermission(context.getIdentity(), "select", "data_field:" + fieldName, trustedAttributes)) {
        return null; // No masking required - OpenFGA allowed full access
    }
    else if (checkPermission(context.getIdentity(), "mask", "data_field:" + fieldName, trustedAttributes)) {
        return generateMaskExpression(column, trustedAttributes); // Apply mask - OpenFGA allowed masked access
    }
    else {
        return ViewExpression.builder()
            .expression("NULL")  // Deny access to column - OpenFGA denied all access
            .build();
    }
}

private boolean checkPermission(Identity user, String relation, String object, Map<String, Object> context) {
    // Make HTTP/gRPC call to OpenFGA server with contextual attributes
    CheckRequest request = CheckRequest.builder()
        .user(formatUser(user))
        .relation(relation)
        .object(object)
        .context(context)
        .build();
    return openFGAClient.check(request).isAllowed();
}
```

## Policy Inheritance Rules

### Materialized View Inheritance

Materialized views automatically inherit data access policies from their source tables:

```java
public class MaterializedViewPolicyResolver {

    public List<ViewExpression> resolveInheritedRowFilters(CatalogSchemaTableName materializedView) {
        // 1. Get source tables from view definition
        Set<CatalogSchemaTableName> sourceTables = extractSourceTables(materializedView);

        // 2. Get row filters for each source table
        Map<CatalogSchemaTableName, List<ViewExpression>> sourceFilters =
            sourceTables.stream()
                .collect(toMap(identity(), this::getTableRowFilters));

        // 3. Merge filters using intersection logic
        // User must have access to ALL source data to access materialized view row
        return mergeRowFilters(sourceFilters);
    }
}
```

### View Inheritance

Views inherit policies from their source tables with column mapping using configured trusted attributes:

```sql
-- Source table policies (using configured trusted attributes)
sales.orders: WHERE configured_attr_1 IN (resolved_value_1)
sales.customers: WHERE configured_attr_2 = resolved_value_2

-- View definition
CREATE VIEW sales.order_summary AS
SELECT o.order_id, o.amount, c.customer_name
FROM sales.orders o
JOIN sales.customers c ON o.customer_id = c.customer_id

-- Inherited policy (automatically generated using trusted attributes)
sales.order_summary: WHERE configured_attr_1 IN (resolved_value_1)
                        AND configured_attr_2 = resolved_value_2

-- Example with multi-tenancy configuration:
-- Source table policies:
-- sales.orders: WHERE region IN (user.authorized_regions)
-- sales.customers: WHERE tenant_id = user.tenant_id
--
-- Inherited policy:
-- sales.order_summary: WHERE region IN (user.authorized_regions)
--                        AND tenant_id = user.tenant_id
```

## Performance Optimizations

### Tuple Batching Strategy

```java
public Map<AuthorizationRequest, Boolean> batchAuthorize(Set<AuthorizationRequest> requests) {
    // Group by OpenFGA store for parallel processing
    Map<String, List<AuthorizationRequest>> byStore = groupByStore(requests);

    return byStore.entrySet().parallelStream()
        .flatMap(entry -> {
            // Batch up to 100 requests per OpenFGA API call
            List<List<AuthorizationRequest>> batches = partition(entry.getValue(), 100);
            return batches.stream()
                .flatMap(batch -> executeBatch(entry.getKey(), batch).entrySet().stream());
        })
        .collect(toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));
}
```

### Hierarchical Permission Caching

```java
public class HierarchicalPermissionCache {

    // Cache metadata permissions with hierarchical lookup
    public boolean checkMetadataPermission(Identity user, String permission, String objectPath) {
        // Check cache for exact permission
        Boolean cached = permissionCache.get(new PermissionKey(user, permission, objectPath));
        if (cached != null) {
            return cached;
        }

        // Check parent permissions for inheritance
        String parentPath = getParentPath(objectPath);
        if (parentPath != null && isInheritablePermission(permission)) {
            return checkMetadataPermission(user, permission, parentPath);
        }

        // Fall back to OpenFGA check
        return performOpenFGACheck(user, permission, objectPath);
    }
}
```

## Configuration Examples

### Authorization Model Configuration Framework

**What's in the Upstream Plugin** (Framework capabilities in Trino repository):

The plugin provides the **framework** to parse and enforce these configuration patterns. The actual configuration values are set by each organization.

**What's Per-Project Configuration** (Created by administrators for their deployment):

Organizations create configuration files defining their specific authorization model, object types, and permissions.

### OpenFGA DSL Configuration

**Official Standard**: Uses [OpenFGA Configuration Language](https://openfga.dev/docs/configuration-language) as defined in the [OpenFGA Grammar](https://github.com/openfga/language/blob/main/OpenFGALexer.g4).

**File**: `etc/openfga/authorization-model.fga`

**Integration**: Configuration integrates with Trusted Attribute Injection framework for attribute-aware policy evaluation.

```openfga
model
  schema 1.1

type catalog
  relations
    define admin: [user]
    define discover: [user] or admin
    define create_schema: [user] or admin

type schema
  relations
    define admin: [user] or admin from parent
    define discover: [user] or admin
    define create_table: [user] or admin
    define create_view: [user] or admin
    define create_materialized_view: [user] or admin
    define create_function: [user] or admin
    define parent: [catalog]

type table
  relations
    define admin: [user] or admin from parent
    define discover: [user] or admin from parent
    define show_create: [user] or admin from parent
    define show_stats: [user] or admin from parent
    define show_columns: [user] or admin from parent
    define alter: [user] or admin from parent
    define drop: [user] or admin from parent
    define truncate: [user] or admin from parent
    define parent: [schema]

type view
  relations
    define admin: [user] or admin from parent
    define discover: [user] or admin from parent
    define show_create: [user] or admin from parent
    define show_columns: [user] or admin from parent
    define alter: [user] or admin from parent
    define drop: [user] or admin from parent
    define parent: [schema]

type materialized_view
  relations
    define admin: [user] or admin from parent
    define discover: [user] or admin from parent
    define show_create: [user] or admin from parent
    define show_columns: [user] or admin from parent
    define refresh: [user] or admin from parent
    define alter: [user] or admin from parent
    define drop: [user] or admin from parent
    define parent: [schema]

type function
  relations
    define admin: [user] or admin from parent
    define discover: [user] or admin from parent
    define show_create: [user] or admin from parent
    define execute: [user] or admin from parent
    define alter: [user] or admin from parent
    define drop: [user] or admin from parent
    define parent: [schema]

type procedure
  relations
    define admin: [user] or admin from parent
    define discover: [user] or admin from parent
    define show_create: [user] or admin from parent
    define execute: [user] or admin from parent
    define alter: [user] or admin from parent
    define drop: [user] or admin from parent
    define parent: [schema]

type role
  relations
    define admin: [user] or admin from parent
    define discover: [user] or admin from parent
    define grant: [user] or admin from parent
    define revoke: [user] or admin from parent
    define drop: [user] or admin from parent
    define parent: [schema]

type dataset
  relations
    define select: [user]
    define insert: [user]
    define update: [user]
    define delete: [user]
    define merge: [user]
    define truncate: [user]
    define conditional_select: [user] with condition

type data_field
  relations
    define select: [user]
    define mask: [user]
    define encrypt: [user]
```

### Framework vs Configuration Boundary

**Upstream Framework Provides**:

- Authorization model parsing and validation
- Permission inheritance logic
- OpenFGA tuple generation patterns
- Configuration schema definitions
- Integration with Trino SPI

**Per-Project Configuration Specifies**:

- Actual catalog, schema, table names
- User identity mappings
- OpenFGA store configurations
- Specific permission assignments
- Organization-specific attribute sources

## Official OpenFGA References

This authorization model design is based on official OpenFGA specifications and standards:

### Core Documentation
- **OpenFGA Configuration Language**: https://openfga.dev/docs/configuration-language
- **OpenFGA Concepts**: https://openfga.dev/docs/concepts
- **Relationship Tuples**: https://openfga.dev/docs/concepts#what-is-a-relationship-tuple
- **Authorization Models**: https://openfga.dev/docs/concepts#what-is-an-authorization-model
- **Modeling Guide**: https://openfga.dev/docs/modeling

### API Specifications
- **OpenFGA API**: https://openfga.dev/api/service
- **Protocol Buffers**: https://github.com/openfga/api/blob/main/openfga/v1/openfga.proto
- **Language Grammar**: https://github.com/openfga/language/blob/main/OpenFGALexer.g4

### Implementation Resources
- **OpenFGA SDKs**: https://github.com/openfga
- **OpenFGA Server**: https://github.com/openfga/openfga
- **Community**: https://openfga.dev/community

### Standards Compliance
- **Zanzibar Paper**: https://research.google/pubs/pub48190/ (Google's authorization system)
- **Relationship-Based Access Control (ReBAC)**: Foundation for OpenFGA's approach

---
