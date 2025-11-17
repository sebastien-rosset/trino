# OpenFGA Authorization Model for Trino

## Overview

This document defines how OpenFGA authorization models map to Trino's SQL operations through the [dual-interface architecture](architecture-overview.md). Each operation type requires specific OpenFGA configuration to support the access control coverage defined in the [README](README.md).

## Operation-Specific Authorization Model

### Data Access (SELECT): Authorization + Row Filtering + Column Masking

**SystemAccessControl Integration**: `checkCanSelectFromColumns()`, `getRowFilters()`, `getColumnMasks()`

```openfga
model
  schema 1.1

type user

type dataset
  relations
    define select: [user]
    define conditional_select: [user with row_filter_condition]

type data_field
  relations
    define select: [user]
    define mask: [user]

condition row_filter_condition(user_tenant: string, resource_tenant: string) {
  user_tenant == resource_tenant
}
```

**OpenFGA Tuple Examples**:
```json
// Basic select permission
{
  "user": "user:alice",
  "relation": "select",
  "object": "dataset:sales.orders"
}

// Row filtering with conditions
{
  "user": "user:alice",
  "relation": "conditional_select",
  "object": "dataset:sales.orders",
  "condition": {
    "name": "row_filter_condition",
    "context": {
      "user_tenant": "tenant_123",
      "resource_tenant": "tenant_123"
    }
  }
}

// Column masking
{
  "user": "user:alice",
  "relation": "mask",
  "object": "data_field:sales.orders.customer_email"
}
```

### Data Modification (INSERT/UPDATE/DELETE): Authorization + Statement Rewriting

**StatementRewrite Integration**: Attribute injection and validation
**SystemAccessControl Integration**: `checkCanInsertIntoTable()`, `checkCanUpdateTable()`, `checkCanDeleteFromTable()`

```openfga
type dataset
  relations
    define insert: [user]
    define update: [user]
    define delete: [user]
    define conditional_update: [user with ownership_condition]
    define conditional_delete: [user with ownership_condition]

condition ownership_condition(user_id: string, resource_owner: string) {
  user_id == resource_owner
}
```

**OpenFGA Tuple Examples**:
```json
// Basic modification permissions
{
  "user": "user:alice",
  "relation": "insert",
  "object": "dataset:sales.orders"
}

// Conditional update (user can only update their own records)
{
  "user": "user:alice",
  "relation": "conditional_update",
  "object": "dataset:sales.orders",
  "condition": {
    "name": "ownership_condition",
    "context": {
      "user_id": "alice",
      "resource_owner": "alice"
    }
  }
}
```

### Schema Operations (CREATE/ALTER/DROP): Authorization

**SystemAccessControl Integration**: `checkCanCreateTable()`, `checkCanDropTable()`, `checkCanAlterTable()`

```openfga
type catalog
  relations
    define admin: [user]
    define create_schema: [user] or admin

type schema
  relations
    define admin: [user] or admin from parent
    define create_table: [user] or admin
    define alter_table: [user] or admin
    define drop_table: [user] or admin
    define parent: [catalog]

type table
  relations
    define admin: [user] or admin from parent
    define alter: [user] or admin
    define drop: [user] or admin
    define parent: [schema]
```

**OpenFGA Tuple Examples**:
```json
// Schema creation permission
{
  "user": "user:alice",
  "relation": "create_schema",
  "object": "catalog:sales"
}

// Table creation with inheritance
{
  "user": "user:alice",
  "relation": "create_table",
  "object": "schema:sales/customer"
}

// Parent-child relationships for inheritance
{
  "user": "schema:sales/customer",
  "relation": "parent",
  "object": "catalog:sales"
}
```

### Administrative Operations (SHOW/DESCRIBE/EXPLAIN): Authorization + Metadata Filtering

**SystemAccessControl Integration**: `checkCanShowTables()`, `filterTables()`, `checkCanShowColumns()`

```openfga
type catalog
  relations
    define discover: [user] or admin

type schema
  relations
    define discover: [user] or admin from parent

type table
  relations
    define discover: [user] or admin from parent
    define show_create: [user] or admin from parent
```

**OpenFGA Tuple Examples**:
```json
// Table discovery permission
{
  "user": "user:alice",
  "relation": "discover",
  "object": "table:sales/customer/orders"
}

// Show table definition permission
{
  "user": "user:alice",
  "relation": "show_create",
  "object": "table:sales/customer/orders"
}
```

### System Operations (GRANT/REVOKE): Authorization

**SystemAccessControl Integration**: `checkCanGrantExecuteFunctionPrivilege()`, `checkCanRevokeTablePrivilege()`

```openfga
type role
  relations
    define admin: [user]
    define grant: [user] or admin
    define revoke: [user] or admin

type user_role
  relations
    define manage: [user]
```

**OpenFGA Tuple Examples**:
```json
// Grant permission management
{
  "user": "user:admin",
  "relation": "grant",
  "object": "role:data_analyst"
}
```

## Trino Integration Architecture

### SystemAccessControl Method Mapping

| Trino Method | OpenFGA Check | Purpose |
|--------------|---------------|---------|
| `checkCanSelectFromColumns()` | `check(user, select, dataset:table)` | Authorization decision |
| `getRowFilters()` | `check(user, conditional_select, dataset:table)` | Generate WHERE clause |
| `getColumnMasks()` | `check(user, mask, data_field:table.column)` | Generate column expressions |
| `checkCanInsertIntoTable()` | `check(user, insert, dataset:table)` | Authorization decision |
| `checkCanCreateTable()` | `check(user, create_table, schema:name)` | Authorization decision |
| `checkCanShowTables()` | `check(user, discover, schema:name)` | Authorization decision |
| `filterTables()` | `listObjects(user, discover, schema)` | Metadata filtering |

### StatementRewrite Integration

**Attribute Injection**: Uses OpenFGA context values to inject trusted attributes
**Validation**: Uses OpenFGA conditional checks to validate UPDATE/DELETE operations

```java
// Example: Inject tenant_id based on OpenFGA context
if (openFGAClient.check(user, "insert", "dataset:" + table)) {
    Map<String, Object> context = openFGAClient.getContext(user, table);
    String tenantId = (String) context.get("user_tenant");

    // Inject as trusted attribute
    statement = injectColumn(statement, "tenant_id", tenantId);
}
```

## Authorization Model Requirements

### Required OpenFGA Types

The authorization model must define these types to support all operation categories:

1. **`user`** - Subjects performing operations
2. **`catalog`** - Top-level Trino objects
3. **`schema`** - Database schemas within catalogs
4. **`table`** - Tables, views, materialized views
5. **`dataset`** - Logical data access unit (maps to tables)
6. **`data_field`** - Individual columns for masking
7. **`role`** - User roles for GRANT/REVOKE operations

### Required Relations Per Type

Each type must support relations corresponding to Trino operations:

- **catalog**: `admin`, `discover`, `create_schema`
- **schema**: `admin`, `discover`, `create_table`, `alter_table`, `drop_table`, `parent`
- **table**: `admin`, `discover`, `show_create`, `alter`, `drop`, `parent`
- **dataset**: `select`, `insert`, `update`, `delete`, `conditional_select`, `conditional_update`, `conditional_delete`
- **data_field**: `select`, `mask`
- **role**: `grant`, `revoke`

### Conditional Access Support

OpenFGA conditions enable:
- **Row filtering**: `conditional_select` with filter conditions
- **Ownership-based updates**: `conditional_update`/`conditional_delete` with ownership checks
- **Attribute-based access**: Any condition using trusted attribute context

## Configuration Integration

### OpenFGA Model Path Configuration

```properties
# /etc/trino/access-control.properties
access-control.name=openfga
openfga.authorization.model.path=/etc/trino/openfga-model.fga
```

### Model Loading and Validation

The plugin loads the OpenFGA model and validates it supports required types and relations for Trino operation coverage.

---

This authorization model provides the OpenFGA foundation for the [dual-interface architecture](architecture-overview.md), enabling comprehensive access control across all Trino SQL operations as specified in the [README](README.md).