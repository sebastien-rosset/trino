# Access Control Enforcement Requirements

## Overview

While the **[Trusted Attribute Injection](trusted-attribute-injection.md)** framework provides the foundational capability to inject attributes into queries, the **access control enforcement layer** must validate that mutation operations comply with security policies. This document analyzes the functionality requirements for policy enforcement across different levels of complexity.

## Enforcement Complexity Levels

### Level 1: Single-Table Attribute Validation

**Scenario**: One table with security attribute columns (tenantId, ownerId, departmentId, etc.)

#### INSERT Operations

- **Client Request**: `INSERT INTO orders (customer_id, amount) VALUES (123, 99.99)`
- **After Attribute Injection**: `INSERT INTO orders (customer_id, amount, tenant_id, created_by) VALUES (123, 99.99, 'tenant_abc', 'user123')`
- **Enforcement Required**: Validate that injected `tenant_id` matches user's JWT claim and policy allows insertion

#### UPDATE Operations

- **Client Request**: `UPDATE orders SET amount = 109.99 WHERE order_id = 456`
- **Enforcement Required**:
  1. **Existing Row Validation**: Retrieve current `tenant_id` of order_id=456
  2. **Policy Evaluation**: Check if user's JWT `tenant_id` can modify rows with that `tenant_id`
  3. **Boundary Enforcement**: Ensure UPDATE doesn't cross tenant boundaries unless policy permits

#### DELETE Operations

- **Client Request**: `DELETE FROM orders WHERE order_id = 456`
- **Enforcement Required**: Same validation pattern as UPDATE - check existing row ownership vs user authorization

### Level 2: Cross-Table Foreign Key Validation

**Scenario**: Multiple tables with referential integrity and cross-table security constraints

#### Example Schema

```sql
-- Asset table (primary entity)
CREATE TABLE assets (
  asset_id BIGINT PRIMARY KEY,
  tenant_id VARCHAR(50),
  asset_name VARCHAR(200),
  owner_department VARCHAR(50)
);

-- AssetVulnerability table (references Asset)
CREATE TABLE asset_vulnerabilities (
  vulnerability_id BIGINT PRIMARY KEY,
  asset_id BIGINT REFERENCES assets(asset_id),
  severity_level INTEGER,
  discovered_by VARCHAR(100)
);
```

#### Cross-Table INSERT Enforcement

- **Client Request**: `INSERT INTO asset_vulnerabilities (asset_id, severity_level) VALUES (789, 3)`
- **Enforcement Required**:
  1. **Foreign Key Lookup**: Query `SELECT tenant_id, owner_department FROM assets WHERE asset_id = 789`
  2. **Cross-Table Policy Evaluation**: Check if user can create vulnerability records for assets in that tenant/department
  3. **Attribute Consistency**: Ensure vulnerability record attributes align with referenced asset attributes

#### Enforcement Questions for Cross-Table Operations

1. **Lookup Performance**: How do we validate foreign key constraints without impacting query performance?
2. **Policy Scope**: Should policies be defined at the table level, entity level, or relationship level?
3. **Transactional Consistency**: What happens if foreign key lookup fails or returns multiple candidates?
4. **Circular Dependencies**: How do we handle complex entity relationships without infinite lookup chains?

### Level 3: Multi-Table Transaction Validation

**Scenario**: Single operation affects multiple tables through JOINs, subqueries, or CTEs

#### Complex UPDATE with JOIN

```sql
UPDATE asset_vulnerabilities av
SET severity_level = 5
FROM assets a
WHERE av.asset_id = a.asset_id
  AND a.asset_name LIKE 'prod-%'
  AND av.severity_level < 3
```

**Enforcement Challenges**:

1. **Multi-Table Policy Evaluation**: Policies must be evaluated across both `asset_vulnerabilities` and `assets` tables
2. **Row-Level Filtering**: Ensure user can only update vulnerabilities for assets they have access to
3. **Policy Intersection**: Handle cases where user has different permission levels on different tables

### Level 4: Dynamic Policy Evaluation

**Scenario**: Policies that depend on computed values, external lookups, or temporal conditions

#### Time-Based Access Control

```openfga
model
  schema 1.1

type user

type role
  relations
    define member: [user]

type table
  relations
    define insert: [role#member with trading_hours_condition]

condition trading_hours_condition(current_time: timestamp, market_open: timestamp, market_close: timestamp) {
  current_time >= market_open && current_time <= market_close
}
```

**Example Authorization Tuple:**

```json
{
  "user": "role:trader#member",
  "relation": "insert",
  "object": "table:financial_transactions",
  "condition": {
    "name": "trading_hours_condition",
    "context": {
      "current_time": "2024-01-15T14:30:00Z",
      "market_open": "2024-01-15T09:30:00Z",
      "market_close": "2024-01-15T16:00:00Z"
    }
  }
}
```

#### Conditional Cross-Entity Access

```openfga
model
  schema 1.1

type user

type role
  relations
    define member: [user]

type department
  relations
    define manager: [role#member]
    define member: [user]

type entity_asset
  relations
    define update: [user] or manager from owner_department
    define owner: [user]
    define owner_department: [department]
```

**Example Authorization Tuples:**

```json
// Direct ownership access
{
  "user": "user:alice",
  "relation": "update",
  "object": "entity_asset:server_001"
}

// Department manager access
{
  "user": "role:department_manager#member",
  "relation": "manager",
  "object": "department:infrastructure"
}

// Asset ownership relationships
{
  "user": "user:alice",
  "relation": "owner",
  "object": "entity_asset:server_001"
}

{
  "user": "department:infrastructure",
  "relation": "owner_department",
  "object": "entity_asset:server_001"
}
```

## Required Enforcement Capabilities

### Pre-Operation Validation

**INSERT Validation Requirements**:

- Validate all client-provided values against user attributes and policies
- Ensure injected trusted attributes are consistent with policy constraints
- Prevent insertion of data that violates cross-table security boundaries

**UPDATE/DELETE Validation Requirements**:

- Retrieve existing row attributes for policy evaluation
- Validate that user has permission to modify/delete specific rows
- Handle bulk operations (UPDATE/DELETE affecting multiple rows)
- Ensure modifications don't create policy violations

### Cross-Table Constraint Enforcement

**Foreign Key Security Validation**:

- Validate that foreign key references point to entities user can access
- Ensure referential integrity respects security boundaries
- Handle cascading security implications (if user can't access parent, can they access child?)

**Join Operation Security**:

- Apply row-level filtering to all tables involved in JOIN operations
- Ensure JOIN conditions don't leak information across security boundaries
- Validate that user has appropriate permissions on all joined entities

### Policy Evaluation Engine Requirements

**Policy Types to Support**:

- **Direct Attribute Matching**: `user.tenant_id == resource.tenant_id`
- **Role-Based Access**: `user.role in ["admin", "manager"]`
- **Hierarchical Access**: `user.security_clearance >= resource.classification_level`
- **Temporal Access**: `context.time between resource.valid_from and resource.valid_to`
- **Computed Access**: `user.authorized_regions contains resource.data_region`
- **Cross-Entity Access**: `user has_access_to resource.parent_entity`

**Evaluation Context Requirements**:

- User attributes (from JWT claims, LDAP, database lookup)
- Resource attributes (from target tables, related entities)
- Environmental context (time, request metadata, session state)
- Operation context (INSERT/UPDATE/DELETE, affected columns, transaction scope)

### Performance and Scalability Requirements

**Query Performance Impact**:

- Enforcement overhead must be <50ms for simple operations
- Bulk operations (affecting 1000+ rows) must complete in reasonable time
- Foreign key lookups must be optimized (caching, batching, indexing)

**Caching Requirements**:

- Cache policy evaluation results for repeated access patterns
- Cache foreign key attribute lookups to avoid repeated database queries
- Invalidate cached results when underlying data or policies change

**Scalability Requirements**:

- Support enforcement across all Trino connectors without connector modifications
- Handle high-concurrency mutation workloads (100+ concurrent operations)
- Scale to large datasets (millions of rows) without performance degradation

## Open Questions and Design Considerations

### Policy Definition Granularity

- Should policies be defined at table, column, row, or entity level?
- How do we handle policy conflicts between different granularity levels?
- What's the precedence order when multiple policies apply?

### Enforcement Point Architecture

- Should enforcement happen at query analysis time or execution time?
- How do we handle enforcement for complex queries with subqueries and CTEs?
- What's the interaction between enforcement and Trino's query optimization?

### Error Handling and User Experience

- How detailed should error messages be without revealing security information?
- Should partial failures in bulk operations fail entire transaction or continue with allowed rows?
- How do we provide meaningful feedback for policy violations?

### Transaction and Consistency Guarantees

- How do we handle enforcement in multi-statement transactions?
- What consistency guarantees do we provide for cross-table constraints?
- How do we handle race conditions in concurrent modification scenarios?

### Integration with Existing Security Models

- How does this enforcement integrate with existing database-level security?
- What's the relationship with Trino's existing access control mechanisms?
- How do we handle mixed security models (some tables with enforcement, others without)?

## Next Steps for Requirements Analysis

### Iteration 1: Core Enforcement Patterns

1. Define standard enforcement patterns for common scenarios
2. Analyze performance implications of different enforcement approaches
3. Design policy evaluation framework architecture

### Iteration 2: Complex Scenario Analysis

1. Deep-dive into multi-table constraint validation
2. Define cross-entity policy evaluation requirements
3. Analyze transaction consistency requirements

### Iteration 3: Integration and Performance

1. Define integration points with existing Trino security mechanisms
2. Performance testing framework for enforcement overhead
3. Caching and optimization strategy definition

---

This requirements analysis will guide the technical design of the access control enforcement layer, ensuring we build a solution that handles the full complexity of security requirements while maintaining performance and usability.