# Configuration Reference

## Quick Start Configuration

### Basic OpenFGA Plugin Setup

#### Enable the Plugin

```properties
# /etc/trino/access-control.properties
access-control.name=openfga
```

#### OpenFGA Connection Settings

```properties
# OpenFGA server configuration
openfga.api.url=http://localhost:8080
openfga.store.id=01HXYZ123456789
openfga.authorization.model.id=01HXYZ789012345

# Authentication (if required)
openfga.api.key=your-api-key
openfga.auth.method=bearer_token
```

#### Authorization Model Configuration

```properties
# OpenFGA authorization model settings
openfga.authorization.model.path=/etc/trino/openfga-model.fga
openfga.authorization.model.reload.enabled=true
openfga.authorization.model.reload.interval=30m
```

## Complete Configuration Reference

### OpenFGA Connection Settings

```properties
# Required: OpenFGA server URL
openfga.api.url=http://localhost:8080

# Required: OpenFGA store identifier
openfga.store.id=01HXYZ123456789

# Required: Authorization model identifier
openfga.authorization.model.id=01HXYZ789012345

# Optional: API authentication
openfga.api.key=${ENV:OPENFGA_API_KEY}
openfga.auth.method=bearer_token  # bearer_token, basic_auth, none
openfga.auth.username=admin
openfga.auth.password=${ENV:OPENFGA_PASSWORD}

# Connection timeout settings
openfga.http.connect.timeout=2s
openfga.http.request.timeout=5s
openfga.http.read.timeout=10s

# Connection pool settings
openfga.http.connection.pool.size=100
openfga.http.connection.pool.max.per.route=20
openfga.http.connection.idle.timeout=5m
openfga.http.connection.keepalive=2m
```

### Performance and Caching Settings

```properties
# Cache configuration
openfga.cache.enabled=true

# Session-level cache (L1)
openfga.cache.session.enabled=true
openfga.cache.session.ttl=300s
openfga.cache.session.max.size=10000

# User-level cache (L2)
openfga.cache.user.enabled=true
openfga.cache.user.ttl=900s
openfga.cache.user.max.size=50000

# Policy compilation cache (L3)
openfga.cache.policy.enabled=true
openfga.cache.policy.ttl=3600s
openfga.cache.policy.max.size=1000

# Batch processing settings
openfga.batch.enabled=true
openfga.batch.size=100
openfga.batch.timeout=10ms
openfga.batch.parallelism=4

# Predictive pre-authorization
openfga.prediction.enabled=true
openfga.prediction.session.startup=true
openfga.prediction.pattern.analysis=true
```

### Authorization Model Settings

```properties
# OpenFGA authorization model configuration
openfga.authorization.model.path=/etc/trino/openfga-model.fga
openfga.authorization.model.reload.enabled=true
openfga.authorization.model.reload.interval=30m
openfga.authorization.model.validation.strict=true

# Model loading and caching
openfga.authorization.model.cache.enabled=true
openfga.authorization.model.cache.ttl=1h
openfga.authorization.model.preload.on.startup=true

# Context building configuration for conditional tuples
openfga.context.user.attributes=tenant_id,role,department,authorized_regions
openfga.context.resource.attributes=data_classification,sensitivity_level,tenant_id
openfga.context.environment.attributes=current_time,query_id,session_id

# Tuple generation optimization
openfga.tuple.generation.batch.size=50
openfga.tuple.generation.cache.enabled=true
openfga.tuple.generation.cache.ttl=5m
```

### Materialized View Configuration

```properties
# Materialized view policy inheritance
openfga.materialized.view.inheritance.enabled=true
openfga.materialized.view.inheritance.strategy=intersection  # intersection, union

# Inheritance behavior
openfga.materialized.view.inheritance.handle.aggregations=true
openfga.materialized.view.inheritance.handle.transformations=true
openfga.materialized.view.inheritance.preserve.column.masks=true

# Performance settings
openfga.materialized.view.inheritance.cache.enabled=true
openfga.materialized.view.inheritance.cache.ttl=1h
openfga.materialized.view.inheritance.lazy.evaluation=true
openfga.materialized.view.lineage.analysis.timeout=30s

# Fallback behavior
openfga.materialized.view.inheritance.on.lineage.failure=deny_access  # deny_access, allow_access
openfga.materialized.view.inheritance.on.complex.transformation=conservative  # conservative, permissive
```

### Reliability and Circuit Breaker Settings

```properties
# Circuit breaker configuration
openfga.circuit.breaker.enabled=true
openfga.circuit.breaker.failure.threshold=10
openfga.circuit.breaker.recovery.timeout=30s
openfga.circuit.breaker.health.check.interval=5s

# Retry policy
openfga.retry.enabled=true
openfga.retry.max.attempts=3
openfga.retry.backoff.strategy=exponential  # fixed, exponential
openfga.retry.backoff.initial.delay=100ms
openfga.retry.backoff.max.delay=2s
openfga.retry.backoff.multiplier=2.0

# Fallback strategies
openfga.fallback.strategy=cached  # deny_all, cached, role_based
openfga.fallback.cache.stale.tolerance=1h
openfga.fallback.role.based.config=/etc/trino/fallback-roles.properties
```

### Entity Integration Settings

```properties
# Entity access control
openfga.entity.integration.enabled=false
openfga.entity.integration.provider=config_based  # config_based, api_based, database_driven

# Configuration-based entity provider
openfga.entity.config.file=/etc/trino/entity-mapping.yaml
openfga.entity.config.validation.strict=true
openfga.entity.config.hot.reload=true

# API-based entity provider
openfga.entity.api.url=http://localhost:9090/entities
openfga.entity.api.auth.token=${ENV:ENTITY_API_TOKEN}
openfga.entity.api.cache.ttl=300s

# Database-driven entity provider
openfga.entity.database.url=jdbc:postgresql://localhost:5432/metadata
openfga.entity.database.username=trino_openfga
openfga.entity.database.password=${ENV:ENTITY_DB_PASSWORD}
openfga.entity.database.cache.ttl=600s
```

### Monitoring and Logging Settings

```properties
# Metrics and monitoring
openfga.metrics.enabled=true
openfga.metrics.jmx.enabled=true
openfga.metrics.prometheus.enabled=true

# Logging configuration
openfga.logging.level=INFO  # TRACE, DEBUG, INFO, WARN, ERROR
openfga.logging.authorization.decisions=false
openfga.logging.policy.evaluation=false
openfga.logging.performance.metrics=true
openfga.logging.cache.statistics=true

# Audit logging
openfga.audit.enabled=false
openfga.audit.log.file=/var/log/trino/openfga-audit.log
openfga.audit.log.format=json  # json, csv
openfga.audit.include.successful.access=false
openfga.audit.include.denied.access=true
```

## Configuration Files

### OpenFGA Authorization Model Structure

```openfga
# /etc/trino/openfga-model.fga
model
  schema 1.1

# Core SQL object types with hierarchical permissions
type catalog
  relations
    define admin: [user, role#member]
    define discover: [user, role#member] or admin
    define create_schema: [user, role#member] or admin

type schema
  relations
    define admin: [user, role#member] or admin from parent
    define discover: [user, role#member] or admin
    define create_table: [user, role#member] or admin
    define parent: [catalog]

type table
  relations
    define admin: [user, role#member] or admin from parent
    define select: [user, role#member] or admin
    define insert: [user, role#member] or admin
    define update: [user, role#member] or admin
    define delete: [user, role#member] or admin
    define parent: [schema]

# Data access types with conditional permissions
type dataset
  relations
    define select: [user, role#member]
    define conditional_select: [user, role#member with tenant_condition]

type data_field
  relations
    define select: [user, role#member]
    define mask: [user, role#member]

# Conditional access definitions
condition tenant_condition(user_tenant: string, resource_tenant: string) {
  user_tenant == resource_tenant
}
```

### Entity Mapping Configuration

```yaml
# /etc/trino/entity-mapping.yaml
entities:
  Asset:
    description: "IT Asset entity with cross-store mapping"
    tables:
      - catalog: postgres_prod
        schema: assets
        table: assets
        primary_key: asset_id

      - catalog: druid_analytics
        schema: assets
        table: asset_metrics
        primary_key: asset_id

      - catalog: opensearch_logs
        schema: assets
        table: asset_events
        primary_key: asset_id

    column_mappings:
      asset_id: asset_id
      asset_name: name
      owner: owner_id
      department: dept_code

    policies:
      tenant_isolation:
        expression: "asset.tenant_id = user.tenant_id"
        applies_to: [select, insert, update, delete]

      department_access:
        expression: "asset.department in user.departments"
        applies_to: [select]

      owner_access:
        expression: "asset.owner = user.user_id OR user.role = 'admin'"
        applies_to: [update, delete]

  SecurityAdvisory:
    description: "Security advisory information"
    tables:
      - catalog: postgres_prod
        schema: security
        table: advisories

    policies:
      security_clearance:
        expression: "advisory.classification_level <= user.security_clearance"
        applies_to: [select]
```

### OpenFGA Tuple Examples Configuration

```yaml
# /etc/trino/openfga-tuples.yaml - Example tuples for reference
# These are examples of the tuples that would be stored in OpenFGA
example_tuples:
  # User and role assignments
  user_roles:
    - user: "user:alice"
      relation: "member"
      object: "role:data_analyst"

    - user: "user:bob"
      relation: "member"
      object: "role:admin"

  # Catalog-level permissions
  catalog_access:
    - user: "role:data_analyst#member"
      relation: "discover"
      object: "catalog:sales"

    - user: "role:admin#member"
      relation: "admin"
      object: "catalog:sales"

  # Schema-level permissions with inheritance
  schema_access:
    - user: "role:data_analyst#member"
      relation: "discover"
      object: "schema:sales/customer"

  # Table-level permissions
  table_access:
    - user: "role:data_analyst#member"
      relation: "select"
      object: "table:sales/customer/orders"

  # Conditional data access with tenant isolation
  conditional_access:
    - user: "user:alice"
      relation: "conditional_select"
      object: "dataset:sales.customer.orders"
      condition:
        name: "tenant_condition"
        context:
          user_tenant: "tenant_123"
          resource_tenant: "tenant_123"

  # Column-level masking
  column_access:
    - user: "user:alice"
      relation: "mask"
      object: "data_field:sales.customer.orders.email"
```

## Environment-Specific Configurations

### Development Environment

```properties
# /etc/trino/access-control.properties (development)
access-control.name=openfga

# Local OpenFGA instance
openfga.api.url=http://localhost:8080
openfga.store.id=dev-store-123
openfga.authorization.model.id=dev-model-456

# Relaxed settings for development
openfga.cache.enabled=true
openfga.cache.session.ttl=60s
openfga.authorization.model.reload.enabled=true
openfga.authorization.model.reload.interval=10s
openfga.logging.level=DEBUG
openfga.circuit.breaker.enabled=false
```

### Production Environment

```properties
# /etc/trino/access-control.properties (production)
access-control.name=openfga

# Production OpenFGA cluster
openfga.api.url=https://openfga.example.com
openfga.store.id=${ENV:OPENFGA_STORE_ID}
openfga.authorization.model.id=${ENV:OPENFGA_MODEL_ID}
openfga.api.key=${ENV:OPENFGA_API_KEY}

# Optimized for performance
openfga.cache.enabled=true
openfga.cache.session.ttl=300s
openfga.cache.user.ttl=900s
openfga.batch.enabled=true
openfga.batch.size=100
openfga.prediction.enabled=true

# Production reliability
openfga.circuit.breaker.enabled=true
openfga.retry.enabled=true
openfga.fallback.strategy=cached

# Security and compliance
openfga.audit.enabled=true
openfga.logging.authorization.decisions=true
openfga.authorization.model.validation.strict=true
```

## Generic User Attribute Mapping Framework

### Schema-Driven Identity Provider Integration

```yaml
# Generic attribute mapping schema - organizations define their own attribute names
user_attribute_mapping_schema:
  # Isolation/Tenancy attribute (organization chooses name and source)
  {org_isolation_attribute}:  # Could be: tenant_id, account_id, organization_id, workspace_id, etc.
    source: "identity.extra_credentials.{org_credential_key}"
    type: "string"
    required: true
    description: "Primary isolation boundary for multi-tenant access control"

  # Authorization role attribute (organization chooses name and processing)
  {org_role_attribute}:  # Could be: role, primary_role, job_function, authorization_level, etc.
    source: "identity.groups"
    type: "string"
    transformer: "{org_role_transformer}"
    default: "{org_default_role}"
    description: "Primary authorization role for access control decisions"

  # Organizational unit attribute (organization chooses name and source)
  {org_unit_attribute}:  # Could be: department, division, business_unit, team, etc.
    source: "identity.extra_credentials.{org_unit_key}"
    type: "string"
    default: "{org_default_unit}"
    description: "Organizational unit for department-based access control"

  # Geographic/Location attribute (organization chooses name and structure)
  {org_location_attribute}:  # Could be: authorized_regions, locations, territories, offices, etc.
    source: "identity.extra_credentials.{org_location_key}"
    type: "set<string>"
    default: []
    description: "Geographic or location-based access boundaries"

  # Security/Classification attribute (organization chooses name and mapping)
  {org_security_attribute}:  # Could be: security_clearance, access_level, classification_tier, etc.
    source: "identity.extra_credentials.{org_security_key}"
    type: "integer"
    transformer: "{org_security_transformer}"
    default: {org_default_security_level}
    description: "Security clearance or classification level"

# Generic transformer definitions (organizations define their own)
attribute_transformers:
  {org_role_transformer}:  # Organization defines transformer name and logic
    type: "regex"
    pattern: "{org_role_pattern}"  # Organization's role extraction pattern
    group: 1
    description: "Extracts primary role from identity groups"

  {org_security_transformer}:  # Organization defines security level mapping
    type: "mapping"
    mappings: {org_security_mappings}  # Organization's security level mappings
    description: "Maps security clearance strings to numeric levels"

# Example configurations for different organizations:
example_attribute_mappings:
  # Healthcare organization
  healthcare_example:
    facility_id:
      source: "identity.extra_credentials.healthcare_facility"
      type: "string"
      required: true
    provider_type:
      source: "identity.groups"
      transformer: "extract_provider_role"
      default: "staff"
    authorized_locations:
      source: "identity.extra_credentials.facility_access"
      type: "set<string>"
      default: []

  # Financial services organization
  financial_example:
    trading_desk:
      source: "identity.extra_credentials.desk_assignment"
      type: "string"
      required: true
    risk_level_authorization:
      source: "identity.extra_credentials.risk_clearance"
      type: "integer"
      transformer: "parse_risk_level"
      default: 1
    authorized_markets:
      source: "identity.extra_credentials.market_access"
      type: "set<string>"
      default: []

  # Government/Defense organization
  government_example:
    security_clearance:
      source: "identity.extra_credentials.clearance_level"
      type: "integer"
      transformer: "parse_clearance_level"
      default: 1
    department_code:
      source: "identity.extra_credentials.dept_id"
      type: "string"
      required: true
    classification_access:
      source: "identity.extra_credentials.classification_level"
      type: "string"
      default: "unclassified"
```

## Troubleshooting Configuration

### Common Configuration Issues

#### OpenFGA Connection Failures

```properties
# Enable debug logging
openfga.logging.level=DEBUG

# Increase timeouts
openfga.http.connect.timeout=10s
openfga.http.request.timeout=30s

# Test connectivity
openfga.circuit.breaker.enabled=false
```

#### Performance Issues

```properties
# Optimize caching
openfga.cache.enabled=true
openfga.batch.enabled=true
openfga.prediction.enabled=true

# Increase cache sizes
openfga.cache.session.max.size=50000
openfga.cache.user.max.size=100000

# Enable performance metrics
openfga.metrics.enabled=true
openfga.logging.performance.metrics=true
```

#### Authorization Model Evaluation Errors

```properties
# Enable authorization debugging
openfga.authorization.model.validation.strict=false
openfga.logging.authorization.evaluation=true

# Relaxed inheritance
openfga.materialized.view.inheritance.on.complex.transformation=permissive
```

## Validation Commands

### Configuration Validation

```bash
# Validate plugin configuration
java -cp /usr/lib/trino/plugin/openfga/* \
  io.trino.plugin.openfga.tools.ConfigValidator \
  --config /etc/trino/access-control.properties

# Validate OpenFGA authorization model
java -cp /usr/lib/trino/plugin/openfga/* \
  io.trino.plugin.openfga.tools.AuthorizationModelValidator \
  --model /etc/trino/openfga-model.fga \
  --store-id $OPENFGA_STORE_ID

# Validate entity mappings
java -cp /usr/lib/trino/plugin/openfga/* \
  io.trino.plugin.openfga.tools.EntityValidator \
  --config /etc/trino/entity-mapping.yaml
```

### Health Checks

```bash
# Check OpenFGA connectivity
curl -H "Authorization: Bearer $OPENFGA_API_KEY" \
  "$OPENFGA_API_URL/stores/$STORE_ID/check" \
  -d '{"tuple_key":{"user":"user:health","relation":"check","object":"system:health"}}'

# Check Trino plugin status
curl http://localhost:8080/v1/info | jq '.plugins[] | select(.name == "openfga")'
```

---

This completes the technical design documentation for the OpenFGA plugin. The documentation is now split into focused, reviewable sections that your team can use for implementation planning and review.