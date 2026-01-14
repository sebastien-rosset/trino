# Fine-Grained Access Control System for Apache Trino - Technical Design

This directory contains the complete technical design documentation for a fine-grained access control system for Apache Trino. The system integrates **OpenFGA** (ReBAC authorization decisions), **Trusted Attribute Injection** (foundational framework for injecting attributes from configurable sources), and **ReBAC patterns** to provide access control across all Trino operations and works seamlessly with **all Trino authentication methods** (JWT, OAuth2, Kerberos, LDAP, Certificate, Header, Password).

## Document Structure

### Core Architecture

- **[Architecture Overview](architecture-overview.md)** - High-level system architecture and design principles
- **[Trusted Attribute Injection](trusted-attribute-injection.md)** - General-purpose framework for injecting attributes from configurable sources (databases, APIs, authentication attributes, computed values)
- **[Authorization Model](authorization-model.md)** - Hybrid authorization model design and SQL construct mapping

### Technical Implementation

- **[Access Control Enforcement Requirements](access-control-enforcement-requirements.md)** - Analysis of enforcement functionality requirements for mutation operations
- **[ReBAC Patterns and Write Operations](rebac-patterns-and-write-operations.md)** - OpenFGA ReBAC patterns for write operations (multi-tenancy, RBAC, security clearance)
- **[Mutation Management Plugin](mutation-management-plugin.md)** - Dedicated mutation management plugin with StatementRewrite
- **[Policy Inheritance](policy-inheritance.md)** - Policy inheritance for views, materialized views, Common Table Expressions (CTEs), subqueries, and derived objects
- **[API Interfaces](api-interfaces.md)** - Key interfaces, classes, and API contracts
- **[Performance Optimization](performance-optimization.md)** - Multi-layer caching, batching, and performance strategies

### Project Planning

- **[Implementation Plan](implementation-plan.md)** - Phased implementation roadmap and success criteria
- **[Configuration Reference](configuration-reference.md)** - Configuration options and deployment guide

### Future Enhancements

- **[Policy Language Integration](policy-language-integration.md)** - Potential integration with policy languages (Cedar, OPA/Rego, or both) for policy definition (Phase 2/3 enhancement)

## Project Goals

1. **Fine-grained policy enforcement** - Row-level and column-level access control across all Trino connectors
2. **Cross-connector compatibility** - Works with all Trino connectors (S3, Druid, Postgres, OpenSearch, MySQL, Iceberg, and all others) without modifications
3. **Comprehensive SQL operation coverage** - Access control for all SQL operations including read operations (SELECT), write operations (INSERT, UPDATE, DELETE, TRUNCATE, MERGE), DDL operations (CREATE, ALTER, DROP), administrative operations (SHOW, DESCRIBE, EXPLAIN), and system operations (GRANT, REVOKE)
4. **Trusted Attribute Injection framework** - General-purpose capability for injecting attributes from configurable sources (databases, APIs, JWT claims, computed values) to support security, auditing, compliance, and data governance use cases
5. **Dual access control modes**: Two complementary approaches for security coverage
   - **SQL access control** - Traditional database-oriented policies for tables, views, columns, rows, catalogs, and schemas. Ideal for database administrators and SQL-centric workflows
   - **Entity access control** - Business entity-oriented policies that operate at the conceptual domain layer, automatically applying consistent access rules across multiple data stores where the same entity is persisted (e.g., Asset entity stored in PostgreSQL, Druid, S3, and OpenSearch)
   - See **[Access Control Architecture](access-control-architecture.md)** for detailed architectural explanation of these dual modes
6. **External PBAC integration** - Integration with OpenFGA and other Policy-Based Access Control systems
7. **Community acceptance** - High quality, performant implementation acceptable to Apache Trino community

## Key Design Decisions

### Universal Authentication Support

- **Complete Authentication Method Coverage**: Works with all Trino authentication methods (JWT, OAuth2, Kerberos, LDAP, Certificate, Header, Password)
- **Unified Identity Processing**: All authentication methods converge to Trino's standard `Identity` object for consistent policy evaluation
- **Multi-Source Attribute Resolution**: Supports groups from OAuth2, external LDAP lookups, Group Provider plugins, and custom attribute sources
- **Authentication Infrastructure Integration**: Works with existing authentication infrastructure without requiring changes

### Trusted Attribute Injection Framework (Foundational Component)

**Core Capability**: General-purpose framework for injecting trusted attributes into **all SQL operations** (data access, data modification, schema operations, administrative operations, system operations) from configurable sources, supporting security, auditing, compliance, and data governance use cases.

- **Universal Operation Support**: Injects attributes for SELECT, INSERT/UPDATE/DELETE/TRUNCATE/MERGE, CREATE/ALTER/DROP, SHOW/DESCRIBE/EXPLAIN, GRANT/REVOKE, and all other SQL operations
- **Attribute Provider SPI**: Pluggable providers for all authentication methods (OAuth2/JWT claims, LDAP attributes, Kerberos/AD lookups, certificate attributes, header values), plus database lookups, configuration files, REST APIs, computed values, and custom sources
- **Multi-Source Resolution**: Coordinated attribute resolution with session/query-level caching, batch processing, and fallback strategies
- **Query Transformation Engine**: StatementRewrite integration that modifies query AST to inject attributes before semantic analysis, making them available for all downstream policy enforcement
- **Configuration-Driven**: Declarative attribute definitions with configurable source mappings, access control policies, and caching rules

### Dual-Plugin Architecture (Built on Attribute Framework)

The access control plugins use the Trusted Attribute Injection framework:

- **SystemAccessControl Plugin**: Authorization decisions, data filtering (row filters, column masking), and metadata visibility using injected trusted attributes
- **StatementRewrite Plugin**: Statement modification for data operations using injected attributes - boundary constraints, policy enforcement, and validation
- **Shared Policy Engine**: OpenFGA and Cedar policy evaluation that references injected trusted attributes for consistent policy decisions

**Access Control Coverage by Operation Type:**

- **Data Access (SELECT)**: Authorization + Row Filtering + Column Masking
- **Data Modification (INSERT/UPDATE/DELETE/TRUNCATE/MERGE)**: Authorization + Statement Rewriting
- **Schema Operations (CREATE/ALTER/DROP)**: Authorization
- **Administrative Operations (SHOW/DESCRIBE/EXPLAIN)**: Authorization + Metadata Filtering
- **System Operations (GRANT/REVOKE)**: Authorization

### Hybrid Authorization Model

- **Metadata Layer**: Hierarchical permissions for discovery and visibility
- **Data Layer**: Flat, policy-driven access control for actual data operations
- **Comprehensive Coverage**: All SQL constructs (tables, views, materialized views, functions, procedures)

### Universal Policy Inheritance

- Automatic policy inheritance for all derived objects (views, materialized views, CTEs, subqueries, functions)
- Ensures logical data consistency regardless of access path or object type
- Union-based policy merging for multi-table derived objects

### Multi-Phase Performance Strategy

- **Phase 1**: Functional implementation with initial optimization
- **Phase 2**: Multi-layer caching and batch processing
- **Phase 3**: Predictive pre-authorization and policy compilation

## Architecture Principles

1. **Policy-Driven**: Administrators configure high-level policies; system generates SQL filters automatically
2. **Performance Optimization**: Caching, batching, and optimization for production workloads
3. **Abstraction Layer**: Support multiple integration approaches (config-based, API-driven, database-driven)
4. **Community Standards**: Follow Trino plugin patterns and coding standards for acceptance
5. **Modular Architecture**: Architecture supports future enhancements and additional authorization systems

---
