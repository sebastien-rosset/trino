# Trino OpenFGA Plugin Implementation Tasks

This document outlines the implementation tasks for developing the Trino OpenFGA plugin from scratch.

## Initial Setup and Project Structure

This section covers the foundational setup required to create a new Trino plugin. It includes establishing the directory structure, configuring build tools, adding dependencies, and creating the essential entry points that allow the plugin to integrate with Trino's plugin system.

- [x] Create base plugin directory structure in `trino/plugin/trino-openfga`
- [x] Configure Maven build (pom.xml) with proper dependencies
- [x] Add OpenFGA Java SDK v0.8.1 dependency
- [x] Create plugin entry point (OpenFgaAccessControlPlugin)
- [x] Create initial README.md with plugin description

## Core Plugin Architecture

The core architecture section establishes the main components that enable the plugin to integrate with Trino while communicating with OpenFGA. This includes configuration management, dependency injection setup, and context passing mechanisms that allow authorization decisions to be made based on the current query context.

- [x] Design plugin architecture for OpenFGA integration
- [x] Create basic plugin configuration class (OpenFgaConfig)
- [x] Implement core annotations (@ForOpenFga)
- [x] Create lifecycle management wrapper
- [x] Set up dependency injection module (OpenFgaAccessControlModule)
- [x] Create OpenFgaContext class for passing contextual information

## OpenFGA Client Implementation

The OpenFGA client implementation forms the communication layer between Trino and the OpenFGA authorization service. This section covers the client's functionality for checking permissions, retrieving access control policies, handling errors, and ensuring reliable network communication with the OpenFGA API.

- [x] Create OpenFgaClient class for SDK integration
- [x] Implement configuration loading for API URL and authentication
- [x] Fix compatibility issues with the OpenFGA Java SDK API structure
- [x] Implement basic access control methods (`check()`, `checkCatalogAccess()`, etc.)
- [x] Complete `getApplicableRowFilters()` to retrieve row-level security filters from OpenFGA
- [x] Complete `getApplicableColumnMask()` to retrieve column masking expressions
- [x] Add filter and mask expression retrieval from configuration
- [x] Add robust error handling for OpenFGA API calls
- [x] Add retry logic for failed requests with exponential backoff
- [x] Add connection management to handle network disruptions
- [ ] Add support for HTTP proxy between OpenFGA client and OpenFGA server
- [ ] Implement support for contextual tuples to store filter expressions:
    - The current implementation uses configuration properties to store filter expressions
      since OpenFGA SDK v0.8.1 doesn't support retrieving conditions directly from tuples
    - Need to implement a proper mechanism to store and retrieve filter/mask expressions using
      OpenFGA's contextual tuples feature
    - This involves:
        1. Extending the client to write contextual tuple data when storing filter expressions
        2. Modifying `getFilterExpressionForId()` and `getMaskExpressionForId()` to read from
           contextual tuples rather than configuration
        3. Creating helper methods for encoding/decoding SQL expressions in tuple metadata
        4. Adding migration support for existing filter expressions
- [ ] Implement batch operations support:
    - Use `batchCheck` API for performance optimization when multiple permission checks are needed
    - Add batching capabilities to group related permission checks in a single API call
    - Implement intelligent batching strategies to minimize API calls
- [ ] Support for Enhanced Relation Types:
    - Implement support for computed sets in OpenFGA relationships
    - Add support for user sets for group-based permissions
    - Leverage direct, computed and indirect relations for complex authorization scenarios

## Access Control Implementation

This section implements Trino's SystemAccessControl interface to enforce authorization decisions from OpenFGA. It covers the core security methods that control access to catalogs, schemas, tables, and system information, ensuring that all security decisions are delegated to the OpenFGA authorization system.

- [x] Implement SystemAccessControl interface in OpenFgaAccessControl
- [x] Create OpenFgaAccessControlFactory for instantiating access control
- [x] Implement basic identity checks (impersonation, user setting)
- [x] Implement query execution permission checks
- [x] Implement system information access control methods
- [x] Implement session property permission checks

### Catalog, Schema and Table Permissions

This subsection focuses on implementing access control methods for Trino's metadata layer. It includes methods to check permissions for viewing, creating, modifying, and deleting catalogs, schemas, and tables, as well as filtering these objects based on the user's access rights.

- [x] Implement catalog access control methods
- [x] Implement catalog filtering methods
- [x] Implement schema access control methods (create, drop, rename)
- [x] Implement schema filtering methods
- [x] Implement table access control methods (create, drop, rename, etc.)
- [x] Implement table filtering methods
- [x] Implement column access control methods
- [x] Implement column filtering methods

### Advanced Data Access Control

This subsection implements fine-grained access control at the data level. It covers row-level security that filters which rows a user can see, and column masking that transforms sensitive data before it's returned to users, with both mechanisms driven by OpenFGA authorization decisions.

- [x] Implement row-level security through `getRowFilters()`
- [x] Implement column masking through `getColumnMasks()`
- [x] Connect row and column security to OpenFGA tuples
- [x] Document security behavior (e.g., empty list means allow-all)

### Role and Permission Management

This subsection handles the mapping between Trino's role-based access control and OpenFGA's relationship model. It includes implementing methods for role management operations, privilege grants and revocations, and ensuring these operations are properly synchronized with the OpenFGA authorization model.

- [x] Implement role management methods
- [x] Implement table privilege methods
- [x] Add support for `allowPermissionManagementOperations` configuration
- [ ] Add methods to create OpenFGA relationship tuples for GRANT operations
- [ ] Add methods to delete OpenFGA relationship tuples for REVOKE operations
- [ ] Map Trino role operations to OpenFGA roles
- [ ] Implement role hierarchy support using OpenFGA's relation modeling
- [ ] Add support for dynamic role resolution based on contextual information

## Configuration System

The configuration system provides the settings needed to connect and integrate with OpenFGA. This section covers API connectivity, authentication options, performance tuning parameters, and the configuration of security policy expressions used for row filtering and column masking.

- [x] Design configuration properties structure
- [x] Implement core API configuration (URL, store ID, model ID)
- [x] Implement authentication configuration (API token)
- [x] Add OAuth configuration options
- [x] Add logging configuration options
- [x] Add filter expressions configuration (`openfga.filter-expressions`)
- [x] Add column mask expressions configuration (`openfga.mask-expressions`)
- [ ] Add retry configuration options
- [ ] Add connection timeout configuration options
- [ ] Add SDK version compatibility configuration options
- [ ] Add OpenTelemetry integration configuration

## Model Management

The model management section enables creation and management of OpenFGA authorization models specific to Trino's needs. It includes functionality for loading default models, supporting custom models, versioning, and providing administrative interfaces for model management, ensuring the authorization models accurately reflect Trino's security requirements.

- [x] Implement model creation if it doesn't exist
- [x] Add model versioning support
- [x] Add automatic loading of default model from `default-model.yaml`
- [x] Support custom models specified via config option
- [x] Use configuration-based approach for model management and versioning
- [x] Implement model verification to validate custom models
- [ ] Extend the default model with Trino-specific object types and relations
- [ ] Design and implement standard authorization models for common Trino use cases

### Model Customization Considerations

When implementing model customization, there are important constraints to consider:

1. **Tight coupling with plugin assumptions**: The current plugin code makes specific assumptions about object types (`catalog`, `schema`, `table`, `column`) and relations (`view`, `select`, `insert`, etc.). Custom models must maintain these core types and relations for the plugin to function correctly.

2. **Mapping layer needed**: For truly flexible model customization, the plugin needs a mapping layer that connects custom model concepts to Trino operations. Without this, custom models must follow the exact structure expected by the plugin.

3. **Validation requirements**: Custom models should be validated not just for syntax correctness, but for semantic compatibility with the plugin's expectations. The validation should verify that all required object types and relations exist.

4. **Documentation importance**: Clear documentation must be provided about the minimum required components in any custom model, including the specific object types, relations, and permission flow patterns that the plugin expects.

5. **Versioning challenges**: As the plugin evolves, it may require changes to the authorization model structure. A strategy is needed to handle version compatibility between plugin versions and authorization model versions.

These considerations should be addressed when implementing the model management functionality to ensure that custom models can be properly interpreted and used by the plugin.

## Performance & Reliability

This section focuses on optimizing the plugin for performance, scalability, and reliability in production environments. It includes implementing caching to reduce API calls, connection management strategies, performance monitoring, and techniques for graceful degradation when the authorization service experiences issues.

- [ ] Implement caching layer for OpenFGA authorization checks
- [ ] Add connection pooling for OpenFGA client
- [ ] Consider batch authorization checks where possible
- [ ] Add metrics/telemetry for authorization decisions
- [ ] Integrate with OpenTelemetry for observability:
    - Add metrics for authorization decision latencies
    - Create tracing for access control decisions
    - Set up proper logging with correlation IDs between Trino and OpenFGA calls
- [ ] Implement intelligent prefetching of permissions for common access patterns
- [ ] Add circuit-breaking patterns for OpenFGA service degradation scenarios

## Advanced OpenFGA Integration

This section leverages advanced OpenFGA features to provide deeper integration capabilities. It includes visualization tools for understanding authorization decisions, assertion functionality for testing policies, and enhanced user management operations that provide greater visibility and control over the authorization system.

- [ ] Implement the expand API integration:
    - Add capability to visualize authorization policy evaluation paths
    - Create debug tooling for administrators to understand access decisions
    - Enable permission path analysis for security auditing
- [ ] Add assertion support for testing authorization policies:
    - Implement OpenFGA assertions for validating access control rules
    - Create a testing framework for authorization policies
    - Support dry-run capability for policy changes
- [ ] Enhance user management operations:
    - Implement listUsers API to provide visibility into access permissions
    - Add listObjects API integration for resource permission auditing
    - Create user permission comparison tools for administrators

## Testing

The testing section ensures the plugin functions correctly and securely. It covers various testing approaches from unit tests of individual components to full integration tests with an OpenFGA server, including specialized tests for row-level security, column masking, and performance benchmarking under different load scenarios.

- [x] Create test infrastructure for the plugin
- [x] Unit tests for OpenFgaAccessControl methods
- [x] Unit tests for OpenFgaClient methods
- [x] Implement test coverage for deprecated methods like `checkCanSetUser`
- [x] Add tests for batch operations
- [x] Test with various Trino operations (CREATE, SELECT, INSERT, etc.)
    - Added TestOpenFgaSqlSecurity with JUnit Jupiter tests (@Test, @BeforeEach, @AfterEach)
    - Implemented tests for multiple user personas (admin, HR, finance, regular users)
    - Added test cases for SELECT operations with row-level security enforcement
    - Added test cases for column masking with different masking rules per user role
    - Added tests to verify department-specific data access controls
- [ ] Integration tests with OpenFGA server
- [x] Test row-level security functionality in unit tests
- [x] Test column masking functionality in unit tests
- [ ] SQL-level tests for row-level security
- [ ] SQL-level tests for column masking
- [ ] Test with OpenFGA assertions
- [ ] Create performance benchmarking tests
- [ ] Test conflict resolution in concurrent authorization scenarios
- [x] Unit tests for ModelDefinition YAML/JSON parsing
- [x] Unit tests for model validation functionality
- [x] Tests for model export to YAML format
- [ ] Tests for error handling and recovery scenarios
- [ ] Tests for connection failure scenarios
- [ ] Tests for authentication failures
- [ ] Tests for configuration validation
- [ ] Performance tests for cache effectiveness

## Documentation

The documentation section ensures users can effectively deploy, configure, and troubleshoot the plugin. It includes comprehensive documentation of all features, configuration options, examples of OpenFGA model definitions, guides for implementing row-level security and column masking, and detailed troubleshooting information.

- [x] Create initial README.md
- [ ] Complete plugin configuration documentation
- [ ] Document API token and authentication setup
- [ ] Add example relationship tuple definitions
- [ ] Document row filtering and column masking usage
- [ ] Add examples of filter and mask expressions
- [ ] Add troubleshooting section
- [ ] Create deployment guide for production use
- [ ] Create detailed OpenFGA authorization model design guide for Trino
- [ ] Document advanced relation types and their use cases
- [ ] Add debugging and troubleshooting guide for authorization issues
- [ ] Create authorization policy examples for common enterprise scenarios

## Polish & Packaging

The polish and packaging section focuses on finalizing the plugin for production readiness. It includes optimizing error messages, ensuring thread safety, addressing security concerns, creating release packages, and providing migration tools for version upgrades, ensuring a smooth deployment and upgrade experience.

- [ ] Review error messages for user-friendliness
- [ ] Handle internationalization considerations
- [ ] Ensure thread safety throughout the implementation
- [ ] Review for security issues (e.g., proper error handling)
- [ ] Create release package
- [ ] Upgrade to latest OpenFGA SDK version when available
- [ ] Create migration tools for upgrading between plugin versions
- [ ] Implement monitoring dashboard templates for OpenFGA authorization metrics
