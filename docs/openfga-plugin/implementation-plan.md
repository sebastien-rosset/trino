# Implementation Plan

## Current Status

This plugin is in **advanced design phase** with **generic attribute framework architecture completed**. Key architectural issues resolved:

1. ✅ **Generic Attribute Framework** - Eliminated hardcoded attributes, supports any organization's attribute schema
2. ✅ **Schema-Driven Configuration** - All attribute names and sources configurable via external files
3. ✅ **Trino Plugin Pattern Compliance** - Follows established OPA/Ranger plugin architecture patterns
4. ✅ **Separation of Concerns** - Policy ↔ Mapping ↔ Enforcement cleanly separated
5. 🔄 **OpenFGA Integration Design** - Core patterns defined, implementation validation pending

## Critical Architectural Achievement

**Framework Flexibility Validated**: The generic attribute injection framework now supports:

- **Healthcare**: facility_id, patient_group, provider_type, hipaa_classification
- **Financial**: trading_desk, risk_level, compliance_zone, market_access
- **Government**: clearance_level, department_id, classification, security_zone
- **SaaS**: tenant_id, account_id, workspace_id, organization_id
- **Custom**: Any attribute schema via configuration-driven resolver framework

## Major Open Design Issues

### Unresolved Critical Problems

1. **SystemAccessControl Integration with Injected Attributes**
   - Row filters need to reference dynamically injected attribute values
   - No clear mechanism for schema-driven filter expression generation
   - Hard dependency between attribute injection and access control layers

2. **StatementRewrite AST Modification Complexity**
   - Query transformation logic not fully designed
   - Table identification and attribute application rules undefined
   - SQL generation from injected attributes not proven

3. **OpenFGA Authorization Model Validation**
   - Authorization model patterns not tested with real OpenFGA server
   - Conditional tuple performance characteristics unknown
   - Context building for dynamic attributes unproven

4. **Performance Impact Unknown**
   - No benchmarks of attribute resolution overhead
   - Multi-layer caching strategy not validated
   - Network latency impact of external attribute sources not measured

5. **Configuration Schema Complexity**
   - Generic configuration approach adds significant complexity
   - Schema validation and error handling not designed
   - Migration path from existing systems undefined

### Required Design Validation

Before any implementation can begin:

1. **Proof of Concept Required**: Build minimal working prototype to validate core assumptions
2. **OpenFGA Integration Testing**: Test authorization model with real OpenFGA server
3. **Trino SPI Deep Dive**: Validate that StatementRewrite + SystemAccessControl integration actually works
4. **Performance Baseline**: Establish acceptable overhead limits with realistic workloads

### Honest Assessment

- **Generic framework approach**: Partially designed, needs implementation validation
- **Trino integration points**: Conceptually identified, not proven to work
- **OpenFGA integration**: API patterns researched, actual integration unvalidated
- **Performance requirements**: Undefined and untested

## Next Steps (Reality-Based)

1. **Complete technical design review** with focus on integration gaps
2. **Build minimal proof of concept** to validate core assumptions
3. **Test OpenFGA integration approach** with real server and realistic data
4. **Establish performance requirements** based on actual measurements
5. **Define implementation approach** based on validated technical feasibility

**Implementation planning is premature until fundamental integration questions are resolved through prototyping and testing.**
