# Policy Inheritance for Views and Derived Objects

## Overview

Views, materialized views, and other derived database objects present a fundamental challenge for access control: they contain or compute data derived from source tables, but users interact with them as independent objects. The plugin ensures **logical data consistency** by automatically inheriting and combining access policies from source tables across all derived object types.

## Core Principle

**Universal Policy Inheritance Rule**: Users can access data from any derived object (view, materialized view, CTE, subquery, etc.) only if they would have access to all the corresponding source table rows that contribute to that derived data.

This ensures that derived objects don't become a backdoor to bypass access controls on source data, regardless of how the data is accessed or computed.

## Policy Inheritance Scenarios

The OpenFGA plugin implements policy inheritance for multiple database constructs:

1. **Regular Views** - Logical views computed on-demand from source tables
2. **Materialized Views** - Physically stored views with pre-computed results
3. **Common Table Expressions (CTEs)** - Temporary named result sets within queries
4. **Subqueries** - Nested queries within larger SQL statements
5. **Derived Tables** - Table expressions in FROM clauses
6. **Functions and Procedures** - User-defined functions that access tables

## Inheritance Architecture

### Universal Lineage Discovery Engine

```java
public class UniversalLineageAnalyzer {

    public ViewLineage analyzeLineage(CatalogSchemaTableName derivedObject, DerivedObjectType type) {
        return switch (type) {
            case REGULAR_VIEW -> analyzeViewLineage(derivedObject);
            case MATERIALIZED_VIEW -> analyzeMaterializedViewLineage(derivedObject);
            case CTE -> analyzeCTELineage(derivedObject);
            case SUBQUERY -> analyzeSubqueryLineage(derivedObject);
            case DERIVED_TABLE -> analyzeDerivedTableLineage(derivedObject);
            case FUNCTION -> analyzeFunctionLineage(derivedObject);
        };
    }

    public ViewLineage analyzeViewLineage(CatalogSchemaTableName view) {
        // 1. Get view definition from Trino metadata
        ViewDefinition definition = getViewDefinition(view);

        // 2. Parse SQL to extract table dependencies
        Set<TableReference> sourceTables = parseSourceTables(definition.getQuery());

        // 3. Analyze column mappings and transformations
        Map<String, Set<ColumnReference>> columnLineage = analyzeColumnLineage(
            definition.getQuery(),
            sourceTables
        );

        // 4. Detect aggregations and transformations
        Set<DataTransformation> transformations = analyzeTransformations(definition.getQuery());

        return ViewLineage.builder()
            .derivedObject(view)
            .objectType(DerivedObjectType.REGULAR_VIEW)
            .sourceTables(sourceTables)
            .columnLineage(columnLineage)
            .transformations(transformations)
            .build();
    }

    public ViewLineage analyzeMaterializedViewLineage(CatalogSchemaTableName materializedView) {
        // 1. Get materialized view definition from Trino metadata
        MaterializedViewDefinition definition = getMaterializedViewDefinition(materializedView);

        // 2. Parse SQL to extract table dependencies
        Set<TableReference> sourceTables = parseSourceTables(definition.getQuery());

        // 3. Analyze column mappings and transformations
        Map<String, Set<ColumnReference>> columnLineage = analyzeColumnLineage(
            definition.getQuery(),
            sourceTables
        );

        // 4. Detect aggregations and transformations
        Set<DataTransformation> transformations = analyzeTransformations(definition.getQuery());

        return ViewLineage.builder()
            .derivedObject(materializedView)
            .objectType(DerivedObjectType.MATERIALIZED_VIEW)
            .sourceTables(sourceTables)
            .columnLineage(columnLineage)
            .transformations(transformations)
            .build();
    }

    private Set<TableReference> parseSourceTables(String sqlQuery) {
        // Use Trino SQL parser to extract table references
        Statement statement = sqlParser.createStatement(sqlQuery);
        TableExtractionVisitor visitor = new TableExtractionVisitor();
        visitor.process(statement);
        return visitor.getReferencedTables();
    }
}
```

### Policy Inheritance Engine

```java
public class PolicyInheritanceEngine {

    public InheritedPolicies inheritPolicies(ViewLineage lineage, PolicyContext context) {
        Map<CatalogSchemaTableName, List<AccessPolicy>> sourcePolicies =
            lineage.getSourceTables().stream()
                .collect(toMap(
                    TableReference::getTableName,
                    table -> getPoliciesForTable(table.getTableName(), context)
                ));

        // Generate inherited row filters
        List<ViewExpression> inheritedRowFilters = generateInheritedRowFilters(
            sourcePolicies,
            lineage
        );

        // Generate inherited column masks
        Map<String, ViewExpression> inheritedColumnMasks = generateInheritedColumnMasks(
            sourcePolicies,
            lineage
        );

        return InheritedPolicies.builder()
            .rowFilters(inheritedRowFilters)
            .columnMasks(inheritedColumnMasks)
            .sourceOrigin(sourcePolicies)
            .inheritanceType(determineInheritanceType(lineage))
            .build();
    }
}
```

## Inheritance Scenarios by Object Type

### Regular Views (Logical Views)

Regular views are virtual tables defined by SQL queries that are executed on-demand. Policy inheritance is applied during query analysis phase.

**Example Regular View**:

```sql
CREATE VIEW sales.reports.customer_summary AS
SELECT
    c.customer_id,
    c.customer_name,
    c.region,
    COUNT(o.order_id) as total_orders,
    SUM(o.amount) as total_spent
FROM sales.customers c
LEFT JOIN sales.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name, c.region;
```

**Policy Inheritance for Regular Views**:

- **Query-time evaluation**: Policies applied during each query execution
- **Real-time updates**: Access control reflects current source table policies
- **Performance optimization**: Cached policy evaluation with source table change detection
- **Trino integration**: Uses `getRowFilters()` during query analysis phase

**Implementation**:

```java
public class RegularViewPolicyHandler {

    @Override
    public List<ViewExpression> getRowFilters(SystemSecurityContext context,
                                            CatalogSchemaTableName view) {
        // 1. Detect that this is a regular view
        if (!isRegularView(view)) {
            return Collections.emptyList();
        }

        // 2. Analyze view lineage
        ViewLineage lineage = universalLineageAnalyzer.analyzeViewLineage(view);

        // 3. Inherit policies from source tables
        InheritedPolicies inherited = policyInheritanceEngine.inheritPolicies(lineage, context);

        // 4. Apply real-time to current query
        return inherited.getRowFilters();
    }
}
```

### Common Table Expressions (CTEs)

CTEs create temporary named result sets within a single query scope. Policy inheritance is applied at query planning time.

**Example CTE with Policy Inheritance**:

```sql
WITH regional_sales AS (
  SELECT region, SUM(amount) as total_sales
  FROM sales.orders
  WHERE order_date >= '2024-01-01'
  GROUP BY region
),
top_regions AS (
  SELECT region, total_sales
  FROM regional_sales
  WHERE total_sales > 100000
)
SELECT r.region, r.total_sales, c.customer_count
FROM top_regions r
JOIN (SELECT region, COUNT(*) as customer_count FROM sales.customers GROUP BY region) c
  ON r.region = c.region;
```

**CTE Policy Handling**:

```java
public class CTEPolicyHandler {

    public void applyCTEPolicies(Query query, SystemSecurityContext context) {
        // 1. Extract CTEs from query
        List<WithClause> ctes = extractCTEs(query);

        for (WithClause cte : ctes) {
            // 2. Analyze CTE lineage
            ViewLineage lineage = universalLineageAnalyzer.analyzeCTELineage(cte);

            // 3. Apply inherited policies to CTE definition
            InheritedPolicies policies = policyInheritanceEngine.inheritPolicies(lineage, context);

            // 4. Rewrite CTE with inherited filters
            WithClause rewrittenCTE = applyCTEFilters(cte, policies);
            query.replaceCTE(cte, rewrittenCTE);
        }
    }
}
```

### Subqueries and Derived Tables

Subqueries and derived tables in FROM clauses inherit policies from their source tables during query optimization.

**Example Subquery Policy Inheritance**:

```sql
-- Original query with subquery
SELECT customer_id, avg_order_value
FROM (
    SELECT customer_id, AVG(amount) as avg_order_value
    FROM sales.orders
    WHERE status = 'completed'
    GROUP BY customer_id
) customer_averages
WHERE avg_order_value > 1000;

-- Query with inherited policies applied
SELECT customer_id, avg_order_value
FROM (
    SELECT customer_id, AVG(amount) as avg_order_value
    FROM sales.orders
    WHERE status = 'completed'
      AND region IN (${user.authorized_regions})  -- Inherited policy
      AND tenant_id = ${user.tenant_id}           -- Inherited policy
    GROUP BY customer_id
) customer_averages
WHERE avg_order_value > 1000;
```

### User-Defined Functions and Procedures

Functions that access tables inherit policies from all tables they reference.

**Function Policy Inheritance**:

```java
public class FunctionPolicyHandler {

    public void validateFunctionAccess(String functionName, SystemSecurityContext context) {
        // 1. Get function definition
        FunctionDefinition function = getFunctionDefinition(functionName);

        // 2. Analyze table dependencies
        Set<TableReference> referencedTables = extractTableReferences(function);

        // 3. Validate access to all referenced tables
        for (TableReference table : referencedTables) {
            if (!hasTableAccess(context, table.getTableName())) {
                throw new AccessDeniedException("Insufficient permissions for function " + functionName);
            }
        }

        // 4. Apply inherited policies to function execution context
        applyInheritedPolicies(function, context);
    }
}
```

## Inheritance Strategies

### Simple Table Inheritance

**Scenario**: Derived object (view, materialized view, etc.) selects from single table

```sql
CREATE MATERIALIZED VIEW sales.reports.daily_orders AS
SELECT order_date, SUM(amount) as total_amount, COUNT(*) as order_count
FROM sales.orders.orders
GROUP BY order_date;
```

**Source Policy**:

```sql
-- sales.orders.orders row filter
WHERE region IN (${user.authorized_regions}) AND tenant_id = ${user.tenant_id}
```

**Inherited Policy**:

```sql
-- sales.reports.daily_orders inherits same filter
WHERE region IN (${user.authorized_regions}) AND tenant_id = ${user.tenant_id}
```

### Multi-Table Join Inheritance

**Scenario**: Derived object joins multiple tables

```sql
CREATE MATERIALIZED VIEW sales.reports.customer_orders AS
SELECT
    c.customer_id,
    c.customer_name,
    o.order_id,
    o.amount,
    o.order_date
FROM sales.customer.customers c
JOIN sales.orders.orders o ON c.customer_id = o.customer_id;
```

**Source Policies**:

```sql
-- sales.customer.customers
WHERE tenant_id = ${user.tenant_id}

-- sales.orders.orders
WHERE region IN (${user.authorized_regions})
```

**Inherited Policy (Intersection)**:

```sql
-- User must have access to BOTH source tables
WHERE tenant_id = ${user.tenant_id}
  AND region IN (${user.authorized_regions})
```

### Complex Aggregation Inheritance

**Scenario**: Multi-table aggregation with filtering

```sql
CREATE MATERIALIZED VIEW analytics.summary.regional_sales AS
SELECT
    r.region_name,
    p.product_category,
    DATE_TRUNC('month', o.order_date) as month,
    SUM(oi.quantity * oi.price) as revenue,
    COUNT(DISTINCT o.customer_id) as unique_customers
FROM sales.orders.orders o
JOIN sales.orders.order_items oi ON o.order_id = oi.order_id
JOIN sales.products.products p ON oi.product_id = p.product_id
JOIN sales.regions.regions r ON o.region_id = r.region_id
WHERE o.status = 'completed'
GROUP BY r.region_name, p.product_category, DATE_TRUNC('month', o.order_date);
```

**Inherited Policy Generation**:

```java
public List<ViewExpression> generateComplexInheritedFilters(
        Map<CatalogSchemaTableName, List<ViewExpression>> sourcePolicies,
        ViewLineage lineage) {

    List<ViewExpression> combinedFilters = new ArrayList<>();

    // Apply intersection logic: User needs access to ALL source tables
    for (Map.Entry<CatalogSchemaTableName, List<ViewExpression>> entry : sourcePolicies.entrySet()) {
        CatalogSchemaTableName sourceTable = entry.getKey();
        List<ViewExpression> tableFilters = entry.getValue();

        for (ViewExpression filter : tableFilters) {
            // Map source table columns to materialized view columns
            ViewExpression mappedFilter = mapFilterToMaterializedView(
                filter,
                sourceTable,
                lineage
            );

            if (mappedFilter != null) {
                combinedFilters.add(mappedFilter);
            }
        }
    }

    return combinedFilters;
}
```

## Column Mapping and Masking Inheritance

### Column Lineage Analysis

```java
public class ColumnLineageAnalyzer {

    public Map<String, Set<ColumnReference>> analyzeColumnLineage(String sql,
                                                                 Set<TableReference> sourceTables) {
        // Parse SQL and build column dependency graph
        Statement statement = sqlParser.createStatement(sql);
        ColumnLineageVisitor visitor = new ColumnLineageVisitor(sourceTables);
        visitor.process(statement);

        return visitor.getColumnMappings();
    }

    private static class ColumnLineageVisitor extends DefaultExpressionTraversalVisitor<Void, Void> {

        @Override
        public Void visitDereferenceExpression(DereferenceExpression node, Void context) {
            // Track column references and their sources
            String column = node.getField().getValue();
            Expression base = node.getBase();

            if (base instanceof Identifier) {
                String table = ((Identifier) base).getValue();
                recordColumnMapping(table, column);
            }

            return super.visitDereferenceExpression(node, context);
        }

        @Override
        public Void visitFunctionCall(FunctionCall node, Void context) {
            // Handle aggregation functions and their column dependencies
            String functionName = node.getName().toString();

            if (isAggregationFunction(functionName)) {
                for (Expression arg : node.getArguments()) {
                    recordAggregationDependency(functionName, arg);
                }
            }

            return super.visitFunctionCall(node, context);
        }
    }
}
```

### Column Masking Inheritance

```java
public Map<String, ViewExpression> inheritColumnMasks(
        Map<CatalogSchemaTableName, Map<String, ViewExpression>> sourceColumnMasks,
        ViewLineage lineage) {

    Map<String, ViewExpression> inheritedMasks = new HashMap<>();

    for (Map.Entry<String, Set<ColumnReference>> columnEntry : lineage.getColumnLineage().entrySet()) {
        String materializedViewColumn = columnEntry.getKey();
        Set<ColumnReference> sourceColumns = columnEntry.getValue();

        // Determine masking strategy based on source column masks
        ViewExpression inheritedMask = determineColumnMask(sourceColumns, sourceColumnMasks);

        if (inheritedMask != null) {
            inheritedMasks.put(materializedViewColumn, inheritedMask);
        }
    }

    return inheritedMasks;
}

private ViewExpression determineColumnMask(Set<ColumnReference> sourceColumns,
                                         Map<CatalogSchemaTableName, Map<String, ViewExpression>> sourceColumnMasks) {

    List<ViewExpression> applicableMasks = new ArrayList<>();

    for (ColumnReference sourceColumn : sourceColumns) {
        Map<String, ViewExpression> tableMasks = sourceColumnMasks.get(sourceColumn.getTableName());
        if (tableMasks != null) {
            ViewExpression mask = tableMasks.get(sourceColumn.getColumnName());
            if (mask != null) {
                applicableMasks.add(mask);
            }
        }
    }

    if (applicableMasks.isEmpty()) {
        return null; // No masking required
    }

    // Apply most restrictive mask
    return selectMostRestrictiveMask(applicableMasks);
}
```

## Aggregation and Transformation Handling

### Aggregation Policy Inheritance

```java
public class AggregationPolicyHandler {

    public ViewExpression handleAggregationInheritance(AggregationNode aggregation,
                                                      List<ViewExpression> sourceFilters,
                                                      ViewLineage lineage) {

        switch (aggregation.getType()) {
            case SUM, COUNT, AVG:
                // Numerical aggregations: Apply source filters to underlying data
                return combineFiltersForAggregation(sourceFilters);

            case MIN, MAX:
                // Extrema aggregations: Ensure user can see the actual values
                return combineFiltersWithVisibilityCheck(sourceFilters, aggregation);

            case COUNT_DISTINCT:
                // Distinct count: Apply filters but handle anonymization
                return handleDistinctCountFilter(sourceFilters, aggregation);

            default:
                // Custom aggregations: Conservative approach
                return combineFiltersConservatively(sourceFilters);
        }
    }

    private ViewExpression combineFiltersForAggregation(List<ViewExpression> sourceFilters) {
        if (sourceFilters.isEmpty()) {
            return null;
        }

        // Combine all source filters with AND logic
        String combinedExpression = sourceFilters.stream()
            .map(filter -> "(" + filter.getExpression() + ")")
            .collect(joining(" AND "));

        return ViewExpression.builder()
            .expression(combinedExpression)
            .build();
    }
}
```

### Transformation Handling

```java
public enum TransformationType {
    DIRECT_COLUMN,          // SELECT column AS alias
    AGGREGATION,            // SUM(column), COUNT(*)
    EXPRESSION,             // column1 + column2
    CASE_WHEN,             // CASE WHEN ... THEN ... END
    DATE_FUNCTION,         // DATE_TRUNC, EXTRACT
    STRING_FUNCTION        // UPPER, LOWER, SUBSTR
}

public class TransformationPolicyHandler {

    public ViewExpression handleTransformation(DataTransformation transformation,
                                             List<ViewExpression> sourceFilters) {

        return switch (transformation.getType()) {
            case DIRECT_COLUMN ->
                // Simple column alias: Apply source filters directly
                combineFilters(sourceFilters);

            case AGGREGATION ->
                // Aggregation: Handle according to aggregation rules
                aggregationPolicyHandler.handleAggregationInheritance(
                    (AggregationNode) transformation, sourceFilters, lineage);

            case EXPRESSION ->
                // Mathematical expression: Apply source filters to all input columns
                handleExpressionTransformation(transformation, sourceFilters);

            case CASE_WHEN ->
                // Conditional logic: Complex handling based on conditions
                handleCaseWhenTransformation(transformation, sourceFilters);

            case DATE_FUNCTION, STRING_FUNCTION ->
                // Function application: Apply source filters to input columns
                handleFunctionTransformation(transformation, sourceFilters);
        };
    }
}
```

## Performance Optimization for Policy Inheritance

### Universal Inheritance Caching

```java
public class UniversalInheritanceCacheManager {

    // Cache compiled inheritance rules for all derived object types
    private final Cache<DerivedObjectKey, CompiledInheritanceRules> inheritanceCache;

    // Cache resolved lineage information for all derived object types
    private final Cache<DerivedObjectKey, ViewLineage> lineageCache;

    // Cache regular view inheritance (more frequently accessed)
    private final Cache<DerivedObjectKey, InheritedPolicies> viewPolicyCache;

    public InheritedPolicies getCachedInheritance(CatalogSchemaTableName derivedObject,
                                                 DerivedObjectType objectType,
                                                 PolicyContext context) {

        DerivedObjectKey key = new DerivedObjectKey(derivedObject, objectType);

        // For regular views, check policy cache first (most frequent)
        if (objectType == DerivedObjectType.REGULAR_VIEW) {
            InheritedPolicies cached = viewPolicyCache.getIfPresent(key);
            if (cached != null && !hasSourcePoliciesChanged(cached, context)) {
                return cached;
            }
        }

        // Get cached lineage
        ViewLineage lineage = lineageCache.get(key, () ->
            universalLineageAnalyzer.analyzeLineage(derivedObject, objectType)
        );

        // Get cached inheritance rules
        CompiledInheritanceRules rules = inheritanceCache.get(key, () ->
            compileInheritanceRules(lineage)
        );

        // Apply rules to current context
        InheritedPolicies result = applyInheritanceRules(rules, context);

        // Cache for regular views
        if (objectType == DerivedObjectType.REGULAR_VIEW) {
            viewPolicyCache.put(key, result);
        }

        return result;
    }

    private CompiledInheritanceRules compileInheritanceRules(ViewLineage lineage) {
        // Pre-compile inheritance logic for performance
        return CompiledInheritanceRules.builder()
            .sourceTableFilters(compileSourceTableFilters(lineage))
            .columnMappings(compileColumnMappings(lineage))
            .aggregationRules(compileAggregationRules(lineage))
            .transformationRules(compileTransformationRules(lineage))
            .build();
    }
}
```

### Lazy Inheritance Evaluation

```java
public class LazyInheritanceEvaluator {

    public List<ViewExpression> getRowFilters(SystemSecurityContext context,
                                            CatalogSchemaTableName materializedView) {

        // Check if materialized view has explicit policies first
        List<ViewExpression> explicitFilters = getExplicitPolicies(materializedView, context);
        if (!explicitFilters.isEmpty()) {
            return explicitFilters;
        }

        // Lazy evaluation: Only compute inheritance when needed
        return computeIfAbsent(
            materializedView,
            () -> computeInheritedFilters(materializedView, context)
        );
    }

    private List<ViewExpression> computeInheritedFilters(CatalogSchemaTableName materializedView,
                                                        SystemSecurityContext context) {
        // Perform expensive inheritance computation
        ViewLineage lineage = analyzeLineage(materializedView);
        return inheritanceEngine.inheritPolicies(lineage, context).getRowFilters();
    }
}
```

## Configuration and Control

### Universal Policy Inheritance Configuration

```yaml
# Policy inheritance settings for all derived objects
policy_inheritance:
  enabled: true

  # Inheritance strategy
  strategy: "intersection"  # "intersection" or "union"

  # Object-specific settings
  regular_views:
    enabled: true
    cache_ttl: "15m"  # Shorter TTL for frequently changing policies
    real_time_policy_updates: true

  materialized_views:
    enabled: true
    cache_ttl: "1h"   # Longer TTL for more stable objects
    lazy_evaluation: true

  ctes:
    enabled: true
    inline_policy_application: true

  subqueries:
    enabled: true
    nested_inheritance_depth: 5  # Max nesting levels

  functions:
    enabled: true
    validate_table_access: true
    cache_function_dependencies: true

  # Performance settings
  lineage_analysis_timeout: "30s"
  max_source_tables: 100
  parallel_lineage_analysis: true

  # Inheritance behavior
  handle_aggregations: true
  handle_transformations: true
  preserve_source_column_masks: true

  # Fallback behavior
  on_lineage_analysis_failure: "deny_access"  # or "allow_access"
  on_complex_transformation: "conservative"   # or "permissive"
  on_circular_dependency: "deny_access"
```

### Override Mechanisms

```java
public class UniversalInheritanceOverrideManager {

    public List<ViewExpression> getRowFiltersWithOverrides(
            SystemSecurityContext context,
            CatalogSchemaTableName derivedObject,
            DerivedObjectType objectType) {

        // 1. Check for explicit policies on the derived object
        List<ViewExpression> explicitPolicies = getExplicitPolicies(derivedObject, context);
        if (!explicitPolicies.isEmpty()) {
            return explicitPolicies;
        }

        // 2. Check for inheritance override configuration
        InheritanceOverride override = getInheritanceOverride(derivedObject, objectType);
        if (override != null) {
            return applyOverride(override, context, objectType);
        }

        // 3. Check object-type specific policies
        List<ViewExpression> typeSpecificPolicies = getObjectTypeSpecificPolicies(
            derivedObject, objectType, context
        );
        if (!typeSpecificPolicies.isEmpty()) {
            return typeSpecificPolicies;
        }

        // 4. Default: Compute inherited policies
        return computeInheritedPolicies(derivedObject, objectType, context);
    }

    private List<ViewExpression> getObjectTypeSpecificPolicies(
            CatalogSchemaTableName derivedObject,
            DerivedObjectType objectType,
            SystemSecurityContext context) {

        return switch (objectType) {
            case REGULAR_VIEW -> getViewSpecificPolicies(derivedObject, context);
            case MATERIALIZED_VIEW -> getMaterializedViewSpecificPolicies(derivedObject, context);
            case CTE -> getCTESpecificPolicies(derivedObject, context);
            case SUBQUERY -> getSubquerySpecificPolicies(derivedObject, context);
            case FUNCTION -> getFunctionSpecificPolicies(derivedObject, context);
            default -> Collections.emptyList();
        };
    }
}
```

---
