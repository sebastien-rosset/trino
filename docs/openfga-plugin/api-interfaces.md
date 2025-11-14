# API Interfaces and Key Classes

## Core Plugin Interfaces

### Plugin Entry Point

```java
package io.trino.plugin.openfga;

import io.trino.spi.Plugin;
import io.trino.spi.security.SystemAccessControlFactory;
import com.google.common.collect.ImmutableList;

public class OpenFGAPlugin implements Plugin {

    @Override
    public Iterable<SystemAccessControlFactory> getSystemAccessControlFactories() {
        return ImmutableList.of(new OpenFGASystemAccessControlFactory());
    }

    @Override
    public String getVersion() {
        return OpenFGAPlugin.class.getPackage().getImplementationVersion();
    }
}
```

### System Access Control Factory

```java
package io.trino.plugin.openfga;

import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemAccessControlContext;
import io.trino.spi.security.SystemAccessControlFactory;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;

public class OpenFGASystemAccessControlFactory implements SystemAccessControlFactory {

    @Override
    public String getName() {
        return "openfga";
    }

    @Override
    public SystemAccessControl create(Map<String, String> config,
                                    SystemAccessControlContext context) {
        try {
            Bootstrap app = new Bootstrap(
                new OpenFGAModule(),
                new OpenFGAConfigModule(config)
            );

            Injector injector = app
                .doNotInitializeLogging()
                .initialize();

            return injector.getInstance(OpenFGASystemAccessControl.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create OpenFGA system access control", e);
        }
    }
}
```

### Main System Access Control Implementation

```java
package io.trino.plugin.openfga;

import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.connector.*;
import com.google.inject.Inject;

public class OpenFGASystemAccessControl implements SystemAccessControl {

    private final OpenFGAHighLevelClient openFGAClient;
    private final AuthorizationModelManager authorizationModelManager;
    private final PerformanceOptimizer performanceOptimizer;
    private final MaterializedViewPolicyResolver materializedViewResolver;

    @Inject
    public OpenFGASystemAccessControl(
            OpenFGAHighLevelClient openFGAClient,
            AuthorizationModelManager authorizationModelManager,
            PerformanceOptimizer performanceOptimizer,
            MaterializedViewPolicyResolver materializedViewResolver) {
        this.openFGAClient = requireNonNull(openFGAClient);
        this.authorizationModelManager = requireNonNull(authorizationModelManager);
        this.performanceOptimizer = requireNonNull(performanceOptimizer);
        this.materializedViewResolver = requireNonNull(materializedViewResolver);
    }

    // Fine-grained access control methods
    @Override
    public List<ViewExpression> getRowFilters(SystemSecurityContext context,
                                            CatalogSchemaTableName tableName) {
        return performanceOptimizer.getCachedOrCompute(
            new RowFilterRequest(context, tableName),
            () -> computeRowFilters(context, tableName)
        );
    }

    @Override
    public Map<ColumnSchema, ViewExpression> getColumnMasks(SystemSecurityContext context,
                                                           CatalogSchemaTableName tableName,
                                                           List<ColumnSchema> columns) {
        return performanceOptimizer.getCachedOrCompute(
            new ColumnMaskRequest(context, tableName, columns),
            () -> computeColumnMasks(context, tableName, columns)
        );
    }

    // Standard authorization check methods
    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context,
                                        CatalogSchemaTableName table,
                                        Set<String> columns) {
        AuthorizationRequest request = AuthorizationRequest.builder()
            .identity(context.getIdentity())
            .action("select")
            .resource(formatTableResource(table))
            .columns(columns)
            .build();

        if (!openFGAClient.isAuthorized(request)) {
            throw new AccessDeniedException("Access denied to table: " + table);
        }
    }

    @Override
    public boolean canAccessCatalog(SystemSecurityContext context, String catalogName) {
        AuthorizationRequest request = AuthorizationRequest.builder()
            .identity(context.getIdentity())
            .action("discover")
            .resource("catalog:" + catalogName)
            .build();

        return openFGAClient.isAuthorized(request);
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs) {
        return performanceOptimizer.batchFilter(
            catalogs,
            catalog -> canAccessCatalog(context, catalog)
        );
    }

    // ... (50+ other SystemAccessControl methods)

    private List<ViewExpression> computeRowFilters(SystemSecurityContext context,
                                                  CatalogSchemaTableName tableName) {
        // Check if this is a materialized view
        if (isMaterializedView(tableName)) {
            return materializedViewResolver.resolveInheritedRowFilters(context, tableName);
        }

        // Generate authorization checks for the table using OpenFGA
        List<AuthorizationRequest> authorizationRequests =
            authorizationModelManager.buildRowFilterRequests(context, tableName);
        List<ViewExpression> rowFilters = new ArrayList<>();

        for (AuthorizationRequest request : authorizationRequests) {
            if (openFGAClient.isAuthorized(request)) {
                ViewExpression filter = authorizationModelManager.buildRowFilterExpression(request, context, tableName);
                if (filter != null) {
                    rowFilters.add(filter);
                }
            }
        }

        return rowFilters;
    }
}
```

## OpenFGA Client Interfaces

### High-Level OpenFGA Client

```java
package io.trino.plugin.openfga.client;

import io.trino.plugin.openfga.model.*;
import java.util.concurrent.CompletableFuture;

public interface OpenFGAHighLevelClient {

    // Authorization check methods
    boolean isAuthorized(AuthorizationRequest request);
    CompletableFuture<Boolean> isAuthorizedAsync(AuthorizationRequest request);

    // Batch authorization methods
    Map<AuthorizationRequest, Boolean> batchCheck(List<AuthorizationRequest> requests);
    CompletableFuture<Map<AuthorizationRequest, Boolean>> batchCheckAsync(List<AuthorizationRequest> requests);

    // Object discovery methods
    Set<String> listAuthorizedObjects(ListObjectsRequest request);
    CompletableFuture<Set<String>> listAuthorizedObjectsAsync(ListObjectsRequest request);

    // Row filter and column mask methods
    List<ViewExpression> getRowFilters(RowFilterRequest request);
    Map<ColumnSchema, ViewExpression> getColumnMasks(ColumnMaskRequest request);

    // Relationship management (for entity integration)
    void writeRelationships(List<RelationshipTuple> tuples);
    void deleteRelationships(List<RelationshipTuple> tuples);
    List<RelationshipTuple> readRelationships(ReadRelationshipsRequest request);

    // Health and monitoring
    HealthStatus getHealthStatus();
    ClientMetrics getMetrics();
}

@Component
public class OpenFGAHighLevelClientImpl implements OpenFGAHighLevelClient {

    private final OpenFGAHttpClient httpClient;
    private final BatchAuthorizationManager batchManager;
    private final CacheManager cacheManager;
    private final CircuitBreaker circuitBreaker;

    @Override
    public boolean isAuthorized(AuthorizationRequest request) {
        return circuitBreaker.execute(
            () -> performAuthorization(request),
            () -> fallbackStrategy.fallback(request)
        );
    }

    @Override
    public Map<AuthorizationRequest, Boolean> batchCheck(List<AuthorizationRequest> requests) {
        return batchManager.batchAuthorize(requests);
    }

    // ... other implementations
}
```

### HTTP Client Interface

```java
package io.trino.plugin.openfga.client;

import io.trino.plugin.openfga.model.*;

public interface OpenFGAHttpClient {

    // Core OpenFGA API methods
    CheckResponse check(CheckRequest request);
    BatchCheckResponse batchCheck(BatchCheckRequest request);
    ListObjectsResponse listObjects(ListObjectsRequest request);
    WriteResponse writeRelationships(WriteRequest request);
    ReadResponse readRelationships(ReadRequest request);

    // Store management
    CreateStoreResponse createStore(CreateStoreRequest request);
    GetStoreResponse getStore(String storeId);
    ListStoresResponse listStores();

    // Authorization model management
    WriteAuthorizationModelResponse writeAuthorizationModel(
        String storeId,
        WriteAuthorizationModelRequest request
    );
    ReadAuthorizationModelResponse readAuthorizationModel(String storeId, String modelId);

    // Connection management
    ConnectionStats getConnectionStats();
    void close();
}

@Component
public class OpenFGAHttpClientImpl implements OpenFGAHttpClient {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final OpenFGAConfig config;
    private final RetryPolicy retryPolicy;

    @Override
    public CheckResponse check(CheckRequest request) {
        String url = String.format("%s/stores/%s/check",
            config.getApiUrl(), config.getStoreId());

        try {
            HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + config.getApiKey())
                .POST(HttpRequest.BodyPublishers.ofString(
                    objectMapper.writeValueAsString(request)
                ))
                .build();

            HttpResponse<String> response = httpClient.send(httpRequest,
                HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), CheckResponse.class);
            } else {
                throw new OpenFGAException("Check request failed: " + response.statusCode());
            }
        } catch (Exception e) {
            throw new OpenFGAException("Failed to execute check request", e);
        }
    }
}
```

## Authorization Model Manager Interfaces

### Authorization Model Manager

```java
package io.trino.plugin.openfga.authz;

import io.trino.plugin.openfga.model.*;
import io.trino.spi.security.SystemSecurityContext;

public interface AuthorizationModelManager {

    // Authorization request building methods
    List<AuthorizationRequest> buildRowFilterRequests(SystemSecurityContext context,
                                                     CatalogSchemaTableName table);
    AuthorizationRequest buildColumnAccessRequest(SystemSecurityContext context,
                                                CatalogSchemaTableName table,
                                                String columnName);

    // SQL generation methods
    ViewExpression buildRowFilterExpression(AuthorizationRequest request,
                                          SystemSecurityContext context,
                                          CatalogSchemaTableName table);
    ViewExpression buildColumnMaskExpression(AuthorizationRequest request,
                                           SystemSecurityContext context,
                                           CatalogSchemaTableName table,
                                           String columnName);

    // Model management and loading
    void loadAuthorizationModel(String modelPath);
    void reloadAuthorizationModel();
    AuthorizationModelMetadata getModelMetadata();

    // Context building
    Map<String, Object> buildAuthorizationContext(SystemSecurityContext securityContext,
                                                CatalogSchemaTableName table);
}

@Component
public class AuthorizationModelManagerImpl implements AuthorizationModelManager {

    private final OpenFGAModelLoader modelLoader;
    private final SQLGenerator sqlGenerator;
    private final AttributeInjector attributeInjector;
    private final TupleBuilder tupleBuilder;

    @Override
    public ViewExpression buildRowFilterExpression(AuthorizationRequest request,
                                                 SystemSecurityContext context,
                                                 CatalogSchemaTableName table) {
        Map<String, Object> authzContext = buildAuthorizationContext(context, table);

        // Build conditional access based on OpenFGA response and trusted attributes
        if (request.hasConditions()) {
            return sqlGenerator.generateConditionalFilter(request.getConditions(), authzContext);
        } else {
            return ViewExpression.builder()
                .expression("TRUE")  // Unconditional access granted
                .build();
        }
    }

    @Override
    public List<AuthorizationRequest> buildRowFilterRequests(SystemSecurityContext context,
                                                           CatalogSchemaTableName table) {
        String datasetResource = String.format("dataset:%s.%s.%s",
            table.getCatalogName(),
            table.getSchemaTableName().getSchemaName(),
            table.getSchemaTableName().getTableName());

        Map<String, Object> authzContext = buildAuthorizationContext(context, table);

        return List.of(
            AuthorizationRequest.builder()
                .identity(context.getIdentity())
                .action("select")
                .resource(datasetResource)
                .context(authzContext)
                .build(),
            AuthorizationRequest.builder()
                .identity(context.getIdentity())
                .action("conditional_select")
                .resource(datasetResource)
                .context(authzContext)
                .build()
        );
    }
}
```

### Policy Model Classes

```java
package io.trino.plugin.openfga.model;

// Authorization request model
@JsonSerialize
@JsonDeserialize
public class AuthorizationRequest {
    private final Identity identity;
    private final String action;
    private final String resource;
    private final Map<String, Object> context;
    private final Set<String> columns;

    // Builder pattern
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        // Builder implementation
    }
}

// OpenFGA authorization model metadata
@JsonSerialize
@JsonDeserialize
public class AuthorizationModelMetadata {
    private final String modelId;
    private final String schemaVersion;
    private final Set<String> supportedTypes;
    private final Map<String, Set<String>> typeRelations;
    private final Map<String, String> conditionDefinitions;

    // Getters and builder
}

// View expression model for SQL generation
public class ViewExpression {
    private final String expression;
    private final Optional<Identity> identity;
    private final Optional<String> catalog;
    private final Optional<String> schema;

    public static Builder builder() {
        return new Builder();
    }
}

// OpenFGA tuple model
@JsonSerialize
@JsonDeserialize
public class RelationshipTuple {
    private final String user;
    private final String relation;
    private final String object;
    private final Optional<Map<String, Object>> context;

    // Constructor and getters
}
```

## Performance and Caching Interfaces

### Performance Optimizer

```java
package io.trino.plugin.openfga.performance;

public interface PerformanceOptimizer {

    // Caching methods
    <T, R> R getCachedOrCompute(CacheKey<T> key, Supplier<R> computation);
    void invalidateCache(CacheKey<?> key);
    void invalidateAllCaches();

    // Batch processing methods
    <T> Set<T> batchFilter(Set<T> items, Predicate<T> filter);
    <T, R> Map<T, R> batchTransform(Set<T> items, Function<T, R> transformer);

    // Predictive authorization
    void preauthorizeSession(SessionContext session);
    void preauthorizeCommonPatterns(Identity identity);

    // Metrics and monitoring
    CacheStats getCacheStats();
    PerformanceMetrics getPerformanceMetrics();
}

@Component
public class PerformanceOptimizerImpl implements PerformanceOptimizer {

    private final HierarchicalCacheManager cacheManager;
    private final BatchProcessor batchProcessor;
    private final PredictiveAuthorizer predictiveAuthorizer;
    private final MetricsCollector metricsCollector;

    @Override
    public <T, R> R getCachedOrCompute(CacheKey<T> key, Supplier<R> computation) {
        return cacheManager.getOrCompute(key, computation);
    }

    @Override
    public <T> Set<T> batchFilter(Set<T> items, Predicate<T> filter) {
        return batchProcessor.parallelFilter(items, filter);
    }
}
```

### Cache Management Interfaces

```java
package io.trino.plugin.openfga.cache;

public interface CacheManager {

    // Generic cache operations
    <K, V> V get(K key, Function<K, V> loader);
    <K, V> void put(K key, V value);
    <K> void invalidate(K key);
    void invalidateAll();

    // Cache statistics
    CacheStats getStats(String cacheName);
    Map<String, CacheStats> getAllStats();

    // Cache configuration
    void configureCacheTTL(String cacheName, Duration ttl);
    void configureCacheSize(String cacheName, long maxSize);
}

public interface SessionCache extends CacheManager {
    // Session-specific caching with automatic cleanup
    void bindToSession(String sessionId);
    void unbindSession(String sessionId);
    void cleanupExpiredSessions();
}
```

## Entity Integration Interfaces

### Entity Access Control Provider

```java
package io.trino.plugin.openfga.entity;

public interface EntityAccessControlProvider {

    // Entity metadata methods
    EntityMetadata getEntityMetadata(String entityName);
    List<EntityMetadata> getAllEntities();

    // Table mapping methods
    List<TableMapping> getTableMappings(String entityName);
    Optional<EntityMetadata> getEntityForTable(CatalogSchemaTableName table);

    // Policy methods
    List<AccessPolicy> getEntityPolicies(String entityName);
    AccessPolicy getEntityPolicy(String entityName, String operation);

    // Configuration and lifecycle
    void reloadConfiguration();
    void validateConfiguration();
}

// Entity metadata model
public class EntityMetadata {
    private final String entityName;
    private final String description;
    private final List<EntityAttribute> attributes;
    private final List<EntityRelationship> relationships;
    private final EntityAccessConfiguration accessConfig;

    // Getters and builder
}

// Table mapping model
public class TableMapping {
    private final String entityName;
    private final CatalogSchemaTableName tableName;
    private final Map<String, String> columnMappings;
    private final Optional<String> filterExpression;

    // Getters and builder
}
```

## Configuration Interfaces

### Configuration Classes

```java
package io.trino.plugin.openfga.config;

@ConfigProperties("openfga")
public class OpenFGAConfig {

    @NotNull
    private URI apiUrl;

    @NotNull
    private String storeId;

    @NotNull
    private String authorizationModelId;

    private String apiKey;
    private Duration httpTimeout = Duration.ofSeconds(5);
    private int batchSize = 100;
    private boolean cacheEnabled = true;
    private Duration cacheTTL = Duration.ofMinutes(5);

    // OpenFGA authorization model configuration
    private String authorizationModelPath = "/etc/trino/openfga-model.fga";
    private boolean authorizationModelReloadEnabled = true;
    private Duration authorizationModelReloadInterval = Duration.ofMinutes(30);

    // Entity configuration
    private boolean entityIntegrationEnabled = false;
    private String entityConfigurationFile = "/etc/trino/entity-config.yaml";

    // Performance configuration
    private int connectionPoolSize = 100;
    private Duration connectionIdleTimeout = Duration.ofMinutes(5);
    private boolean circuitBreakerEnabled = true;
    private int circuitBreakerFailureThreshold = 10;

    // Getters and setters with @Config annotations
}
```

---
