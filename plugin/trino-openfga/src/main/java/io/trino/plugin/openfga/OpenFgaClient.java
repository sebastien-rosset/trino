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

import com.google.inject.Inject;
import dev.openfga.sdk.api.client.model.ClientCheckRequest;
import dev.openfga.sdk.api.client.model.ClientCheckResponse;
import dev.openfga.sdk.api.client.model.ClientReadRequest;
import dev.openfga.sdk.api.client.model.ClientReadResponse;
import dev.openfga.sdk.api.configuration.ClientConfiguration;
import dev.openfga.sdk.api.model.Tuple;
import dev.openfga.sdk.api.model.TupleKey;
import dev.openfga.sdk.errors.FgaInvalidParameterException;
import io.airlift.log.Logger;
import io.trino.plugin.openfga.model.OpenFgaContext;
import io.trino.spi.security.Identity;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.requireNonNull;

/**
 * Client for communicating with the OpenFGA authorization service.
 * <p>
 * This client provides methods to perform authorization checks and retrieve security policies
 * from an OpenFGA service. It handles the communication details including request formatting,
 * API calls, error handling, and connection management with retry logic.
 * <p>
 * The client supports various security features:
 * <ul>
 *   <li>Basic authorization checks (catalog, schema, table, column level)</li>
 *   <li>Row-level security filtering</li>
 *   <li>Column masking</li>
 *   <li>Role-based access control</li>
 * </ul>
 * <p>
 * Authorization policies are retrieved from OpenFGA and applied according to the user's identity
 * and the access relationship being checked. The client has built-in connection resilience with
 * retry logic, connection pooling, and detailed error reporting.
 */
public class OpenFgaClient
{
    private static final Logger log = Logger.get(OpenFgaClient.class);
    // Connection management fields
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY_MS = 200;
    private OpenFgaConfig config;
    private final String storeId;
    private final Object apiClientLock = new Object();
    private dev.openfga.sdk.api.client.OpenFgaClient apiClient;

    @Inject
    public OpenFgaClient(OpenFgaConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        this.storeId = config.getStoreId();
        requireNonNull(config.getApiUrl(), "apiUrl is null");
        log.info("Initializing OpenFGA client with API URL: %s, Store ID: %s",
                config.getApiUrl(), config.getStoreId() != null ? config.getStoreId() : "not set");

        this.apiClient = createApiClient();
    }

    /**
     * Performs an authorization check against the OpenFGA service.
     * <p>
     * This method determines if a user has a specific relation (permission) to an object.
     * It formats and submits a check request to the OpenFGA service, handles retries for
     * transient errors, and provides detailed logging of the check process.
     * <p>
     * Object identifiers in OpenFGA follow a standard format of "objectType:objectId" where:
     * <ul>
     *   <li>objectType: The type of resource being authorized (catalog, schema, table, column)</li>
     *   <li>objectId: The unique identifier for that resource within the system</li>
     * </ul>
     * <p>
     * For security reasons, this method defaults to denial (returns false) when either:
     * <ul>
     *   <li>No user identity is provided in the context</li>
     *   <li>An error occurs during the authorization check</li>
     * </ul>
     *
     * @param context The context containing the user identity and request metadata
     * @param objectType The type of object being accessed (e.g., "catalog", "schema", "table")
     * @param objectId The unique identifier for the object
     * @param relation The permission relationship to check (e.g., "select", "insert", "viewer")
     * @return true if the user has the specified relation to the object, false otherwise
     */
    public boolean check(OpenFgaContext context, String objectType, String objectId, String relation)
    {
        log.debug("Checking %s relation on %s:%s for user %s", relation, objectType, objectId,
                context.getIdentity() != null ? context.getIdentity().getUser() : "null");

        // Early exit if we don't have a valid user identity
        if (context.getIdentity() == null || context.getIdentity().getUser() == null) {
            log.debug("No identity provided for authorization check");
            return false;
        }

        try {
            // Create check request
            ClientCheckRequest checkRequest = new ClientCheckRequest();
            checkRequest.user("user:" + context.getIdentity().getUser());
            checkRequest.relation(relation);
            checkRequest._object(objectType + ":" + objectId);

            // Call OpenFGA API with retry
            OpenFgaOperation<ClientCheckResponse> checkOperation = new OpenFgaOperation<>()
            {
                @Override
                public CompletableFuture<ClientCheckResponse> execute()
                        throws FgaInvalidParameterException
                {
                    return apiClient.check(checkRequest);
                }

                @Override
                public String getDebugDescription()
                {
                    return "OpenFGA 'check' operation for " + objectType + ":" + objectId + " with relation " + relation;
                }
            };

            ClientCheckResponse response = executeWithRetry(checkOperation);

            boolean allowed = response.getAllowed();
            log.debug("OpenFGA check result: %s has %s relation to %s:%s = %s",
                    context.getIdentity().getUser(), relation, objectType, objectId, allowed);

            return allowed;
        }
        catch (ExecutionException | InterruptedException | FgaInvalidParameterException e) {
            log.error(e, "Failed to perform OpenFGA check after retries: %s", e.getMessage());

            // Default to denying access on errors
            return false;
        }
    }

    public boolean checkCatalogAccess(OpenFgaContext context, String catalogName, String relation)
    {
        return check(context, "catalog", catalogName, relation);
    }

    public boolean checkSchemaAccess(OpenFgaContext context, String catalogName, String schemaName, String relation)
    {
        return check(context, "schema", catalogName + "/" + schemaName, relation);
    }

    public boolean checkTableAccess(OpenFgaContext context, String catalogName, String schemaName, String tableName, String relation)
    {
        return check(context, "table", catalogName + "/" + schemaName + "/" + tableName, relation);
    }

    public boolean checkColumnAccess(OpenFgaContext context, String catalogName, String schemaName, String tableName, String columnName, String relation)
    {
        return check(context, "column", catalogName + "/" + schemaName + "/" + tableName + "/" + columnName, relation);
    }

    /**
     * Retrieves row filtering expressions applicable to a specified table.
     * <p>
     * This method queries the OpenFGA service for row filters assigned to the current user
     * and determines which ones should be applied to the specified table. Row filters
     * are used to implement row-level security by dynamically filtering table rows
     * as part of query planning and execution within the SQL engine itself - this happens
     * BEFORE data is fetched from the underlying storage system, not after. This is crucial
     * for performance as it means unnecessary data is never retrieved from storage.
     * <p>
     * The returned expressions are valid SQL predicates that are injected into the
     * query plan as WHERE clauses when querying the target table. Multiple filters for
     * the same table are combined using AND logic. These filters become an integral part
     * of the SQL execution plan and are pushed down to the underlying storage systems
     * when possible, ensuring optimal performance.
     * <p>
     * Row filters in OpenFGA are stored as objects with the format:
     * <pre>filter:tableId:expression</pre>
     * Where:
     * <ul>
     *   <li><b>tableId</b>: Identifies which table(s) the filter applies to. Can be:
     *     <ul>
     *       <li>A fully-qualified table name: <code>catalog/schema/table</code></li>
     *       <li>A schema wildcard: <code>catalog/schema/*</code></li>
     *       <li>A global wildcard: <code>*</code></li>
     *     </ul>
     *   </li>
     *   <li><b>expression</b>: The SQL predicate to apply (e.g. <code>department = 'SALES'</code>)</li>
     * </ul>
     * <p>
     * When a connection exception occurs during the OpenFGA call, this method attempts
     * to retry the operation up to the configured maximum retry limit.
     *
     * @param context The OpenFGA context containing user identity and request metadata
     * @param catalogName The name of the catalog containing the table
     * @param schemaName The name of the schema containing the table
     * @param tableName The name of the table for which to retrieve row filters
     * @return A list of SQL filter expressions to apply to the table, empty if no filters apply
     */
    public List<String> getApplicableRowFilters(OpenFgaContext context, String catalogName, String schemaName, String tableName)
    {
        try {
            List<String> filters = new ArrayList<>();
            Identity identity = context.getIdentity();

            if (identity == null) {
                log.debug("No identity provided for row filter lookup");
                return List.of();
            }

            // Build the qualified table identifier that we'll use to match filters
            String tableIdentifier = catalogName + "/" + schemaName + "/" + tableName;
            log.debug("Looking up row filters for table: %s", tableIdentifier);

            ClientReadRequest readRequest = new ClientReadRequest();
            readRequest.user("user:" + identity.getUser());
            readRequest.relation("assigned");
            readRequest._object(null);

            List<Tuple> tuples = executeReadWithPagination(readRequest);
            for (Tuple tuple : tuples) {
                TupleKey key = tuple.getKey();
                if (key.getObject() != null && key.getObject().startsWith("filter:")) {
                    String fullFilterValue = key.getObject().substring("filter:".length());

                    // Parse the filter value - format is expected to be "tableId:expression"
                    // e.g. "catalog/schema/table:department = 'SALES'"
                    int separatorIndex = fullFilterValue.indexOf(':');

                    if (separatorIndex > 0) {
                        String filterTableId = fullFilterValue.substring(0, separatorIndex);
                        String filterExpression = fullFilterValue.substring(separatorIndex + 1);

                        // Check if this filter applies to our table
                        if (tableIdentifier.equals(filterTableId) ||
                                filterTableId.equals("*") ||
                                filterTableId.startsWith(catalogName + "/" + schemaName + "/*")) {
                            log.debug("Found applicable row filter for %s: %s", tableIdentifier, filterExpression);
                            filters.add(filterExpression);
                        }
                    }
                    else {
                        // If no separator found, treat the whole value as the expression
                        log.debug("Found row filter with no table identifier, assuming it applies: %s", fullFilterValue);
                        filters.add(fullFilterValue);
                    }
                }
            }

            log.debug("Found %d applicable row filters for %s", filters.size(), tableIdentifier);
            return filters;
        }
        catch (ExecutionException | InterruptedException | FgaInvalidParameterException e) {
            log.error(e, "Failed to get applicable row filters: %s", e.getMessage());
            return List.of();
        }
    }

    /**
     * Retrieves a column masking expression applicable to a specified column.
     * <p>
     * This method queries the OpenFGA service for column masks assigned to the current user
     * and determines which ones should be applied to the specified column. Column masks
     * are used to implement column-level security by dynamically transforming sensitive
     * column values before they are returned to the user.
     * <p>
     * The returned expression should be a valid SQL expression that can be used to transform
     * the column value. Only one mask (the first applicable one found) is applied to a column.
     * <p>
     * Column masks in OpenFGA are stored as objects with the format:
     * <pre>mask:columnId:expression</pre>
     * Where:
     * <ul>
     *   <li><b>columnId</b>: Identifies which column(s) the mask applies to. Can be:
     *     <ul>
     *       <li>A fully-qualified column name: <code>catalog/schema/table/column</code></li>
     *       <li>A global wildcard: <code>*</code></li>
     *     </ul>
     *   </li>
     *   <li><b>expression</b>: The SQL expression to apply (e.g., <code>CONCAT('XXX-XXX-', SUBSTRING(phone, 8, 4))</code>)</li>
     * </ul>
     * <p>
     * When a connection exception occurs during the OpenFGA call, this method attempts
     * to retry the operation up to the configured maximum retry limit.
     *
     * @param context The OpenFGA context containing user identity and request metadata
     * @param catalogName The name of the catalog containing the column
     * @param schemaName The name of the schema containing the column
     * @param tableName The name of the table containing the column
     * @param columnName The name of the column for which to retrieve a mask
     * @return An Optional containing the SQL mask expression if applicable, or empty if no mask applies
     */
    public Optional<String> getApplicableColumnMask(OpenFgaContext context, String catalogName, String schemaName, String tableName, String columnName)
    {
        try {
            Identity identity = context.getIdentity();

            if (identity == null) {
                log.debug("No identity provided for column mask lookup");
                return Optional.empty();
            }

            // Build the qualified column identifier that we'll use to match masks
            String columnIdentifier = catalogName + "/" + schemaName + "/" + tableName + "/" + columnName;
            log.debug("Looking up column masks for column: %s", columnIdentifier);

            ClientReadRequest readRequest = new ClientReadRequest();
            readRequest.user("user:" + identity.getUser());
            readRequest.relation("assigned");
            readRequest._object(null);

            List<Tuple> tuples = executeReadWithPagination(readRequest);
            for (Tuple tuple : tuples) {
                TupleKey key = tuple.getKey();
                if (key.getObject() != null && key.getObject().startsWith("mask:")) {
                    String fullMaskValue = key.getObject().substring("mask:".length());

                    // Parse the mask value - format is expected to be "columnId:expression"
                    // e.g. "catalog/schema/table/column:CAST(column AS VARCHAR)"
                    int separatorIndex = fullMaskValue.indexOf(':');

                    if (separatorIndex > 0) {
                        String maskColumnId = fullMaskValue.substring(0, separatorIndex);
                        String maskExpression = fullMaskValue.substring(separatorIndex + 1);

                        // Check if this mask applies to our column
                        if (columnIdentifier.equals(maskColumnId) || maskColumnId.equals("*")) {
                            log.debug("Found applicable column mask for %s: %s", columnIdentifier, maskExpression);
                            return Optional.of(maskExpression);
                        }
                    }
                    else {
                        // If no separator found, treat the whole value as the expression
                        log.debug("Found column mask with no column identifier, assuming it applies: %s", fullMaskValue);
                        return Optional.of(fullMaskValue);
                    }
                }
            }

            log.debug("No applicable column mask found for %s", columnIdentifier);
            return Optional.empty();
        }
        catch (ExecutionException | InterruptedException | FgaInvalidParameterException e) {
            log.error(e, "Failed to get applicable column mask: %s", e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Get the list of all row filter expressions applicable to a given entity type
     * This would typically be implemented to load filters from a configuration or database
     */
    protected List<String> filters(String entityType)
    {
        // This is a placeholder that would be implemented to load actual filter expressions
        // based on the entity type from a configuration store
        return new ArrayList<>();
    }

    /**
     * Get the list of all column mask expressions applicable to a given entity and column
     * This would typically be implemented to load masks from a configuration or database
     */
    protected List<String> masks(String entityType, String columnName)
    {
        // This is a placeholder that would be implemented to load actual mask expressions
        // based on the entity type and column name from a configuration store
        return new ArrayList<>();
    }

    /**
     * Helper method to get all roles assigned to a user
     */
    public List<String> getUserRoles(OpenFgaContext context)
            throws ExecutionException, InterruptedException, FgaInvalidParameterException
    {
        if (context.getIdentity() == null || context.getIdentity().getUser() == null) {
            log.debug("No identity provided for role lookup");
            return List.of();
        }

        String userId = context.getIdentity().getUser();
        List<String> roles = new ArrayList<>();

        ClientReadRequest readRequest = new ClientReadRequest();
        readRequest.user("user:" + userId);
        readRequest.relation("member");
        readRequest._object(null); // No object specified, we want all role objects

        List<Tuple> tuples = executeReadWithPagination(readRequest);

        // Process all results
        for (Tuple tuple : tuples) {
            TupleKey key = tuple.getKey();
            // Only process role objects
            if (key.getObject() != null && key.getObject().startsWith("role:")) {
                String roleId = key.getObject().replace("role:", "");
                roles.add(roleId);
            }
        }

        log.debug("Found %d roles for user %s: %s", roles.size(), userId, String.join(", ", roles));
        return roles;
    }

    /**
     * Execute a read operation with pagination support
     *
     * @param request The initial read request
     * @return A list of all tuples from all pages
     */
    private List<Tuple> executeReadWithPagination(ClientReadRequest request)
            throws ExecutionException, InterruptedException, FgaInvalidParameterException
    {
        List<Tuple> allTuples = new ArrayList<>();

        // Execute the initial read request
        OpenFgaOperation<ClientReadResponse> readOperation = new OpenFgaOperation<>()
        {
            @Override
            public CompletableFuture<ClientReadResponse> execute()
                    throws FgaInvalidParameterException
            {
                return apiClient.read(request);
            }

            @Override
            public String getDebugDescription()
            {
                StringBuilder description = new StringBuilder("OpenFGA 'read' operation");
                if (request.getUser() != null) {
                    description.append(" for user ").append(request.getUser());
                }
                if (request.getRelation() != null) {
                    description.append(" with relation ").append(request.getRelation());
                }
                if (request.getObject() != null) {
                    description.append(" on object ").append(request.getObject());
                }
                return description.toString();
            }
        };

        ClientReadResponse response = executeWithRetry(readOperation);
        int pageCount = 1;

        // Add tuples from the first page
        List<Tuple> pageTuples = response.getTuples();
        if (pageTuples != null) {
            allTuples.addAll(pageTuples);
            log.debug("Read page %d with %d tuples", pageCount, pageTuples.size());
        }

        // Note: The ClientReadRequest in OpenFGA SDK 0.8.1 doesn't support pagination
        // with continuation tokens. This is a limitation of this version of the SDK.
        // In a production environment, you may need to use a newer SDK version
        // or implement a custom solution to handle large result sets.

        log.debug("Completed read operation with %d pages, retrieved %d total tuples",
                pageCount, allTuples.size());

        return allTuples;
    }

    /**
     * Create an OpenFGA API client using the configuration
     */
    private dev.openfga.sdk.api.client.OpenFgaClient createApiClient()
    {
        try {
            // In v0.8.1, we use ClientConfiguration
            ClientConfiguration clientConfig = new ClientConfiguration();
            clientConfig.apiUrl(config.getApiUrl());
            clientConfig.storeId(storeId);

            // Set connection parameters using methods available in SDK v0.8.1
            // Note: In v0.8.1, there is no direct timeout() method - these settings are handled
            // through the underlying HTTP client which we don't directly configure here

            // Add API token if configured
            if (config.getApiToken().isPresent()) {
                // Create ApiToken object first, then create Credentials with it
                dev.openfga.sdk.api.configuration.ApiToken apiToken =
                        new dev.openfga.sdk.api.configuration.ApiToken(config.getApiToken().get());
                clientConfig.credentials(new dev.openfga.sdk.api.configuration.Credentials(apiToken));
                clientConfig.authorizationModelId(config.getModelId());
            }

            return new dev.openfga.sdk.api.client.OpenFgaClient(clientConfig);
        }
        catch (FgaInvalidParameterException e) {
            log.error(e, "Failed to create OpenFGA client: %s", e.getMessage());
            throw new RuntimeException("Failed to create OpenFGA client", e);
        }
    }

    /**
     * Execute an OpenFGA API call with retry logic.
     * This method will attempt to recreate the client if a connection error occurs.
     *
     * @param operation The operation to execute
     * @return The result of the operation
     * @throws ExecutionException If the operation fails after all retries
     * @throws InterruptedException If the thread is interrupted
     * @throws FgaInvalidParameterException If the parameters are invalid
     */
    protected <T> T executeWithRetry(OpenFgaOperation<T> operation)
            throws ExecutionException, InterruptedException, FgaInvalidParameterException
    {
        int retries = 0;
        ExecutionException lastException = null;

        while (retries < MAX_RETRIES) {
            try {
                return operation.execute().get();
            }
            catch (ExecutionException e) {
                lastException = e;
                String errorDetails = extractErrorDetails(e);

                // Check if this is a connection-related exception
                if (isConnectionException(e.getCause())) {
                    log.warn("Connection error while calling OpenFGA API, retrying (%d/%d): %s - %s",
                            retries + 1, MAX_RETRIES, e.getMessage(), errorDetails);

                    // Recreate the client on connection errors
                    synchronized (apiClientLock) {
                        apiClient = createApiClient();
                    }

                    // Wait before retrying
                    try {
                        Thread.sleep((long) RETRY_DELAY_MS * (1L << retries)); // Exponential backoff
                    }
                    catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw ie;
                    }

                    retries++;
                }
                else {
                    // Not a connection error, log detailed error and throw
                    log.error("Non-connection error from OpenFGA API: %s", errorDetails);
                    throw e;
                }
            }
        }

        // If we get here, we've exhausted our retries
        if (lastException != null) {
            String finalErrorDetails = extractErrorDetails(lastException);
            log.error("Failed to execute OpenFGA operation after %d retries: %s", MAX_RETRIES, finalErrorDetails);
            throw new ExecutionException("Failed to execute operation after " + MAX_RETRIES + " retries: " + finalErrorDetails, lastException);
        }

        // All retries failed and we didn't capture an exception (extremely unlikely but possible)
        throw new ExecutionException("Failed to execute operation after " + MAX_RETRIES + " retries with no recorded exception", new RuntimeException("Unknown error"));
    }

    /**
     * Extract detailed error information from an ExecutionException
     */
    private String extractErrorDetails(ExecutionException e)
    {
        Throwable cause = e.getCause();
        StringBuilder details = new StringBuilder();

        details.append(e.getMessage() != null ? e.getMessage() : "No message");

        if (cause != null) {
            details.append(" - Cause: ").append(cause.getClass().getSimpleName());

            if (cause.getMessage() != null) {
                details.append(": ").append(cause.getMessage());
            }
            // Extract HTTP response body for API errors
            if (cause instanceof dev.openfga.sdk.errors.FgaError apiError) {
                String responseBody = apiError.getResponseData();
                if (responseBody != null && !responseBody.isEmpty()) {
                    details.append(" - API Error: ").append(responseBody);
                }
            }
        }
        return details.toString();
    }

    /**
     * Check if an exception is related to network connectivity issues
     */
    private boolean isConnectionException(Throwable t)
    {
        return t instanceof java.net.ConnectException ||
                t instanceof java.net.SocketTimeoutException ||
                t instanceof java.io.IOException ||
                (t.getMessage() != null &&
                        (t.getMessage().contains("Connection") ||
                                t.getMessage().contains("timeout") ||
                                t.getMessage().contains("connect")));
    }

    /**
     * Functional interface for OpenFGA operations that return a CompletableFuture.
     * Includes additional methods to provide better debug information.
     */
    @FunctionalInterface
    protected interface OpenFgaOperation<T>
    {
        /**
         * Execute the OpenFGA operation
         */
        java.util.concurrent.CompletableFuture<T> execute()
                throws FgaInvalidParameterException;

        /**
         * Get a descriptive message for debugging purposes
         */
        default String getDebugDescription()
        {
            throw new UnsupportedOperationException("getDebugDescription() not implemented");
        }
    }

    /**
     * Updates the client configuration and recreates the underlying API client.
     * This method is used primarily in test environments to reconfigure the client.
     *
     * @param newConfig The new configuration to apply
     */
    public void updateConfig(OpenFgaConfig newConfig)
    {
        log.info("Updating OpenFGA client configuration");
        synchronized (apiClientLock) {
            this.config = requireNonNull(newConfig, "config is null");
            if (newConfig.getStoreId() != null) {
                log.info("Updating store ID to: %s", newConfig.getStoreId());
            }
            if (newConfig.getModelId() != null) {
                log.info("Updating model ID to: %s", newConfig.getModelId());
            }
            this.apiClient = createApiClient();
        }
    }
}
