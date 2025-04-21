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

import dev.openfga.sdk.api.client.model.ClientCheckRequest;
import dev.openfga.sdk.api.client.model.ClientCheckResponse;
import dev.openfga.sdk.api.client.model.ClientReadRequest;
import dev.openfga.sdk.api.client.model.ClientReadResponse;
import dev.openfga.sdk.errors.FgaInvalidParameterException;
import io.trino.plugin.openfga.model.ModelDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A mock implementation of OpenFgaClient for unit testing.
 * <p>
 * This class is designed to replace the real OpenFgaClient during unit tests, avoiding any real
 * network calls to an OpenFGA server. It uses the expectation-setting pattern similar to Mockito
 * to allow tests to specify what responses should be returned for specific operations.
 * <p>
 * It provides the following capabilities:
 * <ul>
 *   <li>Records all API calls for verification in tests</li>
 *   <li>Returns predefined responses that can be configured via the add* methods</li>
 *   <li>Allows tests to specify expected behaviors for specific operations</li>
 *   <li>Supports simulating row filters and column masks for data access control testing</li>
 * </ul>
 * <p>
 * Usage example:
 * <pre>
 *   // Create a testing client
 *   TestingOpenFgaClient client = new TestingOpenFgaClient(config);
 *
 *   // Configure predefined check responses
 *   client.addCheckResponse("table", "sales/orders", "can_select", true);
 *
 *   // Configure behavioral expectations
 *   ClientCheckRequest expectedRequest = new ClientCheckRequest();
 *   expectedRequest.user("user:alice");
 *   expectedRequest.relation("can_drop");
 *   expectedRequest._object("table:default/test/orders");
 *
 *   ClientCheckResponse mockResponse = createMockResponse(false);
 *   client.whenCheck(expectedRequest).thenReturn(mockResponse);
 *
 *   // Use in tests
 *   boolean hasAccess = client.checkTableAccess(context, "default", "test", "orders", "can_drop");
 *   assertFalse(hasAccess);
 *
 *   // Verify call counts
 *   assertEquals(1, client.getCheckCallCount("table", "default/test/orders", "can_drop"));
 * </pre>
 */
public class TestingOpenFgaClient
        extends OpenFgaClient
{
    // Add logger at the top of the class
    private static final Logger log = Logger.getLogger(TestingOpenFgaClient.class.getName());
    // Track API calls for verification
    private final Map<String, Integer> apiCallCounts = new HashMap<>();
    // For check responses
    private final Map<CheckKey, Boolean> checkResponses = new HashMap<>();
    private final Map<TypeObjectRelationTriplet, Integer> checkCalls = new HashMap<>();
    private final Map<CheckKey, Integer> checkCallCounts = new HashMap<>();
    // For tracking row filter and column mask calls
    private final Map<TypeRelationPair, Integer> filterCalls = new HashMap<>();
    private final Map<TypeRelationPair, Integer> maskCalls = new HashMap<>();
    // For storing read response data
    private final Map<ReadKey, List<String>> readResponses = new HashMap<>();
    private final Map<ReadKey, Integer> readCallCounts = new HashMap<>();
    // For row filter testing
    private final Map<String, RowFilter> rowFilters = new HashMap<>();
    private final Map<String, Set<String>> userToRowFilters = new HashMap<>();
    private final Map<String, Set<String>> roleToRowFilters = new HashMap<>();
    private final Map<String, Set<String>> userToRoles = new HashMap<>();
    // For column mask testing
    private final Map<String, ColumnMask> columnMasks = new HashMap<>();
    private final Map<String, Set<String>> userToColumnMasks = new HashMap<>();
    private final Map<String, Set<String>> roleToColumnMasks = new HashMap<>();
    // For behavior-based mocking style
    private final Map<RequestMatcher<ClientCheckRequest>, ClientCheckResponse> checkExpectations = new HashMap<>();
    private final Map<RequestMatcher<ClientReadRequest>, ClientReadResponse> readExpectations = new HashMap<>();
    // Operation handlers registry
    private final Map<Class<?>, OperationHandler<?>> operationHandlers = new HashMap<>();
    // Reference to the read handler for fluent API
    private final ReadOperationHandler readHandler = new ReadOperationHandler();
    // Reference to the check handler for fluent API
    private final CheckOperationHandler checkHandler = new CheckOperationHandler();

    /**
     * Creates a test client with the specified configuration.
     * All callers must provide a config with apiUrl, storeId, and modelId.
     *
     * @param config The OpenFgaConfig to use for this client
     */
    public TestingOpenFgaClient(OpenFgaConfig config)
    {
        super(config);
        registerDefaultHandlers();
    }

    /**
     * Register the default operation handlers
     */
    private void registerDefaultHandlers()
    {
        // Register handler for check operations using our dedicated handler
        registerOperationHandler(ClientCheckRequest.class, checkHandler);

        // Register handler for read operations
        registerOperationHandler(ClientReadRequest.class, readHandler);
    }

    /**
     * Register a handler for a specific operation type
     *
     * @param requestClass The class of the request object
     * @param handler The handler for operations of this type
     * @param <T> The type of request
     */
    private <T> void registerOperationHandler(Class<T> requestClass, OperationHandler<T> handler)
    {
        operationHandlers.put(requestClass, handler);
    }

    /**
     * Override the executeWithRetry method to simulate API responses without making network calls.
     * This implementation uses the expectation-setting style mocking approach with operation handlers.
     */
    @Override
    protected <T> T executeWithRetry(OpenFgaOperation<T> operation)
            throws ExecutionException, InterruptedException, FgaInvalidParameterException
    {
        log.info("Executing operation: " + operation.getDebugDescription());

        try {
            // Try each registered operation handler
            log.info("Registered handlers: " + operationHandlers.keySet());

            for (Class<?> requestClass : operationHandlers.keySet()) {
                @SuppressWarnings("unchecked")
                OperationHandler<Object> handler = (OperationHandler<Object>) operationHandlers.get(requestClass);

                log.info("Trying handler for: " + requestClass.getSimpleName());

                // Try to capture the request using this handler
                Object request = handler.captureRequest(operation);

                // If we successfully captured a request, this is the right handler
                if (request != null) {
                    log.info("Request captured: " + request.getClass().getSimpleName() + " - " + request);

                    // Record the call for verification
                    handler.recordCall(request);

                    // Get the response
                    log.info("Getting response for request: " + request);
                    Object response = handler.getResponse(request);
                    log.info("Got response: " + (response != null ? response.getClass().getSimpleName() : "null"));

                    // Return the response, with appropriate casting
                    @SuppressWarnings("unchecked")
                    T typedResponse = (T) response;
                    return typedResponse;
                }
                else {
                    log.info("Handler for " + requestClass.getSimpleName() + " did not capture the request");
                }
            }

            // If no handler captured a request, we don't know how to handle this operation
            log.severe("No handler found for operation: " + operation.getDebugDescription());
            throw new ExecutionException("Unhandled mock operation",
                    new UnsupportedOperationException("No handler for: " + operation.getDebugDescription()));
        }
        catch (Exception e) {
            log.log(Level.SEVERE, "Error handling operation: " + e.getMessage(), e);
            if (e instanceof ExecutionException) {
                throw (ExecutionException) e;
            }
            throw new ExecutionException("Error handling operation: " + e.getMessage(), e);
        }
    }

    /**
     * Record a check call for verification purposes
     */
    private void recordCheckCall(ClientCheckRequest request)
    {
        String user = request.getUser();
        String relation = request.getRelation();
        String object = request.getObject();

        // Record check call with the full criteria
        CheckKey key = new CheckKey(user, relation, object);
        checkCallCounts.merge(key, 1, Integer::sum);

        // Also record by type-object-relation for backward compatibility
        String objectType = "unknown";
        String objectId = object;
        if (object != null && object.contains(":")) {
            String[] parts = object.split(":", 2);
            objectType = parts[0];
            objectId = parts[1];
        }

        TypeObjectRelationTriplet triplet = new TypeObjectRelationTriplet(objectType, objectId, relation);
        checkCalls.merge(triplet, 1, Integer::sum);
    }

    /**
     * Record a read call for verification purposes
     */
    private void recordReadCall(ClientReadRequest request)
    {
        String user = request.getUser();
        String relation = request.getRelation();
        String object = request.getObject();

        ReadKey key = new ReadKey(user, relation, object);
        readCallCounts.merge(key, 1, Integer::sum);
    }

    /**
     * Try to capture a check request by executing the operation with a special capture client
     */
    private ClientCheckRequest captureCheckRequest(OpenFgaOperation<?> operation)
    {
        final ClientCheckRequest[] capturedRequest = new ClientCheckRequest[1];

        try {
            dev.openfga.sdk.api.configuration.ClientConfiguration config = new dev.openfga.sdk.api.configuration.ClientConfiguration();
            config.apiUrl("http://localhost");
            config.storeId("test-store-id");

            dev.openfga.sdk.api.client.OpenFgaClient captureClient = new dev.openfga.sdk.api.client.OpenFgaClient(config)
            {
                @Override
                public CompletableFuture<ClientCheckResponse> check(ClientCheckRequest request)
                {
                    capturedRequest[0] = request;
                    return CompletableFuture.completedFuture(createMockCheckResponse(false));
                }
            };

            // Try to execute the operation with the capture client to capture the request
            operation.execute();

            // If the capture didn't work (common case), try parsing the operation description
            if (capturedRequest[0] == null) {
                log.info("Capture via execution didn't work, trying to parse operation description");
                String description = operation.getDebugDescription();

                // Parse the debug description to extract information
                // Format is typically: "OpenFGA 'check' operation for [type]:[object] with relation [relation]"
                if (description.contains("'check' operation")) {
                    ClientCheckRequest request = new ClientCheckRequest();

                    // Extract object and relation
                    int forPos = description.indexOf(" for ");
                    int withRelationPos = description.indexOf(" with relation ");

                    if (forPos >= 0 && withRelationPos > forPos) {
                        String objectPart = description.substring(forPos + 5, withRelationPos);
                        String relation = description.substring(withRelationPos + 14);

                        // Set the relation - trim any whitespace
                        request.relation(relation.trim());

                        // Set the object - trim any whitespace
                        request._object(objectPart.trim());

                        // Default user - in most tests the context has user "test-user"
                        request.user("user:test-user");

                        // Try to find any explicit user info in the thread context or operation
                        // This is just a fallback, the actual test should configure proper expectations

                        log.info("Parsed check request from description: object=" + objectPart.trim() +
                                ", relation=" + relation.trim() + ", user=user:test-user");
                        return request;
                    }
                }
            }

            return capturedRequest[0];
        }
        catch (Exception e) {
            log.log(Level.WARNING, "Error capturing check request: " + e.getMessage(), e);
            return null;
        }
    }

    /**
     * Attempt to extract a ClientReadRequest from an operation
     * This is used by the ReadOperationHandler to inspect operations
     */
    private ClientReadRequest captureReadRequest(OpenFgaOperation<?> operation)
    {
        log.info("Attempting to capture read request from: " + operation.getDebugDescription());

        // Parse the debug description to extract information
        // Format is typically: "OpenFGA 'read' operation for user user:X with relation Y"
        String description = operation.getDebugDescription();

        if (description.contains("'read' operation")) {
            try {
                // Create a new request with information extracted from the debug description
                ClientReadRequest request = new ClientReadRequest();

                // Extract user
                if (description.contains("for user ")) {
                    int userStart = description.indexOf("for user ") + 9;
                    int userEnd = description.indexOf(" with relation");
                    if (userEnd > userStart) {
                        String user = description.substring(userStart, userEnd).trim();
                        request.user(user);
                        log.info("Extracted user: " + user);
                    }
                }

                // Extract relation
                if (description.contains("with relation ")) {
                    int relationStart = description.indexOf("with relation ") + 14;
                    String relation = description.substring(relationStart).trim();
                    request.relation(relation);
                    log.info("Extracted relation: " + relation);
                }

                // For read operations looking for filters, object is typically null
                request._object(null);

                log.info("Created request: " + request);
                return request;
            }
            catch (Exception e) {
                log.log(Level.WARNING, "Error capturing read request: " + e.getMessage(), e);
            }
        }

        return null;
    }

    /**
     * Create a mock CheckResponse with the specified allowed status
     */
    private ClientCheckResponse createMockCheckResponse(boolean allowed)
    {
        Map<String, List<String>> headers = new HashMap<>();
        dev.openfga.sdk.api.model.CheckResponse checkResponse = new dev.openfga.sdk.api.model.CheckResponse();
        checkResponse.setAllowed(allowed);

        // Create a raw response
        String rawResponse = "{}";
        try {
            rawResponse = ModelDefinition.toJson(checkResponse);
        }
        catch (IOException e) {
            // Use default empty JSON
        }

        dev.openfga.sdk.api.client.ApiResponse<dev.openfga.sdk.api.model.CheckResponse> apiResponse =
                new dev.openfga.sdk.api.client.ApiResponse<>(200, headers, rawResponse, checkResponse);

        return new ClientCheckResponse(apiResponse);
    }

    /**
     * Create a mock ReadResponse with the specified tuples
     */
    private ClientReadResponse createMockReadResponse(List<String> objectIds, String relation, String user)
    {
        ClientReadRequest request = new ClientReadRequest();
        request.relation(relation);
        request.user(user);
        return createMockReadResponse(request, objectIds);
    }

    /**
     * Create a mock ReadResponse with the specified object IDs and relation
     */
    private ClientReadResponse createMockReadResponse(ClientReadRequest request, List<String> objectIds)
    {
        // Extract information from the request
        String relation = request.getRelation();
        String user = request.getUser();

        Map<String, List<String>> headers = new HashMap<>();
        dev.openfga.sdk.api.model.ReadResponse readResponse = new dev.openfga.sdk.api.model.ReadResponse();

        List<dev.openfga.sdk.api.model.Tuple> tuples = new ArrayList<>();
        for (String objectId : objectIds) {
            dev.openfga.sdk.api.model.TupleKey tupleKey = new dev.openfga.sdk.api.model.TupleKey();
            tupleKey.setRelation(relation);
            tupleKey.setObject(objectId);
            tupleKey.setUser(user);

            dev.openfga.sdk.api.model.Tuple tuple = new dev.openfga.sdk.api.model.Tuple();
            tuple.setKey(tupleKey);
            tuples.add(tuple);
        }

        readResponse.setTuples(tuples);

        // Create a raw response
        String rawResponse = "{}";
        try {
            rawResponse = io.trino.plugin.openfga.model.ModelDefinition.toJson(readResponse);
        }
        catch (IOException e) {
            // Use default empty JSON
        }

        dev.openfga.sdk.api.client.ApiResponse<dev.openfga.sdk.api.model.ReadResponse> apiResponse =
                new dev.openfga.sdk.api.client.ApiResponse<>(200, headers, rawResponse, readResponse);

        return new ClientReadResponse(apiResponse);
    }

    /**
     * Configures a predefined authorization check response in the mock OpenFga client.
     *
     * <p>This method simulates the OpenFGA "check" API which determines whether a specific user
     * has a particular relation to an object. In authorization terms, this represents whether
     * a user has a specific permission on a resource (e.g., can a user select from a table).
     *
     * <p>During testing, this allows precise control over which authorization checks should
     * succeed or fail, enabling verification of proper access control enforcement.
     *
     * @param objectType The type of object (e.g., "table", "column", "schema")
     * @param objectId The identifier for the specific object
     * @param relation The permission or relation to check (e.g., "can_select", "can_insert")
     * @param allowed Whether the access check should return allowed (true) or denied (false)
     */
    public void addCheckResponse(String objectType, String objectId, String relation, boolean allowed)
    {
        checkResponses.put(new CheckKey(objectType, objectId, relation), allowed);
    }

    /**
     * Configures a predefined read response in the mock OpenFga client.
     *
     * <p>This method simulates the OpenFGA "read" API which retrieves all relationship tuples
     * that match the specified criteria. In OpenFGA, these tuples represent authorization
     * relationships between users and objects.
     *
     * <p>During testing, this allows precise control over what relationship tuples are returned
     * when the system queries authorization relationships, enabling verification of proper
     * access control behavior in list/enumeration operations.
     *
     * @param objectType The type of object (e.g., "table", "column", "schema")
     * @param objectId The identifier for the specific object
     * @param relation The permission or relation to check (e.g., "can_select", "can_insert")
     * @param users List of user IDs that should be returned as having access to the object
     */
    public void addReadResponse(String objectType, String objectId, String relation, List<String> users)
    {
        ReadKey key = new ReadKey(objectType, objectId, relation);
        readResponses.put(key, new ArrayList<>(users));
    }

    /**
     * Configures a row filter for fine-grained data access control.
     *
     * <p>Row filters allow authorization systems to limit which rows a user can see
     * within a table. This method creates a named row filter that applies to a specific
     * table and defines the filter expression that should be applied when users with
     * this filter access the table.
     *
     * <p>To associate users with this filter, use the addUserToRowFilter method after
     * creating the filter. This enables testing of dynamic row-level security where
     * different users see different subsets of data based on authorization rules.
     *
     * @param filterId A unique identifier for this row filter
     * @param tableId The identifier of the table this filter applies to
     * @param expression The filter expression to apply (e.g., "department = 'sales'")
     */
    public void addRowFilter(String filterId, String tableId, String expression)
    {
        rowFilters.put(filterId, new RowFilter(tableId, expression));
    }

    /**
     * Associate a row filter with a user
     */
    public void addUserToRowFilter(String userId, String filterId)
    {
        userToRowFilters.computeIfAbsent(userId, k -> new HashSet<>()).add(filterId);
    }

    /**
     * Associate a row filter with a role
     */
    public void addRoleToRowFilter(String roleId, String filterId)
    {
        roleToRowFilters.computeIfAbsent(roleId, k -> new HashSet<>()).add(filterId);
    }

    /**
     * Associate a user with a role
     */
    public void addUserToRole(String userId, String roleId)
    {
        userToRoles.computeIfAbsent(userId, k -> new HashSet<>()).add(roleId);
    }

    /**
     * Add a column mask definition
     */
    public void addColumnMask(String maskId, String columnId, String expression)
    {
        columnMasks.put(maskId, new ColumnMask(columnId, expression));
    }

    /**
     * Associate a column mask with a user
     */
    public void addUserToColumnMask(String userId, String maskId)
    {
        userToColumnMasks.computeIfAbsent(userId, k -> new HashSet<>()).add(maskId);
    }

    /**
     * Associate a column mask with a role
     */
    public void addRoleToColumnMask(String roleId, String maskId)
    {
        roleToColumnMasks.computeIfAbsent(roleId, k -> new HashSet<>()).add(maskId);
    }

    /**
     * Utility method to directly set column access permissions for a column
     */
    public void addColumnAccess(String catalogName, String schemaName, String tableName, String columnName, String permission, boolean allowed)
    {
        String objectId = catalogName + "/" + schemaName + "/" + tableName + "/" + columnName;
        addCheckResponse("column", objectId, permission, allowed);
    }

    /**
     * Gets the count of check calls made for a specific type, object, and relation.
     *
     * <p>This method is useful in tests to verify that permission checks were
     * properly applied during query execution. It returns the number of times checks were
     * requested for a particular type, object, and relation combination.
     *
     * @param type The object type
     * @param object The object identifier
     * @param relation The relation to check
     * @return The number of check calls made for this combination
     */
    public int getCheckCallCount(String type, String object, String relation)
    {
        TypeObjectRelationTriplet key = new TypeObjectRelationTriplet(type, object, relation);
        return checkCalls.getOrDefault(key, 0);
    }

    /**
     * Gets the count of row filter calls made for a specific user and table.
     *
     * <p>This method is useful in tests to verify that row-level security was properly
     * applied during query execution. It returns the number of times row filters were
     * requested for a particular user and table combination.
     *
     * @param userId The ID of the user
     * @param tableId The identifier for the table
     * @return The number of row filter calls made for this user and table
     */
    public int getRowFilterCallCount(String userId, String tableId)
    {
        TypeRelationPair key = new TypeRelationPair(userId, tableId);
        return filterCalls.getOrDefault(key, 0);
    }

    /**
     * Gets the count of column mask calls made for a specific user and column.
     *
     * <p>This method is useful in tests to verify that column-level security masks were properly
     * applied during query execution. It returns the number of times column masks were
     * requested for a particular user and column combination.
     *
     * @param userId The ID of the user
     * @param columnId The identifier for the column
     * @return The number of column mask calls made for this user and column
     */
    public int getColumnMaskCallCount(String userId, String columnId)
    {
        TypeRelationPair key = new TypeRelationPair(userId, columnId);
        return maskCalls.getOrDefault(key, 0);
    }

    /**
     * Configure this client to return a specific response when a check request matching
     * the given criteria is received.
     *
     * @param request The request to match against
     * @return An expectation builder that allows setting the response
     */
    public CheckExpectation whenCheck(ClientCheckRequest request)
    {
        return new CheckExpectation(request);
    }

    /**
     * Configure this client to return a specific response when a read request matching
     * the given criteria is received.
     *
     * @param request The request to match against
     * @return An expectation builder that allows setting the response
     */
    public ReadExpectation whenRead(ClientReadRequest request)
    {
        return new ReadExpectation(request);
    }

    /**
     * Override getApplicableColumnMask to properly track column mask calls for testing
     */
    @Override
    public Optional<String> getApplicableColumnMask(io.trino.plugin.openfga.model.OpenFgaContext context, String catalogName, String schemaName, String tableName, String columnName)
    {
        // Record this call for testing purposes
        if (context.getIdentity() != null && context.getIdentity().getUser() != null) {
            String columnId = catalogName + "/" + schemaName + "/" + tableName + "/" + columnName;
            String userId = context.getIdentity().getUser();

            // Increment the call counter for this user and column
            TypeRelationPair key = new TypeRelationPair(userId, columnId);
            maskCalls.merge(key, 1, Integer::sum);
        }

        // Call the real implementation
        return super.getApplicableColumnMask(context, catalogName, schemaName, tableName, columnName);
    }

    /**
     * Interface for operation handlers
     *
     * @param <T> The request type handled by this handler
     */
    private interface OperationHandler<T>
    {
        /**
         * Capture the request object from the operation
         *
         * @param operation The operation to capture from
         * @return The captured request, or null if capture failed
         */
        T captureRequest(OpenFgaOperation<?> operation);

        /**
         * Record that a call was made with this request
         *
         * @param request The request that was made
         */
        void recordCall(T request);

        /**
         * Get the response for this request
         *
         * @param request The request to respond to
         * @return The response object
         */
        Object getResponse(T request);
    }

    /**
     * Interface for request matchers
     */
    private interface RequestMatcher<T>
    {
        boolean matches(T request);
    }

    /**
     * Key class for check responses and call counts
     */
    private static class CheckKey
    {
        private final String objectType;
        private final String objectId;
        private final String relation;

        public CheckKey(String objectType, String objectId, String relation)
        {
            this.objectType = objectType;
            this.objectId = objectId;
            this.relation = relation;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CheckKey checkKey = (CheckKey) o;
            return objectType.equals(checkKey.objectType) &&
                    objectId.equals(checkKey.objectId) &&
                    relation.equals(checkKey.relation);
        }

        @Override
        public int hashCode()
        {
            return 31 * (31 * objectType.hashCode() + objectId.hashCode()) + relation.hashCode();
        }
    }

    /**
     * Key class for tracking check calls by type, object, and relation
     */
    private static class TypeObjectRelationTriplet
    {
        private final String objectType;
        private final String objectId;
        private final String relation;

        public TypeObjectRelationTriplet(String objectType, String objectId, String relation)
        {
            this.objectType = objectType;
            this.objectId = objectId;
            this.relation = relation;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TypeObjectRelationTriplet that = (TypeObjectRelationTriplet) o;
            return objectType.equals(that.objectType) &&
                    objectId.equals(that.objectId) &&
                    relation.equals(that.relation);
        }

        @Override
        public int hashCode()
        {
            return 31 * (31 * objectType.hashCode() + objectId.hashCode()) + relation.hashCode();
        }
    }

    /**
     * Key class for tracking calls by object type and relation
     */
    private static class TypeRelationPair
    {
        private final String objectType;
        private final String relation;

        public TypeRelationPair(String objectType, String relation)
        {
            this.objectType = objectType;
            this.relation = relation;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TypeRelationPair that = (TypeRelationPair) o;
            return objectType.equals(that.objectType) && relation.equals(that.relation);
        }

        @Override
        public int hashCode()
        {
            return 31 * objectType.hashCode() + relation.hashCode();
        }
    }

    /**
     * Key class used for storing Read call counts
     */
    private static class ReadKey
    {
        private final String user;
        private final String relation;
        private final String object;

        public ReadKey(String user, String relation, String object)
        {
            this.user = user;
            this.relation = relation;
            this.object = object;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ReadKey readKey = (ReadKey) o;
            return Objects.equals(user, readKey.user) &&
                    Objects.equals(relation, readKey.relation) &&
                    Objects.equals(object, readKey.object);
        }

        @Override
        public int hashCode()
        {
            // Use Objects.hash to safely handle null values
            return Objects.hash(user, relation, object);
        }
    }

    /**
     * Class to represent a row filter
     */
    private static class RowFilter
    {
        private final String tableId;
        private final String expression;

        public RowFilter(String tableId, String expression)
        {
            this.tableId = tableId;
            this.expression = expression;
        }
    }

    /**
     * Class to represent a column mask
     */
    private static class ColumnMask
    {
        private final String columnId;
        private final String expression;

        public ColumnMask(String columnId, String expression)
        {
            this.columnId = columnId;
            this.expression = expression;
        }
    }

    /**
     * Matcher for check requests
     */
    private static class CheckRequestMatcher
            implements RequestMatcher<ClientCheckRequest>
    {
        private final ClientCheckRequest expected;

        public CheckRequestMatcher(ClientCheckRequest expected)
        {
            this.expected = expected;
        }

        @Override
        public boolean matches(ClientCheckRequest actual)
        {
            // Compare relevant fields
            if (actual == null) {
                return false;
            }

            log.info("Comparing check requests - Expected: " + expected + ", Actual: " + actual);

            // Match user
            boolean userMatches = expected.getUser() == null ||
                    expected.getUser().equals(actual.getUser());

            // Match relation - trim whitespace for both since our parser might add it
            boolean relationMatches = expected.getRelation() == null ||
                    (expected.getRelation().trim().equals(actual.getRelation().trim()));

            // Match object
            boolean objectMatches = expected.getObject() == null ||
                    expected.getObject().equals(actual.getObject());

            log.info(String.format("Check request field matching - User: %s, Relation: %s, Object: %s",
                    userMatches, relationMatches, objectMatches));

            return userMatches && relationMatches && objectMatches;
        }
    }

    /**
     * Matcher for read requests
     */
    private static class ReadRequestMatcher
            implements RequestMatcher<ClientReadRequest>
    {
        private final ClientReadRequest expected;

        public ReadRequestMatcher(ClientReadRequest expected)
        {
            this.expected = expected;
        }

        @Override
        public boolean matches(ClientReadRequest actual)
        {
            // Compare relevant fields
            if (actual == null) {
                return false;
            }

            // Match based on non-null fields
            if (expected.getUser() != null && !expected.getUser().equals(actual.getUser())) {
                return false;
            }
            if (expected.getRelation() != null && !expected.getRelation().equals(actual.getRelation())) {
                return false;
            }
            return expected.getObject() == null || expected.getObject().equals(actual.getObject());
        }
    }

    /**
     * Class for setting up read operation expectations
     */
    public class ReadExpectation
    {
        private final ClientReadRequest request;

        ReadExpectation(ClientReadRequest request)
        {
            this.request = request;
        }

        public void thenReturn(List<String> objectIds)
        {
            readHandler.addExpectation(request, objectIds);
        }
    }

    /**
     * Class to build check expectations in a fluent style
     */
    public class CheckExpectation
    {
        private final ClientCheckRequest request;

        private CheckExpectation(ClientCheckRequest request)
        {
            this.request = request;
        }

        /**
         * Configure the response to return for the matching request
         *
         * @param response The response to return
         */
        public void thenReturn(ClientCheckResponse response)
        {
            checkExpectations.put(new CheckRequestMatcher(request), response);
        }

        /**
         * Configure a boolean response to return for the matching request
         *
         * @param allowed Whether the check should return allowed or denied
         */
        public void thenReturn(boolean allowed)
        {
            thenReturn(createMockCheckResponse(allowed));
        }
    }

    /**
     * Handler for OpenFGA read operations
     */
    private class ReadOperationHandler
            implements OperationHandler<ClientReadRequest>
    {
        private final Map<ClientReadRequest, List<String>> expectedResponses = new HashMap<>();
        private final Map<ClientReadRequest, Integer> callCounts = new HashMap<>();

        @Override
        public ClientReadRequest captureRequest(OpenFgaOperation<?> operation)
        {
            log.info("ReadOperationHandler.captureRequest: Trying to capture from: " + operation.getDebugDescription());
            if (operation.getDebugDescription().contains("'read' operation")) {
                // Use the captureReadRequest method to extract the request
                ClientReadRequest request = captureReadRequest(operation);
                if (request != null) {
                    log.info("Successfully captured read request: " + request);
                }
                else {
                    log.warning("Failed to capture read request from operation");
                }
                return request;
            }
            return null;
        }

        @Override
        public void recordCall(ClientReadRequest request)
        {
            log.info("ReadOperationHandler.recordCall: Recording call with request: " + request);
            callCounts.put(request, callCounts.getOrDefault(request, 0) + 1);
            // Also record in the main client for backward compatibility
            recordReadCall(request);
            apiCallCounts.merge("read", 1, Integer::sum);
        }

        @Override
        public ClientReadResponse getResponse(ClientReadRequest request)
        {
            log.info("ReadOperationHandler.getResponse: Getting response for request: " + request);
            log.info("Expected responses: " + expectedResponses.size() + " configured");

            // Dump all expectations for debugging
            int i = 0;
            for (Map.Entry<ClientReadRequest, List<String>> entry : expectedResponses.entrySet()) {
                log.info("Expectation " + (++i) + ": " + entry.getKey() + " -> " + entry.getValue());
            }

            // Look for exact matches in the expectations
            for (Map.Entry<ClientReadRequest, List<String>> entry : expectedResponses.entrySet()) {
                ClientReadRequest expectedRequest = entry.getKey();
                boolean matches = matchesRequest(expectedRequest, request);
                log.info("Checking if expected request matches: " + expectedRequest + " -> " + matches);

                if (matches) {
                    log.info("Found matching expectation, returning response with " + entry.getValue().size() + " objects");
                    return createMockReadResponse(request, entry.getValue());
                }
            }

            // If no expectation was found, throw an exception
            log.severe("No mock response configured for request: " + request);
            throw new UnsupportedOperationException("No mock response configured for: " + request);
        }

        /**
         * Check if a request matches an expectation
         */
        private boolean matchesRequest(ClientReadRequest expected, ClientReadRequest actual)
        {
            log.info("Matching requests - Expected: " + expected + ", Actual: " + actual);

            // Basic safety check
            if (expected == null || actual == null) {
                log.info("Request matching failed: One of the requests is null");
                return false;
            }

            // Match based on non-null fields
            boolean userMatches = expected.getUser() == null || expected.getUser().equals(actual.getUser());
            boolean relationMatches = expected.getRelation() == null || expected.getRelation().equals(actual.getRelation());
            boolean objectMatches = expected.getObject() == null || expected.getObject().equals(actual.getObject());

            log.info(String.format("Request field matching - User: %s, Relation: %s, Object: %s",
                    userMatches, relationMatches, objectMatches));

            return userMatches && relationMatches && objectMatches;
        }

        /**
         * Record an expected response for a specific read request
         */
        public void addExpectation(ClientReadRequest request, List<String> objectIds)
        {
            log.info("Adding read expectation: " + request + " -> " + objectIds);
            expectedResponses.put(request, new ArrayList<>(objectIds));
        }
    }

    /**
     * Handler for OpenFGA check operations
     */
    private class CheckOperationHandler
            implements OperationHandler<ClientCheckRequest>
    {
        private final Map<ClientCheckRequest, Boolean> expectedResponses = new HashMap<>();
        private final Map<ClientCheckRequest, Integer> callCounts = new HashMap<>();

        @Override
        public ClientCheckRequest captureRequest(OpenFgaOperation<?> operation)
        {
            log.info("CheckOperationHandler.captureRequest: Trying to capture from: " + operation.getDebugDescription());
            if (operation.getDebugDescription().contains("'check' operation")) {
                // Use the captureCheckRequest method to extract the request
                ClientCheckRequest request = captureCheckRequest(operation);
                if (request != null) {
                    log.info("Successfully captured check request: " + request);
                }
                else {
                    log.warning("Failed to capture check request from operation");
                }
                return request;
            }
            return null;
        }

        @Override
        public void recordCall(ClientCheckRequest request)
        {
            log.info("CheckOperationHandler.recordCall: Recording call with request: " + request);
            callCounts.put(request, callCounts.getOrDefault(request, 0) + 1);
            // Also record in the main client for backward compatibility
            recordCheckCall(request);
            apiCallCounts.merge("check", 1, Integer::sum);
        }

        @Override
        public ClientCheckResponse getResponse(ClientCheckRequest request)
        {
            log.info("CheckOperationHandler.getResponse: Getting response for request: " + request);

            // First check the expectation map for matching requests
            for (Map.Entry<RequestMatcher<ClientCheckRequest>, ClientCheckResponse> entry : checkExpectations.entrySet()) {
                if (entry.getKey().matches(request)) {
                    log.info("Found matching check expectation, returning response");
                    return entry.getValue();
                }
            }

            // If no expectation was found, fall back to the legacy response system
            String object = request.getObject();
            String relation = request.getRelation();

            if (object != null && object.contains(":")) {
                String[] parts = object.split(":", 2);
                String objectType = parts[0];
                String objectId = parts[1];

                CheckKey key = new CheckKey(objectType, objectId, relation);
                Boolean allowed = checkResponses.get(key);
                if (allowed != null) {
                    log.info("Found legacy response for " + key + ", returning: " + allowed);
                    return createMockCheckResponse(allowed);
                }
            }

            // If no response is configured, return a default deny
            log.warning("No mock response configured for check request: " + request + ", returning default deny");
            return createMockCheckResponse(false);
        }

        /**
         * Record an expected response for a specific check request
         */
        public void addExpectation(ClientCheckRequest request, boolean allowed)
        {
            log.info("Adding check expectation: " + request + " -> " + allowed);
            expectedResponses.put(request, allowed);
            checkExpectations.put(new CheckRequestMatcher(request), createMockCheckResponse(allowed));
        }
    }
}
