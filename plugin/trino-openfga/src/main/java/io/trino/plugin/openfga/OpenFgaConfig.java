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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

import java.util.Map;
import java.util.Optional;

/**
 * Configuration properties for the OpenFGA authorization service integration.
 * <p>
 * This class manages configuration settings for connecting to and interacting with
 * an OpenFGA (Open Fine-Grained Authorization) service. It provides settings for:
 * <ul>
 *   <li>API connection details (URL, store ID, model ID)</li>
 *   <li>Authentication parameters (API token, OAuth credentials)</li>
 *   <li>Operational settings (logging, permission management)</li>
 *   <li>Security policy definitions (row filters, column masks)</li>
 *   <li>Authorization model management</li>
 * </ul>
 * <p>
 * Configuration values can be provided via Trino's configuration system. Examples:
 * <pre>
 * openfga.api.url=https://openfga.example.com/api
 * openfga.store.id=01ABCDEF12345678
 * openfga.model.id=01ABCDEF87654321
 * </pre>
 * <p>
 * This configuration drives all aspects of OpenFGA integration including how
 * authorization checks are performed, how row-level security filters and column
 * masking are applied, and how the system handles permission management operations.
 */
public class OpenFgaConfig
{
    private String apiUrl;
    private String storeId;
    private String modelId;
    private Optional<String> apiToken = Optional.empty();
    private String authority;
    private String clientId;
    private String clientSecret;
    private boolean logRequests;
    private boolean logResponses;
    private boolean allowPermissionManagementOperations;
    private Map<String, String> filterExpressions = Map.of();
    private Map<String, String> maskExpressions = Map.of();
    private Optional<String> customModelPath = Optional.empty();
    private Optional<String> schemaVersion = Optional.empty();
    private boolean autoCreateModel;

    /**
     * Returns the configured API URL for the OpenFGA service.
     * <p>
     * This URL is used to establish connections to the OpenFGA server for
     * authorization checks, policy retrieval, and other API operations.
     *
     * @return The OpenFGA API server URL
     */
    @NotNull
    public String getApiUrl()
    {
        return apiUrl;
    }

    @Config("openfga.api.url")
    @ConfigDescription("URL for OpenFGA API server")
    public OpenFgaConfig setApiUrl(String apiUrl)
    {
        this.apiUrl = apiUrl;
        return this;
    }

    /**
     * Returns the configured OpenFGA store ID.
     * <p>
     * Each OpenFGA store represents a separate authorization database with its own
     * set of relations, types, and rules. The store ID identifies which authorization
     * store to use for all operations.
     *
     * @return The OpenFGA store ID
     */
    @NotNull
    public String getStoreId()
    {
        return storeId;
    }

    @Config("openfga.store.id")
    @ConfigDescription("OpenFGA store ID")
    public OpenFgaConfig setStoreId(String storeId)
    {
        this.storeId = storeId;
        return this;
    }

    /**
     * Returns the configured authorization model ID.
     * <p>
     * The model ID identifies a specific version of the authorization model within the store.
     * This allows for versioning of authorization models and ensures that checks are performed
     * against a consistent set of rules.
     *
     * @return The OpenFGA authorization model ID
     */
    @NotNull
    public String getModelId()
    {
        return modelId;
    }

    @Config("openfga.model.id")
    @ConfigDescription("OpenFGA authorization model ID")
    public OpenFgaConfig setModelId(String modelId)
    {
        this.modelId = modelId;
        return this;
    }

    /**
     * Returns the optional API token for authenticating with the OpenFGA service.
     * <p>
     * If provided, this token is used for authenticating all requests to the OpenFGA API.
     * For security reasons, this token should be kept confidential and rotated regularly.
     *
     * @return An Optional containing the API token if configured, or empty if not configured
     */
    public Optional<String> getApiToken()
    {
        return apiToken;
    }

    @Config("openfga.api.token")
    @ConfigDescription("API token for OpenFGA if authentication is required")
    public OpenFgaConfig setApiToken(String apiToken)
    {
        this.apiToken = Optional.ofNullable(apiToken);
        return this;
    }

    /**
     * Returns the OAuth authority URL used for token-based authentication.
     * <p>
     * This setting is used when OAuth authentication is required instead of direct API token
     * authentication. The authority URL is the identity provider that issues access tokens.
     *
     * @return The OAuth authority URL
     */
    public String getAuthority()
    {
        return authority;
    }

    @Config("openfga.auth.authority")
    @ConfigDescription("OAuth authority for OpenFGA API authentication")
    public OpenFgaConfig setAuthority(String authority)
    {
        this.authority = authority;
        return this;
    }

    /**
     * Returns the OAuth client ID used for authentication.
     * <p>
     * The client ID is used in conjunction with the client secret to obtain
     * access tokens from the OAuth authority.
     *
     * @return The OAuth client ID
     */
    public String getClientId()
    {
        return clientId;
    }

    @Config("openfga.auth.client-id")
    @ConfigDescription("OAuth client ID for OpenFGA API authentication")
    public OpenFgaConfig setClientId(String clientId)
    {
        this.clientId = clientId;
        return this;
    }

    /**
     * Returns the OAuth client secret used for authentication.
     * <p>
     * The client secret should be kept secure and not exposed in logs or traces.
     * This is used with the client ID to obtain access tokens.
     *
     * @return The OAuth client secret
     */
    public String getClientSecret()
    {
        return clientSecret;
    }

    @Config("openfga.auth.client-secret")
    @ConfigDescription("OAuth client secret for OpenFGA API authentication")
    public OpenFgaConfig setClientSecret(String clientSecret)
    {
        this.clientSecret = clientSecret;
        return this;
    }

    /**
     * Returns whether requests to the OpenFGA API should be logged.
     * <p>
     * When enabled, all requests sent to the OpenFGA API are logged, which can be
     * useful for debugging but may expose sensitive information in logs.
     *
     * @return true if request logging is enabled, false otherwise
     */
    public boolean getLogRequests()
    {
        return this.logRequests;
    }

    @Config("openfga.log-requests")
    @ConfigDescription("Whether to log requests prior to sending them to OpenFGA")
    public OpenFgaConfig setLogRequests(boolean logRequests)
    {
        this.logRequests = logRequests;
        return this;
    }

    /**
     * Returns whether responses from the OpenFGA API should be logged.
     * <p>
     * When enabled, all responses received from the OpenFGA API are logged.
     * This can be useful for debugging but may include sensitive information.
     *
     * @return true if response logging is enabled, false otherwise
     */
    public boolean getLogResponses()
    {
        return this.logResponses;
    }

    @Config("openfga.log-responses")
    @ConfigDescription("Whether to log responses received from OpenFGA")
    public OpenFgaConfig setLogResponses(boolean logResponses)
    {
        this.logResponses = logResponses;
        return this;
    }

    /**
     * Indicates whether permission management operations are allowed.
     * <p>
     * This setting controls whether operations like GRANT, DENY, REVOKE, and role management
     * are permitted. When disabled, all such operations will be denied regardless of
     * what OpenFGA would return. This provides a way to make the authorization system
     * read-only from the SQL interface.
     *
     * @return true if permission management operations are allowed, false otherwise
     */
    public boolean getAllowPermissionManagementOperations()
    {
        return this.allowPermissionManagementOperations;
    }

    @Config("openfga.allow-permission-management-operations")
    @ConfigDescription("Whether to allow permission management (GRANT, DENY, ...) and role management operations - OpenFGA will not be queried for any such operations, they will be bulk allowed or denied depending on this setting")
    public OpenFgaConfig setAllowPermissionManagementOperations(boolean allowPermissionManagementOperations)
    {
        this.allowPermissionManagementOperations = allowPermissionManagementOperations;
        return this;
    }

    /**
     * Returns the map of row filter expressions by ID.
     * <p>
     * This map contains SQL expressions used for row-level security filtering. The expressions
     * are identified by filter IDs used in the OpenFGA store to assign filters to users.
     * These filters are applied in the SQL engine BEFORE data is fetched from storage,
     * making them highly efficient for restricting access to large datasets.
     *
     * @return A map from filter ID to SQL filter expression
     */
    public Map<String, String> getFilterExpressions()
    {
        return filterExpressions;
    }

    @Config("openfga.filter-expressions")
    @ConfigDescription("Map of filter IDs to SQL expressions for row-level filters")
    public OpenFgaConfig setFilterExpressions(Map<String, String> filterExpressions)
    {
        this.filterExpressions = filterExpressions != null ? filterExpressions : Map.of();
        return this;
    }

    /**
     * Returns the map of column mask expressions by ID.
     * <p>
     * This map contains SQL expressions used for column masking. The expressions
     * are identified by mask IDs used in the OpenFGA store to assign masks to users.
     * When applied, these masks transform sensitive column values before they are
     * returned to users, while preserving the original data in the database.
     *
     * @return A map from mask ID to SQL mask expression
     */
    public Map<String, String> getMaskExpressions()
    {
        return maskExpressions;
    }

    @Config("openfga.mask-expressions")
    @ConfigDescription("Map of mask IDs to SQL expressions for column-level masking")
    public OpenFgaConfig setMaskExpressions(Map<String, String> maskExpressions)
    {
        this.maskExpressions = maskExpressions != null ? maskExpressions : Map.of();
        return this;
    }

    /**
     * Returns the optional path to a custom authorization model definition.
     * <p>
     * If specified, this path points to a YAML file containing an OpenFGA authorization model
     * definition that can be used to create or update the model in the OpenFGA store.
     * This is useful for automating the deployment of authorization models.
     *
     * @return An Optional containing the path if configured, or empty if not configured
     */
    public Optional<String> getCustomModelPath()
    {
        return customModelPath;
    }

    @Config("openfga.model.custom-path")
    @ConfigDescription("Path to a custom OpenFGA authorization model definition file (YAML format)")
    public OpenFgaConfig setCustomModelPath(String customModelPath)
    {
        this.customModelPath = Optional.ofNullable(customModelPath);
        return this;
    }

    /**
     * Returns the optional schema version for the OpenFGA authorization model.
     * <p>
     * The schema version specifies which version of the OpenFGA schema language
     * to use when interpreting or creating authorization models. Different versions
     * may support different features and syntax.
     *
     * @return An Optional containing the schema version if configured, or empty if not configured
     */
    public Optional<String> getSchemaVersion()
    {
        return schemaVersion;
    }

    @Config("openfga.model.schema-version")
    @ConfigDescription("Schema version for OpenFGA authorization models (default: 1.1)")
    public OpenFgaConfig setSchemaVersion(String schemaVersion)
    {
        this.schemaVersion = Optional.ofNullable(schemaVersion);
        return this;
    }

    /**
     * Indicates whether an authorization model should be automatically created.
     * <p>
     * When enabled, the system will automatically create an authorization model in the
     * OpenFGA store if the specified model doesn't exist. This can be useful for initial
     * deployment or testing scenarios.
     *
     * @return true if automatic model creation is enabled, false otherwise
     */
    public boolean getAutoCreateModel()
    {
        return autoCreateModel;
    }

    @Config("openfga.model.auto-create")
    @ConfigDescription("Whether to automatically create an authorization model if none exists or the configured model doesn't exist")
    public OpenFgaConfig setAutoCreateModel(boolean autoCreateModel)
    {
        this.autoCreateModel = autoCreateModel;
        return this;
    }
}
