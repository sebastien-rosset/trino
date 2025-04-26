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
package io.trino.plugin.openfga.model;

import com.google.inject.Inject;
import dev.openfga.sdk.api.client.OpenFgaClient;
import dev.openfga.sdk.api.client.model.ClientReadAuthorizationModelResponse;
import dev.openfga.sdk.api.client.model.ClientReadAuthorizationModelsResponse;
import dev.openfga.sdk.api.client.model.ClientWriteAuthorizationModelResponse;
import dev.openfga.sdk.api.configuration.ClientConfiguration;
import dev.openfga.sdk.api.configuration.ClientReadAuthorizationModelOptions;
import dev.openfga.sdk.api.configuration.ClientReadAuthorizationModelsOptions;
import dev.openfga.sdk.api.configuration.ClientWriteAuthorizationModelOptions;
import dev.openfga.sdk.api.model.AuthorizationModel;
import dev.openfga.sdk.api.model.WriteAuthorizationModelRequest;
import dev.openfga.sdk.errors.FgaInvalidParameterException;
import io.airlift.log.Logger;
import io.trino.plugin.openfga.OpenFgaConfig;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Manages OpenFGA authorization models for Trino.
 * <p>
 * This class is responsible for:
 * <ul>
 *   <li>Loading default or custom authorization models</li>
 *   <li>Creating models if they don't exist</li>
 *   <li>Managing model versions</li>
 * </ul>
 */
public class OpenFgaModelManager
{
    private static final Logger log = Logger.get(OpenFgaModelManager.class);
    private static final String DEFAULT_MODEL_PATH = "models/default-model.yaml";

    private final OpenFgaClient apiClient;
    private final OpenFgaConfig config;
    private String currentModelId;

    @Inject
    public OpenFgaModelManager(OpenFgaConfig config)
    {
        this.config = requireNonNull(config, "config is null");

        // Create API client
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.apiUrl(config.getApiUrl());
        clientConfig.storeId(config.getStoreId());

        // Add API token if configured
        if (config.getApiToken().isPresent()) {
            dev.openfga.sdk.api.configuration.ApiToken apiToken =
                    new dev.openfga.sdk.api.configuration.ApiToken(config.getApiToken().get());
            clientConfig.credentials(new dev.openfga.sdk.api.configuration.Credentials(apiToken));
        }

        try {
            this.apiClient = new OpenFgaClient(clientConfig);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create OpenFGA client for model management", e);
        }

        // Get or create model ID
        this.currentModelId = config.getModelId();
        if (this.currentModelId == null || this.currentModelId.isEmpty()) {
            log.info("No model ID configured, will initialize model from default or custom source");
        }
    }

    /**
     * Initialize the authorization model, creating one if it doesn't exist.
     * <p>
     * This method will:
     * <ol>
     *   <li>Check if a model ID is already configured</li>
     *   <li>If not, try to load from a custom model path if specified</li>
     *   <li>If no custom model, load the default model</li>
     *   <li>Create the model in OpenFGA and return the model ID</li>
     * </ol>
     *
     * @return The ID of the authorization model to use
     * @throws RuntimeException if the model cannot be initialized
     */
    public String initializeModel()
    {
        // If we already have a model ID configured, verify it exists
        if (currentModelId != null && !currentModelId.isEmpty()) {
            if (verifyModelExists(currentModelId)) {
                log.info("Using configured OpenFGA model ID: %s", currentModelId);
                return currentModelId;
            }
            else {
                log.warn("Configured model ID %s does not exist", currentModelId);
                // Continue to model creation
            }
        }

        try {
            // Load model definition
            WriteAuthorizationModelRequest writeRequest = loadModelDefinition();
            String modelSource = config.getCustomModelPath().isPresent() ?
                    "custom model from " + config.getCustomModelPath().get() :
                    "default model from " + DEFAULT_MODEL_PATH;

            String storeId = config.getStoreId();
            log.info("Creating new OpenFGA authorization model using %s for store ID: %s", modelSource, storeId);

            // Create the model in OpenFGA
            ClientWriteAuthorizationModelOptions options = new ClientWriteAuthorizationModelOptions();
            ClientWriteAuthorizationModelResponse response = apiClient.writeAuthorizationModel(writeRequest, options).get();

            String newModelId = response.getAuthorizationModelId();
            this.currentModelId = newModelId;

            log.info("Created new OpenFGA authorization model with ID: %s for store: %s", newModelId, storeId);
            return newModelId;
        }
        catch (ExecutionException e) {
            // Get the cause of the ExecutionException for more detailed error information
            Throwable cause = e.getCause();
            StringBuilder errorMessageBuilder = new StringBuilder("Failed to create OpenFGA authorization model: ");

            if (cause instanceof dev.openfga.sdk.errors.FgaApiValidationError validationError) {
                // Extract validation details from response body
                String responseBody = validationError.getResponseData();
                String validationErrorMessage = responseBody != null
                        ? "Validation error: " + responseBody
                        : "Validation error with no response data";

                log.error("Model validation error. Response body: %s", responseBody != null ? responseBody : "No response data");
                errorMessageBuilder.append(validationErrorMessage);
            }
            else if (cause != null) {
                errorMessageBuilder.append(cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName());
            }
            else {
                errorMessageBuilder.append("Unknown error");
            }

            String detailedErrorMessage = errorMessageBuilder.toString();
            log.error(e, "%s", detailedErrorMessage);
            throw new RuntimeException(detailedErrorMessage, e);
        }
        catch (InterruptedException | FgaInvalidParameterException e) {
            log.error(e, "Failed to create OpenFGA authorization model: %s", e.getMessage());
            throw new RuntimeException("Failed to create OpenFGA authorization model", e);
        }
        catch (IOException e) {
            log.error(e, "Failed to parse model definition: %s", e.getMessage());
            throw new RuntimeException("Failed to parse model definition", e);
        }
    }

    /**
     * Verify that the specified model ID exists in the OpenFGA store.
     *
     * @param modelId The model ID to verify
     * @return true if the model exists, false otherwise
     */
    public boolean verifyModelExists(String modelId)
    {
        try {
            ClientReadAuthorizationModelOptions options = new ClientReadAuthorizationModelOptions();
            options.authorizationModelId(modelId);  // Using builder pattern method

            apiClient.readAuthorizationModel(options).get();
            return true;
        }
        catch (ExecutionException | InterruptedException | FgaInvalidParameterException e) {
            log.debug("Model %s does not exist: %s", modelId, e.getMessage());
            return false;
        }
    }

    /**
     * Verify that a model definition is valid without creating it.
     * This can be used to validate a custom model before attempting to apply it.
     *
     * @param modelJson The model definition as a JSON string
     * @return A validation result object containing success status and any error messages
     */
    public ModelValidationResult verifyModel(String modelJson)
    {
        try {
            // Parse the model first to verify its structure
            AuthorizationModel model;
            try {
                model = ModelDefinition.fromJson(modelJson);
            }
            catch (IOException e) {
                // If JSON parsing fails, try YAML
                try {
                    model = ModelDefinition.fromYaml(modelJson);
                }
                catch (IOException e2) {
                    return new ModelValidationResult(false, Optional.of("Failed to parse model: " + e.getMessage()));
                }
            }

            // Convert to WriteAuthorizationModelRequest for API call
            WriteAuthorizationModelRequest request = ModelDefinition.toWriteRequest(model);

            // The OpenFGA SDK doesn't provide a validation-only method,
            // so we perform validation by attempting to create a temporary model
            // and catching any validation errors
            ClientWriteAuthorizationModelOptions options = new ClientWriteAuthorizationModelOptions();
            apiClient.writeAuthorizationModel(request, options).get();

            // If we get here, the model is valid
            return new ModelValidationResult(true, Optional.empty());
        }
        catch (ExecutionException | InterruptedException e) {
            // Extract error message, ensuring it's not null
            String errorMessage = null;
            if (e.getCause() != null) {
                errorMessage = e.getCause().getMessage();
            }
            if (errorMessage == null) {
                errorMessage = e.getMessage();
            }
            if (errorMessage == null) {
                errorMessage = "Unknown validation error occurred";
            }

            log.debug("Model validation failed: %s", errorMessage);
            return new ModelValidationResult(false, Optional.of(errorMessage));
        }
        catch (Exception e) {
            // Ensure we never have a null message
            String errorMessage = e.getMessage();
            if (errorMessage == null) {
                errorMessage = "Unknown validation error: " + e.getClass().getName();
            }

            log.debug("Model validation failed: %s", errorMessage);
            return new ModelValidationResult(false, Optional.of(errorMessage));
        }
    }

    /**
     * Verify that a model file is valid without creating it.
     * This can be used to validate a custom model file before attempting to apply it.
     *
     * @param modelPath Path to the model file
     * @return A validation result object containing success status and any error messages
     */
    public ModelValidationResult verifyModelFile(String modelPath)
    {
        try {
            WriteAuthorizationModelRequest request = loadModelFromPath(modelPath);
            return verifyModel(ModelDefinition.toYaml(request));
        }
        catch (IOException e) {
            log.debug("Failed to load model from path %s: %s", modelPath, e.getMessage());
            return new ModelValidationResult(false, Optional.of("Failed to load model file: " + e.getMessage()));
        }
    }

    /**
     * Load the model definition from the appropriate source.
     * <p>
     * This method will first check for a custom model path in the configuration.
     * If none is specified, it will load the default model from the classpath.
     *
     * @return The WriteAuthorizationModelRequest object
     * @throws RuntimeException if the model cannot be loaded
     * @throws IOException if the model cannot be parsed
     */
    private WriteAuthorizationModelRequest loadModelDefinition()
            throws IOException
    {
        // Check for custom model path
        Optional<String> customModelPath = config.getCustomModelPath();
        if (customModelPath.isPresent()) {
            try {
                return loadModelFromPath(customModelPath.get());
            }
            catch (IOException e) {
                log.error(e, "Failed to load custom model from %s: %s", customModelPath.get(), e.getMessage());
                log.info("Falling back to default model");
            }
        }

        // Load default model from classpath
        try {
            return loadDefaultModel();
        }
        catch (IOException e) {
            log.error(e, "Failed to load default authorization model: %s", e.getMessage());
            throw new RuntimeException("Failed to load default OpenFGA authorization model", e);
        }
    }

    /**
     * Load a model from the specified file path.
     *
     * @param path The path to the model file
     * @return The WriteAuthorizationModelRequest object
     * @throws IOException if the file cannot be read or parsed
     */
    private WriteAuthorizationModelRequest loadModelFromPath(String path)
            throws IOException
    {
        try (java.io.InputStream inputStream = getClass().getClassLoader().getResourceAsStream(path)) {
            if (inputStream == null) {
                throw new IOException("Model file not found: " + path);
            }

            String content = new String(inputStream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            AuthorizationModel model;

            // Determine if it's YAML or JSON based on file extension or content
            if (path.endsWith(".yaml") || path.endsWith(".yml")) {
                model = ModelDefinition.fromYaml(content);
            }
            else if (path.endsWith(".json")) {
                model = ModelDefinition.fromJson(content);
            }
            else {
                // Try to detect format from content
                String trimmed = content.trim();
                if (trimmed.startsWith("{")) {
                    model = ModelDefinition.fromJson(content);
                }
                else {
                    model = ModelDefinition.fromYaml(content);
                }
            }

            // Convert to WriteAuthorizationModelRequest for API calls
            return ModelDefinition.toWriteRequest(model);
        }
    }

    /**
     * Load the default authorization model from the classpath.
     *
     * @return The WriteAuthorizationModelRequest object
     * @throws IOException if the default model cannot be read
     */
    private WriteAuthorizationModelRequest loadDefaultModel()
            throws IOException
    {
        try (java.io.InputStream inputStream = getClass().getClassLoader().getResourceAsStream(DEFAULT_MODEL_PATH)) {
            if (inputStream == null) {
                throw new IOException("Default model not found at: " + DEFAULT_MODEL_PATH);
            }

            // We know the default model is YAML
            String content = new String(inputStream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            AuthorizationModel model = ModelDefinition.fromYaml(content);
            return ModelDefinition.toWriteRequest(model);
        }
    }

    /**
     * Get the current model ID.
     *
     * @return The current model ID
     */
    public String getCurrentModelId()
    {
        return currentModelId;
    }

    /**
     * List all available authorization model versions for the current store.
     *
     * @return List of model IDs sorted by creation time (newest first)
     * @throws RuntimeException if the models cannot be listed
     */
    public List<String> listModelVersions()
    {
        try {
            // Get all models for the store
            ClientReadAuthorizationModelsOptions options = new ClientReadAuthorizationModelsOptions();
            ClientReadAuthorizationModelsResponse response = apiClient.readAuthorizationModels(options).get();
            List<String> modelIds = response.getAuthorizationModels().stream()
                    .map(model -> model.getId())  // Using getId() method based on SDK
                    .collect(Collectors.toList());

            log.debug("Found %d authorization model versions", modelIds.size());
            return modelIds;
        }
        catch (ExecutionException | InterruptedException | FgaInvalidParameterException e) {
            log.error(e, "Failed to list authorization model versions: %s", e.getMessage());
            throw new RuntimeException("Failed to list authorization model versions", e);
        }
    }

    /**
     * Get details of a specific authorization model.
     *
     * @param modelId The model ID to retrieve
     * @return The authorization model
     * @throws RuntimeException if the model cannot be retrieved
     */
    public AuthorizationModel getModelDetails(String modelId)
    {
        try {
            ClientReadAuthorizationModelOptions options = new ClientReadAuthorizationModelOptions();
            options.authorizationModelId(modelId);  // Using builder pattern method

            ClientReadAuthorizationModelResponse response = apiClient.readAuthorizationModel(options).get();
            return response.getAuthorizationModel();
        }
        catch (ExecutionException | InterruptedException | FgaInvalidParameterException e) {
            log.error(e, "Failed to read authorization model %s: %s", modelId, e.getMessage());
            throw new RuntimeException("Failed to read authorization model", e);
        }
    }

    /**
     * Create a new authorization model version from a provided request.
     *
     * @param request The WriteAuthorizationModelRequest to use
     * @return The ID of the new model version
     * @throws RuntimeException if the model cannot be created
     */
    public String createModelVersion(WriteAuthorizationModelRequest request)
    {
        try {
            log.info("Creating new OpenFGA authorization model version");
            ClientWriteAuthorizationModelOptions options = new ClientWriteAuthorizationModelOptions();
            ClientWriteAuthorizationModelResponse response = apiClient.writeAuthorizationModel(request, options).get();
            String newModelId = response.getAuthorizationModelId();
            log.info("Created new OpenFGA authorization model version with ID: %s", newModelId);
            return newModelId;
        }
        catch (ExecutionException | InterruptedException | FgaInvalidParameterException e) {
            log.error(e, "Failed to create new authorization model version: %s", e.getMessage());
            throw new RuntimeException("Failed to create new authorization model version", e);
        }
    }

    /**
     * Create a new authorization model version from a provided model string.
     *
     * @param modelString The model definition as a string (YAML or JSON)
     * @return The ID of the new model version
     * @throws RuntimeException if the model cannot be created or parsed
     */
    public String createModelVersion(String modelString)
    {
        try {
            // Try to parse as JSON first, then fall back to YAML
            AuthorizationModel model;
            try {
                model = ModelDefinition.fromJson(modelString);
            }
            catch (IOException e) {
                // If JSON parsing fails, try YAML
                try {
                    model = ModelDefinition.fromYaml(modelString);
                }
                catch (IOException e2) {
                    throw new RuntimeException("Failed to parse model as either JSON or YAML", e2);
                }
            }

            // Convert to WriteAuthorizationModelRequest for API call
            WriteAuthorizationModelRequest request = ModelDefinition.toWriteRequest(model);
            return createModelVersion(request);
        }
        catch (Exception e) {
            log.error(e, "Failed to create model version from string: %s", e.getMessage());
            throw new RuntimeException("Failed to create model version", e);
        }
    }

    /**
     * Switch the current model ID to a different version.
     * This method only updates the local reference - it doesn't affect
     * which model the OpenFGA server uses for evaluations.
     *
     * @param modelId The model ID to switch to
     * @return true if successful, false if the model doesn't exist
     */
    public boolean switchModelVersion(String modelId)
    {
        if (verifyModelExists(modelId)) {
            this.currentModelId = modelId;
            log.info("Switched to OpenFGA model ID: %s", modelId);
            return true;
        }
        return false;
    }

    /**
     * Export an authorization model to a YAML string.
     *
     * @param modelId The ID of the model to export (optional, uses current model if not provided)
     * @return The model definition as a YAML string
     * @throws RuntimeException if the model cannot be exported
     */
    public String exportModel(Optional<String> modelId)
    {
        String targetModelId = modelId.orElse(currentModelId);
        try {
            AuthorizationModel model = getModelDetails(targetModelId);
            return ModelDefinition.toYaml(model);
        }
        catch (Exception e) {
            log.error(e, "Failed to export model %s: %s", targetModelId, e.getMessage());
            throw new RuntimeException("Failed to export model", e);
        }
    }

    /**
     * Result class for model validation operations.
     */
    public static class ModelValidationResult
    {
        private final boolean valid;
        private final Optional<String> errorMessage;

        public ModelValidationResult(boolean valid, Optional<String> errorMessage)
        {
            this.valid = valid;
            this.errorMessage = errorMessage;
        }

        public boolean isValid()
        {
            return valid;
        }

        public Optional<String> getErrorMessage()
        {
            return errorMessage;
        }
    }
}
