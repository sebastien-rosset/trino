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

import dev.openfga.sdk.api.client.model.ClientReadAuthorizationModelResponse;
import dev.openfga.sdk.api.client.model.ClientReadAuthorizationModelsResponse;
import dev.openfga.sdk.api.client.model.ClientWriteAuthorizationModelResponse;
import dev.openfga.sdk.api.model.AuthorizationModel;
import dev.openfga.sdk.api.model.TypeDefinition;
import dev.openfga.sdk.api.model.WriteAuthorizationModelRequest;
import dev.openfga.sdk.errors.FgaInvalidParameterException;
import io.airlift.log.Logger;
import io.trino.plugin.openfga.OpenFgaConfig;
import io.trino.plugin.openfga.TestingOpenFgaClient;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A test implementation of OpenFgaModelManager that uses TestingOpenFgaClient
 * to avoid making real API calls.
 */
public class TestingOpenFgaModelManager
        extends OpenFgaModelManager
{
    private static final Logger log = Logger.get(TestingOpenFgaModelManager.class);
    private final Map<String, ModelValidationResult> modelValidationResults = new HashMap<>();
    private final List<String> modelIds = new ArrayList<>();
    private final Map<String, AuthorizationModel> modelDetails = new HashMap<>();
    private final TestingOpenFgaClient testClient;
    private String currentModelJson;
    private String currentModelId = "test-model-id";
    private boolean initializeModelCalled;
    private boolean createModelVersionCalled;
    private int modelCreateAttempts;

    public TestingOpenFgaModelManager(OpenFgaConfig config)
    {
        super(config);

        // Create a test client that will be used by the parent class
        this.testClient = new TestingOpenFgaClient(config);

        // Replace the apiClient field in the parent class with our mock client
        try {
            Field apiClientField = OpenFgaModelManager.class.getDeclaredField("apiClient");
            apiClientField.setAccessible(true);
            apiClientField.set(this, createMockApiClient());
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            log.error(e, "Failed to inject mock API client into parent class");
            throw new RuntimeException("Failed to set up testing model manager", e);
        }

        // Load the default model as our initial model
        try {
            currentModelJson = TestingModelDefinition.getDefaultModelJson();

            // Create initial model ID and details
            modelIds.add(currentModelId);
            modelDetails.put(currentModelId, createMockModelFromJson(currentModelJson));
        }
        catch (Exception e) {
            log.warn(e, "Failed to load default model for testing");
            currentModelJson = "{}";
        }
    }

    /**
     * Create a mock OpenFGA API client that returns predefined responses
     */
    private dev.openfga.sdk.api.client.OpenFgaClient createMockApiClient()
    {
        try {
            // In v0.8.1 we need to create a config object
            dev.openfga.sdk.api.configuration.ClientConfiguration config =
                    new dev.openfga.sdk.api.configuration.ClientConfiguration();
            config.apiUrl("http://localhost");
            config.storeId("test-store-id");

            return new dev.openfga.sdk.api.client.OpenFgaClient(config)
            {
                @Override
                public CompletableFuture<ClientReadAuthorizationModelResponse> readAuthorizationModel(
                        dev.openfga.sdk.api.configuration.ClientReadAuthorizationModelOptions options)
                {
                    // Get the model ID from options
                    String modelId = options.getAuthorizationModelId() != null
                            ? options.getAuthorizationModelId()
                            : currentModelId;

                    if (!modelDetails.containsKey(modelId)) {
                        CompletableFuture<ClientReadAuthorizationModelResponse> future = new CompletableFuture<>();
                        future.completeExceptionally(new RuntimeException("Model not found: " + modelId));
                        return future;
                    }

                    // Create response with the appropriate ApiResponse for v0.8.1
                    Map<String, List<String>> headers = new HashMap<>();
                    dev.openfga.sdk.api.model.ReadAuthorizationModelResponse modelResponse =
                            new dev.openfga.sdk.api.model.ReadAuthorizationModelResponse();
                    modelResponse.setAuthorizationModel(modelDetails.get(modelId));

                    // Create a raw response string
                    String rawResponse = "{}"; // Default empty JSON
                    try {
                        rawResponse = ModelDefinition.toJson(modelResponse);
                    }
                    catch (IOException e) {
                        // Ignore, use empty JSON
                    }

                    dev.openfga.sdk.api.client.ApiResponse<dev.openfga.sdk.api.model.ReadAuthorizationModelResponse> apiResponse =
                            new dev.openfga.sdk.api.client.ApiResponse<>(200, headers, rawResponse, modelResponse);

                    ClientReadAuthorizationModelResponse response = new ClientReadAuthorizationModelResponse(apiResponse);
                    return CompletableFuture.completedFuture(response);
                }

                @Override
                public CompletableFuture<ClientReadAuthorizationModelsResponse> readAuthorizationModels(
                        dev.openfga.sdk.api.configuration.ClientReadAuthorizationModelsOptions options)
                {
                    List<AuthorizationModel> models = new ArrayList<>();
                    for (String id : modelIds) {
                        if (modelDetails.containsKey(id)) {
                            models.add(modelDetails.get(id));
                        }
                    }

                    // Create response with the appropriate ApiResponse for v0.8.1
                    Map<String, List<String>> headers = new HashMap<>();
                    dev.openfga.sdk.api.model.ReadAuthorizationModelsResponse modelsResponse =
                            new dev.openfga.sdk.api.model.ReadAuthorizationModelsResponse();
                    modelsResponse.setAuthorizationModels(models);

                    // Create a raw response string
                    String rawResponse = "{}"; // Default empty JSON
                    try {
                        rawResponse = ModelDefinition.toJson(modelsResponse);
                    }
                    catch (IOException e) {
                        // Ignore, use empty JSON
                    }

                    dev.openfga.sdk.api.client.ApiResponse<dev.openfga.sdk.api.model.ReadAuthorizationModelsResponse> apiResponse =
                            new dev.openfga.sdk.api.client.ApiResponse<>(200, headers, rawResponse, modelsResponse);

                    ClientReadAuthorizationModelsResponse response = new ClientReadAuthorizationModelsResponse(apiResponse);
                    return CompletableFuture.completedFuture(response);
                }

                @Override
                public CompletableFuture<ClientReadAuthorizationModelsResponse> readAuthorizationModels()
                {
                    // Just delegate to the options-based method with null options
                    return readAuthorizationModels(null);
                }

                @Override
                public CompletableFuture<ClientWriteAuthorizationModelResponse> writeAuthorizationModel(
                        WriteAuthorizationModelRequest body)
                {
                    modelCreateAttempts++;
                    createModelVersionCalled = true;

                    // For validation failures
                    if (body.getTypeDefinitions() == null || body.getTypeDefinitions().isEmpty()) {
                        CompletableFuture<ClientWriteAuthorizationModelResponse> future = new CompletableFuture<>();
                        future.completeExceptionally(new RuntimeException("Empty type definitions"));
                        return future;
                    }

                    // Generate a new model ID
                    String newModelId = "test-model-id-" + UUID.randomUUID().toString().substring(0, 8);
                    currentModelId = newModelId;

                    // Store model details
                    try {
                        AuthorizationModel model = createMockModelFromRequest(body, newModelId);
                        modelDetails.put(newModelId, model);
                        modelIds.add(0, newModelId); // Add to front to simulate "newest first"

                        // Store as json for export
                        currentModelJson = ModelDefinition.toJson(body);
                    }
                    catch (IOException e) {
                        CompletableFuture<ClientWriteAuthorizationModelResponse> future = new CompletableFuture<>();
                        future.completeExceptionally(e);
                        return future;
                    }

                    // Create response with the appropriate ApiResponse for v0.8.1
                    Map<String, List<String>> headers = new HashMap<>();
                    dev.openfga.sdk.api.model.WriteAuthorizationModelResponse writeResponse =
                            new dev.openfga.sdk.api.model.WriteAuthorizationModelResponse();
                    writeResponse.setAuthorizationModelId(newModelId);

                    // Create a raw response string
                    String rawResponse = "{}"; // Default empty JSON
                    try {
                        rawResponse = ModelDefinition.toJson(writeResponse);
                    }
                    catch (IOException e) {
                        // Ignore, use empty JSON
                    }

                    dev.openfga.sdk.api.client.ApiResponse<dev.openfga.sdk.api.model.WriteAuthorizationModelResponse> apiResponse =
                            new dev.openfga.sdk.api.client.ApiResponse<>(200, headers, rawResponse, writeResponse);

                    ClientWriteAuthorizationModelResponse response = new ClientWriteAuthorizationModelResponse(apiResponse);
                    return CompletableFuture.completedFuture(response);
                }
            };
        }
        catch (FgaInvalidParameterException e) {
            throw new RuntimeException("Failed to create mock API client", e);
        }
    }

    /**
     * Create a mock AuthorizationModel from a WriteAuthorizationModelRequest
     */
    private AuthorizationModel createMockModelFromRequest(WriteAuthorizationModelRequest request, String modelId)
    {
        AuthorizationModel model = new AuthorizationModel();
        model.setId(modelId);
        model.setTypeDefinitions(request.getTypeDefinitions());
        model.setSchemaVersion(request.getSchemaVersion());
        model.setConditions(request.getConditions());
        return model;
    }

    /**
     * Create a mock AuthorizationModel from JSON
     */
    private AuthorizationModel createMockModelFromJson(String json)
            throws IOException
    {
        WriteAuthorizationModelRequest request = ModelDefinition.fromJson(json);
        return createMockModelFromRequest(request, currentModelId);
    }

    /**
     * Get the underlying TestingOpenFgaClient for configuration and verification
     */
    public TestingOpenFgaClient getTestClient()
    {
        return testClient;
    }

    /**
     * Set a predefined validation result for a specific model JSON
     */
    public void setModelValidationResult(String modelJson, boolean isValid, Optional<String> errorMessage)
    {
        modelValidationResults.put(modelJson, new OpenFgaModelManager.ModelValidationResult(isValid, errorMessage));
    }

    /**
     * Shortcut to mark model validation as failing for any input
     */
    public void setModelValidationAlwaysFails(String errorMessage)
    {
        modelValidationResults.put("*", new OpenFgaModelManager.ModelValidationResult(false, Optional.of(errorMessage)));
    }

    @Override
    public OpenFgaModelManager.ModelValidationResult verifyModelFile(String modelPath)
    {
        // If we have a predefined result for modelPath, return it
        if (modelValidationResults.containsKey(modelPath)) {
            return modelValidationResults.get(modelPath);
        }

        // If we have a wildcard result, return that
        if (modelValidationResults.containsKey("*")) {
            return modelValidationResults.get("*");
        }

        // Otherwise use the parent implementation which will use our mock client
        return super.verifyModelFile(modelPath);
    }

    /**
     * Override to track that initialize was called but still use parent implementation
     */
    @Override
    public String initializeModel()
    {
        initializeModelCalled = true;
        return super.initializeModel();
    }

    /**
     * Gets the current model ID
     */
    @Override
    public String getCurrentModelId()
    {
        return currentModelId;
    }

    /**
     * Gets the current model definition as JSON
     */
    public String getCurrentModelJson()
    {
        return currentModelJson;
    }

    /**
     * Checks if initializeModel was called
     */
    public boolean wasInitializeModelCalled()
    {
        return initializeModelCalled;
    }

    /**
     * Checks if createModelVersion was called
     */
    public boolean wasCreateModelVersionCalled()
    {
        return createModelVersionCalled;
    }

    /**
     * Gets the number of model create attempts
     */
    public int getModelCreateAttempts()
    {
        return modelCreateAttempts;
    }

    /**
     * Add a new mock model to the manager
     */
    public void addMockModel(String modelId, List<TypeDefinition> typeDefinitions)
    {
        AuthorizationModel model = new AuthorizationModel();
        model.setId(modelId);
        model.setTypeDefinitions(typeDefinitions);

        modelDetails.put(modelId, model);
        if (!modelIds.contains(modelId)) {
            modelIds.add(modelId);
        }
    }
}
