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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dev.openfga.sdk.api.model.AuthorizationModel;
import io.airlift.log.Logger;
import io.trino.plugin.openfga.OpenFgaConfig;
import io.trino.plugin.openfga.model.OpenFgaModelManager.ModelValidationResult;

import java.io.IOException;
import java.io.InputStream;

/**
 * Provides test model definitions for OpenFGA tests.
 */
public class TestingModelDefinition
{
    private static final Logger log = Logger.get(TestingModelDefinition.class);
    private static final String DEFAULT_MODEL_PATH = "/models/default-model.yaml";

    // Private constructor to prevent instantiation of utility class
    private TestingModelDefinition() {}

    /**
     * Returns a default model definition in JSON format for testing purposes.
     * This loads the default model from the resource file used in production code.
     *
     * @throws RuntimeException if the default model cannot be loaded
     */
    public static String getDefaultModelJson()
    {
        try (InputStream is = TestingModelDefinition.class.getResourceAsStream(DEFAULT_MODEL_PATH)) {
            if (is == null) {
                throw new RuntimeException("Default model resource not found at " + DEFAULT_MODEL_PATH);
            }

            // Load YAML and convert to JSON
            ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
            Object modelObject = yamlMapper.readValue(is, Object.class);

            // Convert to JSON string
            ObjectMapper jsonMapper = new ObjectMapper();
            return jsonMapper.writeValueAsString(modelObject);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to load default model from " + DEFAULT_MODEL_PATH, e);
        }
    }

    /**
     * Returns a model definition in JSON format loaded using OpenFgaModelManager.
     * This uses the same model loading logic as the production code.
     *
     * @throws RuntimeException if the model cannot be loaded
     */
    public static String getModelJsonViaManager()
    {
        try {
            // Just load the model directly without attempting to create a version on a server
            String defaultModelJson = getDefaultModelJson();

            // Parse and validate it without making API calls
            AuthorizationModel model = ModelDefinition.fromJson(defaultModelJson);

            // If we reach here, the model is valid
            return defaultModelJson;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to load model via OpenFgaModelManager", e);
        }
    }

    /**
     * Validates a model definition string using the OpenFgaModelManager.
     *
     * @param modelString The model definition string (YAML or JSON)
     * @return true if the model is valid, false otherwise
     */
    public static boolean validateModel(String modelString)
    {
        try {
            // Create a minimal config for the model manager
            OpenFgaConfig config = new OpenFgaConfig()
                    .setApiUrl("http://localhost:8080")  // Not used in TestingOpenFgaModelManager
                    .setStoreId("test-store-id");        // Not used in TestingOpenFgaModelManager

            // Create the testing model manager that doesn't make real API calls
            TestingOpenFgaModelManager modelManager = new TestingOpenFgaModelManager(config);

            // Validate the model
            ModelValidationResult result = modelManager.verifyModel(modelString);

            if (!result.isValid()) {
                log.warn("Model validation failed: %s", result.getErrorMessage().orElse("Unknown error"));
            }

            return result.isValid();
        }
        catch (Exception e) {
            log.warn(e, "Model validation failed with exception: %s", e.getMessage());
            return false;
        }
    }
}
