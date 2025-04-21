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
import io.trino.plugin.openfga.OpenFgaConfig;
import io.trino.plugin.openfga.model.OpenFgaModelManager.ModelValidationResult;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the OpenFgaModelManager class.
 */
public class TestOpenFgaModelManager
{
    @Test
    public void testLoadDefaultModel()
            throws Exception
    {
        // Create configuration for model manager
        OpenFgaConfig config = new OpenFgaConfig()
                .setApiUrl("http://localhost:8080")
                .setStoreId("test-store-id");

        // Create testing model manager that doesn't make real API calls
        TestingOpenFgaModelManager modelManager = new TestingOpenFgaModelManager(config);

        // Initialize will load the default model if no model ID is configured
        modelManager.initializeModel();

        // Export the model as YAML and convert it to a ModelDefinition
        String modelYaml = modelManager.exportModel(Optional.empty());
        dev.openfga.sdk.api.model.WriteAuthorizationModelRequest writeRequest =
                ModelDefinition.fromYaml(modelYaml);

        // Verify that model definition was loaded successfully
        assertThat(writeRequest).isNotNull();
        assertThat(writeRequest.getSchemaVersion()).isNotNull();
        assertThat(writeRequest.getTypeDefinitions()).isNotEmpty();

        // Verify that required types are present
        boolean hasUserType = writeRequest.getTypeDefinitions().stream()
                .anyMatch(typeDef -> typeDef.getType().equals("user"));
        boolean hasCatalogType = writeRequest.getTypeDefinitions().stream()
                .anyMatch(typeDef -> typeDef.getType().equals("catalog"));
        boolean hasSchemaType = writeRequest.getTypeDefinitions().stream()
                .anyMatch(typeDef -> typeDef.getType().equals("schema"));
        boolean hasTableType = writeRequest.getTypeDefinitions().stream()
                .anyMatch(typeDef -> typeDef.getType().equals("table"));
        boolean hasColumnType = writeRequest.getTypeDefinitions().stream()
                .anyMatch(typeDef -> typeDef.getType().equals("column"));

        assertThat(hasUserType).as("Model should contain 'user' type").isTrue();
        assertThat(hasCatalogType).as("Model should contain 'catalog' type").isTrue();
        assertThat(hasSchemaType).as("Model should contain 'schema' type").isTrue();
        assertThat(hasTableType).as("Model should contain 'table' type").isTrue();
        assertThat(hasColumnType).as("Model should contain 'column' type").isTrue();
    }

    @Test
    public void testCompareDefaultModelLoading()
            throws Exception
    {
        // Get model using the existing TestingModelDefinition approach
        String originalModelJson = TestingModelDefinition.getDefaultModelJson();

        // Get model using the ModelManager approach
        String managerModelJson = TestingModelDefinition.getModelJsonViaManager();

        // Convert both to maps for comparison (to ignore formatting differences)
        ObjectMapper objectMapper = new ObjectMapper();
        Map<?, ?> originalModel = objectMapper.readValue(originalModelJson, Map.class);
        Map<?, ?> managerModel = objectMapper.readValue(managerModelJson, Map.class);

        // Verify that both approaches load the same model
        assertThat(originalModel).as("Models should be identical regardless of loading method").isEqualTo(managerModel);
    }

    @Test
    public void testModelValidation()
    {
        // Valid model from TestingModelDefinition
        String validModelJson = TestingModelDefinition.getDefaultModelJson();
        boolean isValid = TestingModelDefinition.validateModel(validModelJson);
        assertThat(isValid).as("Default model should be valid").isTrue();

        // Invalid model with missing required fields
        String invalidModelJson = """
                {
                  "schema_version": "1.1",
                  "type_definitions": [
                    {
                      "type": "user"
                    },
                    {
                      "type": "catalog",
                      "relations": {}
                    }
                  ]
                }
                """;
        boolean isInvalid = TestingModelDefinition.validateModel(invalidModelJson);
        assertThat(isInvalid).as("Invalid model should fail validation").isFalse();
    }

    @Test
    public void testDirectVerification()
            throws Exception
    {
        // Create configuration for model manager
        OpenFgaConfig config = new OpenFgaConfig()
                .setApiUrl("http://localhost:8080")
                .setStoreId("test-store-id");

        // Create testing model manager that doesn't make real API calls
        TestingOpenFgaModelManager modelManager = new TestingOpenFgaModelManager(config);

        // Get valid model
        String validModelJson = TestingModelDefinition.getDefaultModelJson();

        // Directly verify the model
        ModelValidationResult validResult = modelManager.verifyModel(validModelJson);
        assertThat(validResult.isValid()).as("Default model should be valid").isTrue();
        assertThat(validResult.getErrorMessage()).as("Valid model should not have error message").isNotPresent();

        // Invalid model
        String invalidModelJson = """
                {
                  "schema_version": "1.1",
                  "type_definitions": [
                    {
                      "relations": {
                        "can_read": {
                          "this": {}
                        }
                      }
                    }
                  ]
                }
                """;

        // Set up the mock to return an invalid result for this model
        modelManager.setModelValidationResult(invalidModelJson, false, Optional.of("Type definition is missing required 'type' field"));

        // Verify invalid model
        ModelValidationResult invalidResult = modelManager.verifyModel(invalidModelJson);
        assertThat(invalidResult.isValid()).as("Invalid model should fail validation").isFalse();
        assertThat(invalidResult.getErrorMessage()).as("Invalid model should have error message").isPresent();
    }
}
