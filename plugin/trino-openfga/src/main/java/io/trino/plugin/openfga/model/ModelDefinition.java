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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import dev.openfga.sdk.api.model.AuthorizationModel;
import dev.openfga.sdk.api.model.Condition;
import dev.openfga.sdk.api.model.TypeDefinition;
import dev.openfga.sdk.api.model.WriteAuthorizationModelRequest;
import io.airlift.log.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Represents an OpenFGA authorization model definition.
 * <p>
 * This class provides methods to parse YAML/JSON directly into OpenFGA SDK model classes.
 */
public final class ModelDefinition
{
    private static final Logger log = Logger.get(ModelDefinition.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE =
            new TypeReference<>() {};

    /**
     * Private constructor to prevent instantiation of utility class
     */
    private ModelDefinition()
    {
        // Utility class should not be instantiated
    }

    /**
     * Create a WriteAuthorizationModelRequest from a YAML string.
     *
     * @param yamlString The YAML string to parse
     * @return The parsed WriteAuthorizationModelRequest
     * @throws IOException if parsing fails
     */
    public static WriteAuthorizationModelRequest fromYaml(String yamlString)
            throws IOException
    {
        // First convert YAML to a Map with proper type handling
        Yaml yaml = new Yaml();
        // Load as HashMap and then convert using Jackson for proper type handling
        Object yamlObject = yaml.load(yamlString);
        Map<String, Object> modelMap = JSON_MAPPER.convertValue(yamlObject, MAP_TYPE_REFERENCE);

        log.debug("Parsed YAML to map with keys: %s", modelMap.keySet());

        // Handle specific fields that need special treatment
        if (modelMap.containsKey("type_definitions")) {
            Object typeDefsObject = modelMap.get("type_definitions");
            if (typeDefsObject instanceof List<?> typeDefinitions) {
                log.debug("Found %d type definitions in YAML", typeDefinitions.size());

                // Log information about each type definition
                for (Object typeDef : typeDefinitions) {
                    if (typeDef instanceof Map<?, ?> typeDefMap) {
                        // Use ? wildcard for Map keys and values since we don't know the exact types
                        Object typeValue = typeDefMap.get("type");
                        String type = (typeValue != null) ? typeValue.toString() : "unknown";

                        // Log which types have relations and metadata
                        boolean hasRelations = typeDefMap.containsKey("relations");
                        boolean hasMetadata = typeDefMap.containsKey("metadata");

                        log.debug("Type '%s': hasRelations=%s, hasMetadata=%s",
                                type, hasRelations, hasMetadata);

                        // Check if metadata contains relations section
                        if (hasMetadata) {
                            Object metadataObj = typeDefMap.get("metadata");
                            if (metadataObj instanceof Map<?, ?> metadata) {
                                boolean hasRelationsMetadata = metadata.containsKey("relations");
                                log.debug("  Type '%s' metadata: hasRelationsMetadata=%s",
                                        type, hasRelationsMetadata);
                            }
                        }
                    }
                }
            }
        }

        // Then convert Map to JSON string
        String jsonString = JSON_MAPPER.writeValueAsString(modelMap);

        log.debug("Converted Map to JSON: %s", jsonString.substring(0, Math.min(200, jsonString.length())) + "...");

        try {
            // Parse JSON string to WriteAuthorizationModelRequest
            WriteAuthorizationModelRequest request = JSON_MAPPER.readValue(jsonString, WriteAuthorizationModelRequest.class);

            // Validate the request has required fields
            if (request.getTypeDefinitions() == null || request.getTypeDefinitions().isEmpty()) {
                log.warn("Authorization model has no type definitions after parsing");
            }
            else {
                log.debug("Successfully parsed authorization model with %d type definitions",
                        request.getTypeDefinitions().size());

                // Log the first few type definitions to ensure they're properly structured
                int count = 0;
                for (TypeDefinition typeDef : request.getTypeDefinitions()) {
                    if (count < 3) {  // Limit to first 3 for brevity
                        log.debug("TypeDef[%d]: type=%s, relations=%d",
                                count, typeDef.getType(),
                                typeDef.getRelations() != null ? typeDef.getRelations().size() : 0);
                        count++;
                    }
                }
            }

            return request;
        }
        catch (Exception e) {
            log.error(e, "Failed to parse JSON to WriteAuthorizationModelRequest");
            throw e;
        }
    }

    /**
     * Create a WriteAuthorizationModelRequest from a JSON string.
     *
     * @param jsonString The JSON string to parse
     * @return The parsed WriteAuthorizationModelRequest
     * @throws IOException if parsing fails
     */
    public static WriteAuthorizationModelRequest fromJson(String jsonString)
            throws IOException
    {
        return fromJson(jsonString, WriteAuthorizationModelRequest.class);
    }

    /**
     * Create an object of the specified class from a JSON string.
     *
     * @param <T> The type of object to create
     * @param jsonString The JSON string to parse
     * @param clazz The class of the object to create
     * @return The parsed object
     * @throws IOException if parsing fails
     */
    public static <T> T fromJson(String jsonString, Class<T> clazz)
            throws IOException
    {
        return JSON_MAPPER.readValue(jsonString, clazz);
    }

    /**
     * Create an object of the specified type from a JSON string.
     *
     * @param <T> The type of object to create
     * @param jsonString The JSON string to parse
     * @param typeReference The type reference for the object to create
     * @return The parsed object
     * @throws IOException if parsing fails
     */
    public static <T> T fromJson(String jsonString, TypeReference<T> typeReference)
            throws IOException
    {
        return JSON_MAPPER.readValue(jsonString, typeReference);
    }

    /**
     * Convert any object to a JSON string.
     *
     * @param obj The object to convert to JSON
     * @return The JSON representation
     * @throws IOException if conversion fails
     */
    public static String toJson(Object obj)
            throws IOException
    {
        return JSON_MAPPER.writeValueAsString(obj);
    }

    /**
     * Create a WriteAuthorizationModelRequest from a map representation.
     *
     * @param modelMap The map representation of a model definition
     * @return The parsed WriteAuthorizationModelRequest
     * @throws IOException if conversion fails
     */
    public static WriteAuthorizationModelRequest fromMap(Map<String, Object> modelMap)
            throws IOException
    {
        String jsonString = JSON_MAPPER.writeValueAsString(modelMap);
        return JSON_MAPPER.readValue(jsonString, WriteAuthorizationModelRequest.class);
    }

    /**
     * Convert an AuthorizationModel to a YAML string.
     *
     * @param model The AuthorizationModel to convert
     * @return The YAML representation
     * @throws IOException if conversion fails
     */
    public static String toYaml(AuthorizationModel model)
            throws IOException
    {
        // Convert to map with explicit typing
        Map<String, Object> modelMap = JSON_MAPPER.convertValue(model, MAP_TYPE_REFERENCE);

        // Then convert map to YAML
        org.yaml.snakeyaml.DumperOptions options = new org.yaml.snakeyaml.DumperOptions();
        options.setDefaultFlowStyle(org.yaml.snakeyaml.DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(true);
        options.setIndent(2);

        Yaml yaml = new Yaml(options);
        return yaml.dump(modelMap);
    }

    /**
     * Convert a WriteAuthorizationModelRequest to a YAML string.
     *
     * @param request The WriteAuthorizationModelRequest to convert
     * @return The YAML representation
     * @throws IOException if conversion fails
     */
    public static String toYaml(WriteAuthorizationModelRequest request)
            throws IOException
    {
        // Convert to map with explicit typing
        Map<String, Object> modelMap = JSON_MAPPER.convertValue(request, MAP_TYPE_REFERENCE);

        // Then convert map to YAML
        org.yaml.snakeyaml.DumperOptions options = new org.yaml.snakeyaml.DumperOptions();
        options.setDefaultFlowStyle(org.yaml.snakeyaml.DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(true);
        options.setIndent(2);

        Yaml yaml = new Yaml(options);
        return yaml.dump(modelMap);
    }

    /**
     * Get a WriteAuthorizationModelRequest from type definitions and conditions.
     *
     * @param schemaVersion The schema version
     * @param typeDefinitions The list of type definitions
     * @param conditions The map of conditions
     * @return The WriteAuthorizationModelRequest
     */
    public static WriteAuthorizationModelRequest createRequest(
            String schemaVersion,
            List<TypeDefinition> typeDefinitions,
            Map<String, Condition> conditions)
    {
        WriteAuthorizationModelRequest request = new WriteAuthorizationModelRequest();
        request.setSchemaVersion(schemaVersion);
        request.setTypeDefinitions(typeDefinitions);

        if (conditions != null && !conditions.isEmpty()) {
            request.setConditions(conditions);
        }

        return request;
    }

    static {
        // Register any custom deserializers if needed
        SimpleModule module = new SimpleModule();
        // module.addDeserializer(TypeDefinition.class, new TypeDefinitionDeserializer());
        JSON_MAPPER.registerModule(module);
    }
}
