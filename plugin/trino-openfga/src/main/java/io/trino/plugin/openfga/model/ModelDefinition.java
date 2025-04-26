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
import dev.openfga.sdk.api.model.ObjectRelation;
import dev.openfga.sdk.api.model.TupleToUserset;
import dev.openfga.sdk.api.model.TypeDefinition;
import dev.openfga.sdk.api.model.TypeName;
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
     * Convert an AuthorizationModel to an OpenFGA DSL string representation.
     * <p>
     * The DSL format is a human-readable text format for defining OpenFGA authorization models.
     * This is the format used in the OpenFGA documentation and modeling tools.
     *
     * @param model The AuthorizationModel to convert
     * @return The DSL string representation
     * @throws IOException if conversion fails
     */
    public static String toDSL(AuthorizationModel model)
            throws IOException
    {
        log.debug("Converting AuthorizationModel to DSL format");
        StringBuilder dsl = new StringBuilder();

        // Add model header with schema version if present
        if (model.getSchemaVersion() != null && !model.getSchemaVersion().isEmpty()) {
            dsl.append("model\n");
            dsl.append("  schema ").append(model.getSchemaVersion()).append("\n");
        }
        else {
            dsl.append("model\n");
        }

        // Process type definitions
        if (model.getTypeDefinitions() != null && !model.getTypeDefinitions().isEmpty()) {
            log.debug("Processing %d type definitions for DSL output", model.getTypeDefinitions().size());

            for (TypeDefinition typeDef : model.getTypeDefinitions()) {
                dsl.append("\n");
                dsl.append("type ").append(typeDef.getType()).append("\n");

                // Process relations
                if (typeDef.getRelations() != null && !typeDef.getRelations().isEmpty()) {
                    for (Map.Entry<String, dev.openfga.sdk.api.model.Userset> relation : typeDef.getRelations().entrySet()) {
                        String relationName = relation.getKey();
                        dev.openfga.sdk.api.model.Userset userset = relation.getValue();

                        dsl.append("  relations\n");
                        dsl.append("    ").append(relationName).append(": ");

                        // Convert the userset to DSL syntax
                        dsl.append(usersetToDSL(userset));
                        dsl.append("\n");
                    }
                }

                // Process metadata if present
                if (typeDef.getMetadata() != null) {
                    dsl.append("  metadata\n");

                    // Relations metadata
                    if (typeDef.getMetadata().getRelations() != null && !typeDef.getMetadata().getRelations().isEmpty()) {
                        dsl.append("    relations\n");

                        for (Map.Entry<String, dev.openfga.sdk.api.model.RelationMetadata> metadata :
                                typeDef.getMetadata().getRelations().entrySet()) {
                            String relationName = metadata.getKey();
                            dev.openfga.sdk.api.model.RelationMetadata relationMetadata = metadata.getValue();

                            dsl.append("      ").append(relationName).append("\n");

                            if (relationMetadata.getDirectlyRelatedUserTypes() != null) {
                                dsl.append("        directly_related_user_types: ");
                                dsl.append(directlyRelatedUserTypesToDSL(relationMetadata.getDirectlyRelatedUserTypes()));
                                dsl.append("\n");
                            }
                        }
                    }
                }
            }
        }

        // Process conditions if present
        if (model.getConditions() != null && !model.getConditions().isEmpty()) {
            dsl.append("\n");
            dsl.append("conditions\n");

            for (Map.Entry<String, Condition> entry : model.getConditions().entrySet()) {
                String conditionName = entry.getKey();
                Condition condition = entry.getValue();

                dsl.append("  ").append(conditionName).append(" ");
                dsl.append("[").append(conditionParamsToString(condition)).append("] ");

                if (condition.getExpression() != null) {
                    String expression = condition.getExpression();
                    // Escape any quotes in the expression
                    expression = expression.replace("\"", "\\\"");
                    dsl.append("\"").append(expression).append("\"");
                }

                dsl.append("\n");
            }
        }

        log.debug("Successfully generated DSL representation of the authorization model");
        return dsl.toString();
    }

    /**
     * Convert a WriteAuthorizationModelRequest to OpenFGA DSL format.
     *
     * @param request The WriteAuthorizationModelRequest to convert
     * @return The DSL representation
     * @throws IOException if conversion fails
     */
    public static String toDSL(WriteAuthorizationModelRequest request)
            throws IOException
    {
        // For the request, we can create a temporary AuthorizationModel
        AuthorizationModel model = new AuthorizationModel();
        model.setId("temporary");
        model.setSchemaVersion(request.getSchemaVersion());
        model.setTypeDefinitions(request.getTypeDefinitions());
        model.setConditions(request.getConditions());

        // Use the AuthorizationModel version of toDSL
        return toDSL(model);
    }

    /**
     * Convert a Userset to its DSL string representation
     */
    private static String usersetToDSL(dev.openfga.sdk.api.model.Userset userset)
    {
        if (userset == null) {
            return "nil";
        }

        // Handle direct (this) references
        if (userset.getThis() != null) {
            return "self";
        }

        // Handle computed usersets
        if (userset.getComputedUserset() != null) {
            ObjectRelation computed = userset.getComputedUserset();
            return computed.getObject() + "#" + computed.getRelation();
        }

        // Handle tuple to userset
        if (userset.getTupleToUserset() != null) {
            TupleToUserset tupleToUserset = userset.getTupleToUserset();

            // In the SDK model, TupleToUserset has tupleset and computedUserset fields
            ObjectRelation tupleset = tupleToUserset.getTupleset();
            ObjectRelation computedUserset = tupleToUserset.getComputedUserset();

            if (tupleset != null && computedUserset != null) {
                // The DSL format for this is "relation from object#relation"
                return tupleset.getRelation() + " from " + computedUserset.getObject() + "#" + computedUserset.getRelation();
            }
        }

        // Handle union
        if (userset.getUnion() != null && userset.getUnion().getChild() != null) {
            List<dev.openfga.sdk.api.model.Userset> children = userset.getUnion().getChild();
            return children.stream()
                    .map(ModelDefinition::usersetToDSL)
                    .collect(java.util.stream.Collectors.joining(" or "));
        }

        // Handle intersection
        if (userset.getIntersection() != null && userset.getIntersection().getChild() != null) {
            List<dev.openfga.sdk.api.model.Userset> children = userset.getIntersection().getChild();
            return children.stream()
                    .map(ModelDefinition::usersetToDSL)
                    .collect(java.util.stream.Collectors.joining(" and "));
        }

        // Handle difference
        if (userset.getDifference() != null) {
            dev.openfga.sdk.api.model.Difference difference = userset.getDifference();
            return usersetToDSL(difference.getBase()) + " but not " + usersetToDSL(difference.getSubtract());
        }

        return "";
    }

    /**
     * Convert directly related user types to DSL format
     */
    private static String directlyRelatedUserTypesToDSL(List<dev.openfga.sdk.api.model.RelationReference> types)
    {
        if (types == null || types.isEmpty()) {
            return "[]";
        }

        return types.stream()
                .map(ref -> {
                    StringBuilder sb = new StringBuilder();

                    // Handle type
                    if (ref.getType() != null) {
                        sb.append("{type: \"").append(ref.getType()).append("\"");

                        // Handle relation if present
                        if (ref.getRelation() != null) {
                            sb.append(", relation: \"").append(ref.getRelation()).append("\"");
                        }
                        // Handle wildcard if present
                        else if (ref.getWildcard() != null) {
                            sb.append(", wildcard: {}");
                        }

                        sb.append("}");
                    }

                    return sb.toString();
                })
                .collect(java.util.stream.Collectors.joining(", "));
    }

    /**
     * Convert condition parameters to string representation
     */
    private static String conditionParamsToString(Condition condition)
    {
        if (condition.getParameters() == null || condition.getParameters().isEmpty()) {
            return "";
        }

        return condition.getParameters().entrySet().stream()
                .map(entry -> entry.getKey() + ": " + paramTypeRefToString(entry.getValue()))
                .collect(java.util.stream.Collectors.joining(", "));
    }

    /**
     * Convert a condition parameter type reference to string
     */
    private static String paramTypeRefToString(dev.openfga.sdk.api.model.ConditionParamTypeRef typeRef)
    {
        if (typeRef == null) {
            return "string"; // Default to string for null
        }

        // The SDK uses getTypeName() which returns a TypeName enum
        TypeName typeName = typeRef.getTypeName();
        if (typeName != null) {
            String typeNameStr = typeName.toString();

            switch (typeNameStr) {
                case "BOOLEAN":
                    return "bool";
                case "STRING":
                    return "string";
                case "INT":
                    return "int";
                case "DOUBLE":
                case "FLOAT":
                    return "float";
                case "ARRAY":
                    if (typeRef.getGenericTypes() != null && !typeRef.getGenericTypes().isEmpty()) {
                        return paramTypeRefToString(typeRef.getGenericTypes().get(0)) + "[]";
                    }
                    return "string[]"; // Default array type if generic type isn't specified
                case "MAP":
                    // For map types, we need the key and value types
                    if (typeRef.getGenericTypes() != null && typeRef.getGenericTypes().size() >= 2) {
                        String keyType = paramTypeRefToString(typeRef.getGenericTypes().get(0));
                        String valueType = paramTypeRefToString(typeRef.getGenericTypes().get(1));
                        return "map[" + keyType + "]" + valueType;
                    }
                    return "map[string]string"; // Default map type if generic types aren't specified
            }
        }

        // Default to string if we don't recognize the type
        return "string";
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
