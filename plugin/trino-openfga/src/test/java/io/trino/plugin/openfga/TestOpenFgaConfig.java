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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationFactory;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenFgaConfig
{
    @Test
    public void testDefaults()
    {
        OpenFgaConfig config = new OpenFgaConfig();

        assertThat(config.getApiUrl()).isNull();
        assertThat(config.getStoreId()).isNull();
        assertThat(config.getModelId()).isNull();
        assertThat(config.getApiToken()).isEmpty();
        assertThat(config.getLogRequests()).isFalse();
        assertThat(config.getLogResponses()).isFalse();
        assertThat(config.getAllowPermissionManagementOperations()).isFalse();
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openfga.api.url", "http://localhost:8080")
                .put("openfga.store.id", "test_store")
                .put("openfga.model.id", "test_model")
                .put("openfga.api.token", "test_token")
                .put("openfga.log-requests", "true")
                .put("openfga.log-responses", "true")
                .put("openfga.allow-permission-management-operations", "true")
                .buildOrThrow();

        OpenFgaConfig expected = new OpenFgaConfig()
                .setApiUrl("http://localhost:8080")
                .setStoreId("test_store")
                .setModelId("test_model")
                .setApiToken("test_token")
                .setLogRequests(true)
                .setLogResponses(true)
                .setAllowPermissionManagementOperations(true);

        ConfigurationFactory factory = new ConfigurationFactory(properties);
        OpenFgaConfig actual = factory.build(OpenFgaConfig.class);

        assertThat(actual.getApiUrl()).isEqualTo(expected.getApiUrl());
        assertThat(actual.getStoreId()).isEqualTo(expected.getStoreId());
        assertThat(actual.getModelId()).isEqualTo(expected.getModelId());
        assertThat(actual.getApiToken()).isEqualTo(expected.getApiToken());
        assertThat(actual.getLogRequests()).isEqualTo(expected.getLogRequests());
        assertThat(actual.getLogResponses()).isEqualTo(expected.getLogResponses());
        assertThat(actual.getAllowPermissionManagementOperations()).isEqualTo(expected.getAllowPermissionManagementOperations());
    }

    @Test
    public void testLoadFromFile()
    {
        // Set required properties
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openfga.api.url", "http://localhost:8080")
                .put("openfga.store.id", "test_store")
                .put("openfga.model.id", "test_model")
                .buildOrThrow();

        ConfigurationFactory configFactory = new ConfigurationFactory(properties);
        OpenFgaConfig config = configFactory.build(OpenFgaConfig.class);

        // Check default values
        assertThat(config.getLogRequests()).isFalse();
        assertThat(config.getLogResponses()).isFalse();
        assertThat(config.getAllowPermissionManagementOperations()).isFalse();
        assertThat(config.getApiToken()).isEmpty();

        // Check required values
        assertThat(config.getApiUrl()).isEqualTo("http://localhost:8080");
        assertThat(config.getStoreId()).isEqualTo("test_store");
        assertThat(config.getModelId()).isEqualTo("test_model");
    }

    @Test
    public void testSettersAndGetters()
    {
        OpenFgaConfig config = new OpenFgaConfig();

        config.setApiUrl("http://test-url:8080");
        assertThat(config.getApiUrl()).isEqualTo("http://test-url:8080");

        config.setStoreId("test_store_id");
        assertThat(config.getStoreId()).isEqualTo("test_store_id");

        config.setModelId("test_model_id");
        assertThat(config.getModelId()).isEqualTo("test_model_id");

        config.setApiToken("test_token");
        assertThat(config.getApiToken()).contains("test_token");

        config.setLogRequests(true);
        assertThat(config.getLogRequests()).isTrue();

        config.setLogResponses(true);
        assertThat(config.getLogResponses()).isTrue();

        config.setAllowPermissionManagementOperations(true);
        assertThat(config.getAllowPermissionManagementOperations()).isTrue();
    }
}
