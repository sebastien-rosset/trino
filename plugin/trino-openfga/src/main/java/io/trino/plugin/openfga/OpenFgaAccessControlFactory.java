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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.plugin.openfga.model.OpenFgaContext;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemAccessControlFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Factory class for creating the OpenFGA access control implementation.
 * <p>
 * This class is responsible for instantiating the OpenFgaAccessControl system
 * when Trino loads the OpenFGA plugin. It's the entry point for the OpenFGA
 * integration as defined in the access-control.properties configuration file:
 * <pre>
 * access-control.name=openfga
 * </pre>
 * <p>
 * The factory sets up dependency injection using Airlift's Bootstrap to wire
 * all required components and configurations together. This includes:
 * <ul>
 *   <li>Loading and parsing configuration properties</li>
 *   <li>Setting up the OpenFgaContext with system information</li>
 *   <li>Initializing the OpenFGA client and connection to the OpenFGA server</li>
 *   <li>Creating the access control implementation that enforces authorization decisions</li>
 * </ul>
 * <p>
 * During initialization, the system uses a temporary system identity until real user
 * identities are available during query processing. This enables the plugin to start
 * properly even before processing any user queries.
 */
public class OpenFgaAccessControlFactory
        implements SystemAccessControlFactory
{
    /**
     * Returns the name of this access control factory.
     * <p>
     * This name must match the value specified in the
     * access-control.name property in the Trino configuration.
     * It identifies this implementation when multiple access control
     * factories might be available in the Trino system.
     *
     * @return The name "openfga" to identify this access control implementation
     */
    @Override
    public String getName()
    {
        return "openfga";
    }

    /**
     * Creates a new SystemAccessControl instance configured with the provided properties.
     * <p>
     * This method is called by Trino during startup when loading the access control system.
     * It initializes all required dependencies through dependency injection and returns
     * a fully configured OpenFgaAccessControl instance that will enforce access control
     * decisions based on OpenFGA's fine-grained authorization model.
     *
     * @param config The configuration properties from access-control.properties
     * @param context The system access control context provided by Trino
     * @return A configured OpenFgaAccessControl instance
     * @throws NullPointerException if config or context is null
     */
    @Override
    public SystemAccessControl create(Map<String, String> config, SystemAccessControlContext context)
    {
        requireNonNull(config, "config is null");
        requireNonNull(context, "context is null");

        Bootstrap app = new Bootstrap(
                new JsonModule(),
                binder ->
                {
                    // Create an initial empty identity since we don't have request/query context yet
                    // This will be replaced with actual user identities during query processing
                    Identity identity = Identity.ofUser("system");
                    binder.bind(OpenFgaContext.class).toInstance(new OpenFgaContext(
                            identity,
                            context.getVersion(),
                            0));
                },
                new OpenFgaAccessControlModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();
        return injector.getInstance(SystemAccessControl.class);
    }
}
