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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.spi.security.SystemAccessControl;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Guice module for binding OpenFGA integration components.
 * <p>
 * This module configures the dependency injection for the OpenFGA access control system.
 * It binds the configuration class and all necessary components to implement
 * the Trino SystemAccessControl interface using OpenFGA for fine-grained authorization.
 * <p>
 * The module sets up:
 * <ul>
 *   <li>Configuration binding through OpenFgaConfig to parse properties from the config file</li>
 *   <li>The OpenFgaClient for communicating with OpenFGA authorization servers</li>
 *   <li>Lifecycle management through LifeCycleManagerWrapper for proper resource cleanup</li>
 *   <li>The SystemAccessControl implementation using OpenFgaAccessControl</li>
 * </ul>
 * <p>
 * All components are bound as singletons to ensure consistent state and efficient
 * resource utilization across the Trino system. This is particularly important for
 * the OpenFgaClient which maintains connection pools to the OpenFGA server.
 */
public class OpenFgaAccessControlModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        // Bind configuration from etc/access-control.properties to OpenFgaConfig class
        configBinder(binder).bindConfig(OpenFgaConfig.class);

        // Bind the OpenFGA client as a singleton for efficient connection pooling
        // and consistent authorization decisions across requests
        binder.bind(OpenFgaClient.class).in(Scopes.SINGLETON);

        // Bind lifecycle manager wrapper for proper resource management
        // during plugin shutdown
        binder.bind(LifeCycleManagerWrapper.class).in(Scopes.SINGLETON);

        // Bind our implementation as the SystemAccessControl provider
        // This is the main entry point for Trino's authorization checks
        binder.bind(SystemAccessControl.class).to(OpenFgaAccessControl.class).in(Scopes.SINGLETON);
    }
}
