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

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;

import static java.util.Objects.requireNonNull;

/**
 * A wrapper for the LifeCycleManager to support testing and dependency management.
 * <p>
 * This class encapsulates Trino's LifeCycleManager and provides a level of indirection
 * that allows for easier mocking in unit tests. It also standardizes lifecycle management
 * across the OpenFGA integration, ensuring proper resource cleanup when the plugin is stopped.
 * <p>
 * In production environments, this wrapper delegates directly to the injected LifeCycleManager.
 * In test environments, it can be replaced with a mock implementation to avoid
 * actual lifecycle management operations.
 */
public class LifeCycleManagerWrapper
{
    private final LifeCycleManager lifeCycleManager;

    /**
     * Creates a new LifeCycleManagerWrapper with the specified LifeCycleManager.
     *
     * @param lifeCycleManager The LifeCycleManager to wrap
     * @throws NullPointerException if lifeCycleManager is null
     */
    @Inject
    public LifeCycleManagerWrapper(LifeCycleManager lifeCycleManager)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
    }

    /**
     * Stops all managed objects and transitions to the STOPPED state.
     * <p>
     * This method should be called when the plugin is being shut down to ensure
     * proper resource cleanup, including closing connections to the OpenFGA server.
     */
    public void stop()
    {
        lifeCycleManager.stop();
    }
}
