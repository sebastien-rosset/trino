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
import io.airlift.bootstrap.LifeCycleManager;

import static java.util.Objects.requireNonNull;

/**
 * A testing implementation that wraps TestingLifeCycleManager for testing
 */
public class TestingLifeCycleManagerWrapper
        extends LifeCycleManagerWrapper
{
    private final TestingLifeCycleManager testingLifeCycleManager;

    public TestingLifeCycleManagerWrapper(TestingLifeCycleManager testingLifeCycleManager)
    {
        super(createLifeCycleManager());
        this.testingLifeCycleManager = requireNonNull(testingLifeCycleManager, "testingLifeCycleManager is null");
    }

    private static LifeCycleManager createLifeCycleManager()
    {
        // Create a minimal LifeCycleManager using Bootstrap to avoid using non-public classes
        Injector injector = new Bootstrap().quiet().initialize();
        return injector.getInstance(LifeCycleManager.class);
    }

    @Override
    public void stop()
    {
        testingLifeCycleManager.stop();
    }
}
