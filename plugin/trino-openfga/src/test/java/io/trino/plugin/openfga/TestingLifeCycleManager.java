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

import java.util.ArrayList;
import java.util.List;

/**
 * A test implementation that wraps LifeCycleManager for testing
 */
public class TestingLifeCycleManager
{
    private final List<Object> instances = new ArrayList<>();
    private boolean stopped;

    public void addInstance(Object instance)
    {
        instances.add(instance);
    }

    public void stop()
    {
        stopped = true;
    }

    public boolean isStopped()
    {
        return stopped;
    }

    public List<Object> getInstances()
    {
        return instances;
    }
}
