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

import io.trino.spi.security.SystemAccessControlFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenFgaAccessControlPlugin
{
    @Test
    public void testGetSystemAccessControlFactories()
    {
        OpenFgaAccessControlPlugin plugin = new OpenFgaAccessControlPlugin();
        Iterable<SystemAccessControlFactory> factories = plugin.getSystemAccessControlFactories();

        // Verify we have at least one factory
        assertThat(factories.iterator().hasNext()).isTrue();

        // Verify the factory is of the correct type
        SystemAccessControlFactory factory = factories.iterator().next();
        assertThat(factory).isInstanceOf(OpenFgaAccessControlFactory.class);

        // Verify factory name
        assertThat(factory.getName()).isEqualTo("openfga");
    }
}
