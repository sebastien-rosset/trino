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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.security.Identity;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenFgaContext
{
    private static final String TEST_USER = "test-user";
    private static final String TEST_VERSION = "test-version";

    @Test
    public void testConstructor()
    {
        Identity identity = Identity.ofUser(TEST_USER);
        OpenFgaContext context = new OpenFgaContext(identity, TEST_VERSION, 0);

        assertThat(context.getUser()).isEqualTo(TEST_USER);
        assertThat(context.getTrinoVersion()).isEqualTo(TEST_VERSION);
    }

    @Test
    public void testUserProperties()
    {
        Map<String, String> properties = ImmutableMap.of("key1", "value1", "key2", "value2");
        Identity identity = Identity.forUser(TEST_USER)
                .withExtraCredentials(properties)
                .build();

        OpenFgaContext context = new OpenFgaContext(identity, TEST_VERSION, 0);

        assertThat(context.getUserProperties()).isEqualTo(properties);
    }

    @Test
    public void testEquals()
    {
        Identity identity1 = Identity.ofUser(TEST_USER);
        Identity identity2 = Identity.ofUser("other-user");

        OpenFgaContext context1 = new OpenFgaContext(identity1, TEST_VERSION, 0);
        OpenFgaContext context2 = new OpenFgaContext(identity1, TEST_VERSION, 0);
        OpenFgaContext context3 = new OpenFgaContext(identity2, TEST_VERSION, 0);
        OpenFgaContext context4 = new OpenFgaContext(identity1, "other-version", 0);
        OpenFgaContext context5 = new OpenFgaContext(identity1, TEST_VERSION, 1);

        // Equality with same values
        assertThat(context1).isEqualTo(context2);
        assertThat(context1.hashCode()).isEqualTo(context2.hashCode());

        // Different user
        assertThat(context1).isNotEqualTo(context3);
        assertThat(context1.hashCode()).isNotEqualTo(context3.hashCode());

        // Different version
        assertThat(context1).isNotEqualTo(context4);
        assertThat(context1.hashCode()).isNotEqualTo(context4.hashCode());

        // Different timestamp
        assertThat(context1).isNotEqualTo(context5);
        assertThat(context1.hashCode()).isNotEqualTo(context5.hashCode());
    }

    @Test
    public void testToString()
    {
        Identity identity = Identity.ofUser(TEST_USER);
        OpenFgaContext context = new OpenFgaContext(identity, TEST_VERSION, 123456789);

        String toString = context.toString();

        // Verify the string contains key information
        assertThat(toString).contains(TEST_USER);
        assertThat(toString).contains(TEST_VERSION);
    }
}
