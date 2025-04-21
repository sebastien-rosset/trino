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

import java.util.Map;
import java.util.Objects;

/**
 * Context for OpenFGA authorization checks
 */
public class OpenFgaContext
{
    private final Identity identity;
    private final String trinoVersion;
    private final long timestamp;

    public OpenFgaContext(Identity identity, String trinoVersion, long timestamp)
    {
        this.identity = identity;
        this.trinoVersion = trinoVersion;
        this.timestamp = timestamp;
    }

    public Identity getIdentity()
    {
        return identity;
    }

    public String getTrinoVersion()
    {
        return trinoVersion;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    /**
     * Get the username from the identity
     */
    public String getUser()
    {
        return identity != null ? identity.getUser() : null;
    }

    /**
     * Get the user properties from the identity
     */
    public Map<String, String> getUserProperties()
    {
        return identity != null ? ImmutableMap.copyOf(identity.getExtraCredentials()) : ImmutableMap.of();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OpenFgaContext that = (OpenFgaContext) o;
        return timestamp == that.timestamp &&
                Objects.equals(identity, that.identity) &&
                Objects.equals(trinoVersion, that.trinoVersion);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(identity, trinoVersion, timestamp);
    }

    @Override
    public String toString()
    {
        return "OpenFgaContext{" +
                "identity=" + identity +
                ", trinoVersion='" + trinoVersion + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
