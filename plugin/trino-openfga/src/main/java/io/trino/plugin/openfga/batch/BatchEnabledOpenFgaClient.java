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
package io.trino.plugin.openfga.batch;

import io.trino.plugin.openfga.model.OpenFgaContext;

import java.util.Set;

/**
 * Interface for OpenFGA clients that support batch operations.
 * This allows for more efficient permission checks when multiple similar operations
 * need to be performed at once, such as filtering multiple catalogs or schemas.
 */
public interface BatchEnabledOpenFgaClient
{
    /**
     * Filter a set of catalogs based on the given permission relation
     *
     * @param context The OpenFGA context containing user identity information
     * @param catalogs Set of catalog names to filter
     * @param relation The relation to check (e.g., "viewer", "admin")
     * @return A filtered set containing only catalogs the user has access to
     */
    Set<String> filterCatalogs(OpenFgaContext context, Set<String> catalogs, String relation);

    /**
     * Filter a set of schemas within a catalog based on the given permission relation
     *
     * @param context The OpenFGA context containing user identity information
     * @param catalogName The catalog containing these schemas
     * @param schemas Set of schema names to filter
     * @param relation The relation to check (e.g., "viewer", "admin")
     * @return A filtered set containing only schemas the user has access to
     */
    Set<String> filterSchemas(OpenFgaContext context, String catalogName, Set<String> schemas, String relation);
}
