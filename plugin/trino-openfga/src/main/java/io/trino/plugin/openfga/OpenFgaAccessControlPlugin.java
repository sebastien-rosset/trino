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

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.security.SystemAccessControlFactory;

/**
 * Entry point for the OpenFGA access control plugin in Trino.
 * <p>
 * This plugin enables Trino to use OpenFGA (Fine Grained Authorization) as an
 * authorization engine for implementing access control policies. It integrates
 * with OpenFGA's relationship-based authorization model to enforce security
 * constraints on Trino resources including catalogs, schemas, tables, columns,
 * and rows.
 * <p>
 * The plugin supports:
 * <ul>
 *   <li>Table, schema, and catalog permission checks</li>
 *   <li>Row-level security filtering</li>
 *   <li>Column masking for sensitive data</li>
 *   <li>Permission management operations</li>
 * </ul>
 */
public class OpenFgaAccessControlPlugin
        implements Plugin
{
    @Override
    public Iterable<SystemAccessControlFactory> getSystemAccessControlFactories()
    {
        return ImmutableList.of(new OpenFgaAccessControlFactory());
    }
}
