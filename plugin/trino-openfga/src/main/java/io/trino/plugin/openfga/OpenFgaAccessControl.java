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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.openfga.batch.BatchEnabledOpenFgaClient;
import io.trino.plugin.openfga.model.OpenFgaContext;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.security.AccessDeniedException.denyCommentColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentTable;
import static io.trino.spi.security.AccessDeniedException.denyCommentView;
import static io.trino.spi.security.AccessDeniedException.denyCreateCatalog;
import static io.trino.spi.security.AccessDeniedException.denyCreateRole;
import static io.trino.spi.security.AccessDeniedException.denyCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyCreateView;
import static io.trino.spi.security.AccessDeniedException.denyDeleteTable;
import static io.trino.spi.security.AccessDeniedException.denyDenyTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denyDropCatalog;
import static io.trino.spi.security.AccessDeniedException.denyDropRole;
import static io.trino.spi.security.AccessDeniedException.denyDropSchema;
import static io.trino.spi.security.AccessDeniedException.denyDropTable;
import static io.trino.spi.security.AccessDeniedException.denyExecuteProcedure;
import static io.trino.spi.security.AccessDeniedException.denyExecuteQuery;
import static io.trino.spi.security.AccessDeniedException.denyExecuteTableProcedure;
import static io.trino.spi.security.AccessDeniedException.denyGrantRoles;
import static io.trino.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denyImpersonateUser;
import static io.trino.spi.security.AccessDeniedException.denyInsertTable;
import static io.trino.spi.security.AccessDeniedException.denyKillQuery;
import static io.trino.spi.security.AccessDeniedException.denyReadSystemInformationAccess;
import static io.trino.spi.security.AccessDeniedException.denyRenameSchema;
import static io.trino.spi.security.AccessDeniedException.denyRenameTable;
import static io.trino.spi.security.AccessDeniedException.denyRevokeRoles;
import static io.trino.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denySelectColumns;
import static io.trino.spi.security.AccessDeniedException.denySetEntityAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetSystemSessionProperty;
import static io.trino.spi.security.AccessDeniedException.denySetTableProperties;
import static io.trino.spi.security.AccessDeniedException.denyShowColumns;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyShowSchemas;
import static io.trino.spi.security.AccessDeniedException.denyShowTables;
import static io.trino.spi.security.AccessDeniedException.denyViewQuery;
import static io.trino.spi.security.AccessDeniedException.denyWriteSystemInformationAccess;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of Trino's SystemAccessControl interface using OpenFGA for fine-grained authorization.
 * <p>
 * This class uses OpenFGA to handle authorization decisions for all Trino system-level permissions,
 * including catalog, schema, table, and column access. It translates Trino security contexts into
 * OpenFGA authorization model concepts and performs checks against the configured OpenFGA store.
 */
public class OpenFgaAccessControl
        implements SystemAccessControl
{
    private static final Logger log = Logger.get(OpenFgaAccessControl.class);

    // Use our wrapper instead of directly using LifeCycleManager
    private final LifeCycleManagerWrapper lifeCycleManager;
    private final OpenFgaClient fgaClient;
    private final boolean allowPermissionManagementOperations;
    private final OpenFgaContext context;

    /**
     * Creates a new OpenFgaAccessControl instance.
     *
     * @param lifeCycleManager Manages the lifecycle of components
     * @param fgaClient Client for making OpenFGA authorization requests
     * @param config Configuration for the OpenFGA access control
     * @param context Default context containing Trino version and other shared information
     */
    @Inject
    public OpenFgaAccessControl(LifeCycleManagerWrapper lifeCycleManager, OpenFgaClient fgaClient, OpenFgaConfig config,
            OpenFgaContext context)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.fgaClient = requireNonNull(fgaClient, "fgaClient is null");
        this.allowPermissionManagementOperations = config.getAllowPermissionManagementOperations();
        this.context = requireNonNull(context, "context is null");
    }

    /**
     * Creates an OpenFgaContext from a SystemSecurityContext, preserving identity information.
     *
     * @param securityContext The Trino security context
     * @return An OpenFgaContext containing the relevant identity and system information
     */
    private OpenFgaContext contextFromSecurity(SystemSecurityContext securityContext)
    {
        return new OpenFgaContext(securityContext.getIdentity(), context.getTrinoVersion(), System.currentTimeMillis());
    }

    /**
     * Checks if the identity can impersonate a given user.
     * OpenFGA relation: user#userName can be impersonated by identity
     */
    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        OpenFgaContext ctx = new OpenFgaContext(identity, context.getTrinoVersion(), System.currentTimeMillis());
        if (!fgaClient.check(ctx, "user", userName, "impersonate")) {
            denyImpersonateUser(identity.getUser(), userName);
        }
    }

    /**
     * @deprecated Use {@link #checkCanImpersonateUser}
     */
    @Override
    @Deprecated
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        // If principal is not present, we need to check if any user is allowed to impersonate
        if (!principal.isPresent()) {
            if (!fgaClient.check(context, "user", userName, "impersonate")) {
                throw new AccessDeniedException("Cannot set user to " + userName);
            }
            return;
        }

        // Create identity from principal if it exists
        Identity identity = Identity.forUser(principal.get().getName())
                .withPrincipal(principal.get())
                .build();

        // Delegate to the non-deprecated method with the same security check
        try {
            checkCanImpersonateUser(identity, userName);
        }
        catch (AccessDeniedException e) {
            // Rethrow with the appropriate message for backward compatibility
            throw new AccessDeniedException("Cannot set user to " + userName);
        }
    }

    /**
     * Checks if the identity can execute a query.
     * OpenFGA relation: system#system can be executed by identity
     */
    @Override
    public void checkCanExecuteQuery(Identity identity, QueryId queryId)
    {
        OpenFgaContext ctx = new OpenFgaContext(identity, context.getTrinoVersion(), System.currentTimeMillis());
        if (!fgaClient.check(ctx, "system", "system", "execute_query")) {
            denyExecuteQuery();
        }
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        OpenFgaContext ctx = new OpenFgaContext(identity, context.getTrinoVersion(), System.currentTimeMillis());
        if (!fgaClient.check(ctx, "user", queryOwner.getUser(), "view_query")) {
            denyViewQuery();
        }
    }

    @Override
    public Collection<Identity> filterViewQueryOwnedBy(Identity identity, Collection<Identity> queryOwners)
    {
        OpenFgaContext ctx = new OpenFgaContext(identity, context.getTrinoVersion(), System.currentTimeMillis());
        Set<Identity> allowed = new HashSet<>();

        for (Identity owner : queryOwners) {
            if (fgaClient.check(ctx, "user", owner.getUser(), "view_query")) {
                allowed.add(owner);
            }
        }

        return allowed;
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        OpenFgaContext ctx = new OpenFgaContext(identity, context.getTrinoVersion(), System.currentTimeMillis());
        if (!fgaClient.check(ctx, "user", queryOwner.getUser(), "kill_query")) {
            denyKillQuery();
        }
    }

    @Override
    public void checkCanReadSystemInformation(Identity identity)
    {
        OpenFgaContext ctx = new OpenFgaContext(identity, context.getTrinoVersion(), System.currentTimeMillis());
        if (!fgaClient.check(ctx, "system", "system", "read")) {
            denyReadSystemInformationAccess();
        }
    }

    @Override
    public void checkCanWriteSystemInformation(Identity identity)
    {
        OpenFgaContext ctx = new OpenFgaContext(identity, context.getTrinoVersion(), System.currentTimeMillis());
        if (!fgaClient.check(ctx, "system", "system", "write")) {
            denyWriteSystemInformationAccess();
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, QueryId queryId, String propertyName)
    {
        OpenFgaContext ctx = new OpenFgaContext(identity, context.getTrinoVersion(), System.currentTimeMillis());
        if (!fgaClient.check(ctx, "system_property", propertyName, "set")) {
            denySetSystemSessionProperty(propertyName);
        }
    }

    /**
     * Checks if the identity can access a catalog.
     * OpenFGA relation: catalog#catalogName has viewer relation with identity
     */
    @Override
    public boolean canAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        return fgaClient.checkCatalogAccess(contextFromSecurity(context), catalogName, "viewer");
    }

    /**
     * Checks if the identity can create a catalog.
     * OpenFGA relation: system#system can be used to create a catalog
     */
    @Override
    public void checkCanCreateCatalog(SystemSecurityContext context, String catalog)
    {
        if (!fgaClient.check(contextFromSecurity(context), "system", "system", "create_catalog")) {
            denyCreateCatalog(catalog);
        }
    }

    /**
     * Checks if the identity can drop a catalog.
     * OpenFGA relation: catalog#catalogName has admin relation with identity
     */
    @Override
    public void checkCanDropCatalog(SystemSecurityContext context, String catalog)
    {
        if (!fgaClient.checkCatalogAccess(contextFromSecurity(context), catalog, "admin")) {
            denyDropCatalog(catalog);
        }
    }

    /**
     * Filters catalogs based on OpenFGA permissions. If the client supports batch operations,
     * this will use the more efficient batch filtering process.
     * OpenFGA relation: catalog#catalogName has viewer relation with identity
     */
    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        OpenFgaContext ctx = contextFromSecurity(context);

        // If the client supports batch filtering, use it
        if (fgaClient instanceof BatchEnabledOpenFgaClient) {
            return ((BatchEnabledOpenFgaClient) fgaClient).filterCatalogs(ctx, catalogs, "viewer");
        }

        // Otherwise use the standard approach
        ImmutableSet.Builder<String> allowedCatalogs = ImmutableSet.builder();
        for (String catalogName : catalogs) {
            if (fgaClient.checkCatalogAccess(ctx, catalogName, "viewer")) {
                allowedCatalogs.add(catalogName);
            }
        }
        return allowedCatalogs.build();
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema,
            Map<String, Object> properties)
    {
        if (!fgaClient.checkCatalogAccess(contextFromSecurity(context), schema.getCatalogName(), "admin")) {
            denyCreateSchema(schema.toString());
        }
    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkSchemaAccess(ctx, schema.getCatalogName(), schema.getSchemaName(), "admin")) {
            denyDropSchema(schema.toString());
        }
    }

    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkSchemaAccess(ctx, schema.getCatalogName(), schema.getSchemaName(), "admin")) {
            denyRenameSchema(schema.toString(), newSchemaName);
        }
    }

    /**
     * Checks if an identity can set authorization for various entity types (schema, table, view).
     * For schemas: Requires admin access to the schema
     * For tables and views: Requires admin access to the table
     *
     * @param context Security context containing identity information
     * @param entityKindAndName Entity to check authorization for
     * @param principal Principal to set as the entity owner
     * @throws TrinoException If the entity name format is invalid
     */
    @Override
    public void checkCanSetEntityAuthorization(SystemSecurityContext context, EntityKindAndName entityKindAndName, TrinoPrincipal principal)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        String kind = entityKindAndName.entityKind().toUpperCase(Locale.ENGLISH);
        List<String> name = entityKindAndName.name();

        switch (kind) {
            case "SCHEMA":
                if (name.size() != 2) {
                    throw new TrinoException(INVALID_ARGUMENTS, "The schema name %s must have two elements".formatted(name));
                }
                if (!fgaClient.checkSchemaAccess(ctx, name.get(0), name.get(1), "admin")) {
                    denySetEntityAuthorization(entityKindAndName, principal);
                }
                break;
            case "TABLE":
                if (name.size() != 3) {
                    throw new TrinoException(INVALID_ARGUMENTS, "The table name %s must have three elements".formatted(name));
                }
                if (!fgaClient.checkTableAccess(ctx, name.get(0), name.get(1), name.get(2), "admin")) {
                    denySetEntityAuthorization(entityKindAndName, principal);
                }
                break;
            case "VIEW":
                if (name.size() != 3) {
                    throw new TrinoException(INVALID_ARGUMENTS, "The view name %s must have three elements".formatted(name));
                }
                if (!fgaClient.checkTableAccess(ctx, name.get(0), name.get(1), name.get(2), "admin")) {
                    denySetEntityAuthorization(entityKindAndName, principal);
                }
                break;
            default:
                denySetEntityAuthorization(entityKindAndName, principal);
        }
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.check(ctx, "catalog", catalogName, "show_schemas")) {
            denyShowSchemas(catalogName);
        }
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        OpenFgaContext ctx = contextFromSecurity(context);

        // If the client supports batch filtering, use it
        if (fgaClient instanceof BatchEnabledOpenFgaClient) {
            return ((BatchEnabledOpenFgaClient) fgaClient).filterSchemas(ctx, catalogName, schemaNames, "viewer");
        }

        // Otherwise use the standard approach
        ImmutableSet.Builder<String> allowedSchemas = ImmutableSet.builder();
        for (String schemaName : schemaNames) {
            if (fgaClient.checkSchemaAccess(ctx, catalogName, schemaName, "viewer")) {
                allowedSchemas.add(schemaName);
            }
        }
        return allowedSchemas.build();
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schemaName)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkSchemaAccess(ctx, schemaName.getCatalogName(), schemaName.getSchemaName(), "viewer")) {
            denyShowCreateSchema(schemaName.toString());
        }
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkTableAccess(ctx, table.getCatalogName(), table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName(), "viewer")) {
            denyShowCreateTable(table.toString());
        }
    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table,
            Map<String, Object> properties)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkSchemaAccess(ctx, table.getCatalogName(), table.getSchemaTableName().getSchemaName(),
                "admin")) {
            denyCreateTable(table.toString());
        }
    }

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkTableAccess(ctx, table.getCatalogName(), table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName(), "admin")) {
            denyDropTable(table.toString());
        }
    }

    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table,
            CatalogSchemaTableName newTable)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkTableAccess(ctx, table.getCatalogName(), table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName(), "admin")) {
            denyRenameTable(table.toString(), newTable.toString());
        }
    }

    @Override
    public void checkCanSetTableProperties(SystemSecurityContext context, CatalogSchemaTableName table,
            Map<String, Optional<Object>> properties)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkTableAccess(ctx, table.getCatalogName(), table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName(), "admin")) {
            denySetTableProperties(table.toString());
        }
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkTableAccess(ctx, table.getCatalogName(), table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName(), "admin")) {
            denyCommentTable(table.toString());
        }
    }

    @Override
    public void checkCanSetViewComment(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkTableAccess(ctx, view.getCatalogName(), view.getSchemaTableName().getSchemaName(),
                view.getSchemaTableName().getTableName(), "admin")) {
            denyCommentView(view.toString());
        }
    }

    @Override
    public void checkCanSetColumnComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkTableAccess(ctx, table.getCatalogName(), table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName(), "admin")) {
            denyCommentColumn(table.toString());
        }
    }

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkSchemaAccess(ctx, schema.getCatalogName(), schema.getSchemaName(), "show_tables")) {
            denyShowTables(schema.toString());
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName,
            Set<SchemaTableName> tableNames)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        ImmutableSet.Builder<SchemaTableName> allowedTables = ImmutableSet.builder();

        for (SchemaTableName tableName : tableNames) {
            if (fgaClient.checkTableAccess(ctx, catalogName, tableName.getSchemaName(), tableName.getTableName(),
                    "viewer")) {
                allowedTables.add(tableName);
            }
        }

        return allowedTables.build();
    }

    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName tableName)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkTableAccess(ctx, tableName.getCatalogName(), tableName.getSchemaTableName().getSchemaName(),
                tableName.getSchemaTableName().getTableName(), "viewer")) {
            denyShowColumns(tableName.toString());
        }
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkTableAccess(ctx, table.getCatalogName(), table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName(), "select")) {
            denySelectColumns(table.getSchemaTableName().getTableName(), columns);
        }
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkTableAccess(ctx, table.getCatalogName(), table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName(), "insert")) {
            denyInsertTable(table.toString());
        }
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkTableAccess(ctx, table.getCatalogName(), table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName(), "delete")) {
            denyDeleteTable(table.toString());
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege,
            CatalogSchemaTableName table, TrinoPrincipal grantee, boolean grantOption)
    {
        denyGrantTablePrivilege(privilege.toString(), table.toString());
    }

    @Override
    public void checkCanDenyTablePrivilege(SystemSecurityContext context, Privilege privilege,
            CatalogSchemaTableName table, TrinoPrincipal grantee)
    {
        denyDenyTablePrivilege(privilege.toString(), table.toString());
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege,
            CatalogSchemaTableName table, TrinoPrincipal revokee, boolean grantOption)
    {
        denyRevokeTablePrivilege(privilege.toString(), table.toString());
    }

    @Override
    public void checkCanCreateRole(SystemSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
        denyCreateRole(role);
    }

    @Override
    public void checkCanDropRole(SystemSecurityContext context, String role)
    {
        denyDropRole(role);
    }

    @Override
    public void checkCanGrantRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees,
            boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        denyGrantRoles(roles, grantees);
    }

    @Override
    public void checkCanRevokeRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees,
            boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        denyRevokeRoles(roles, grantees);
    }

    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext context, CatalogSchemaRoutineName procedure)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkSchemaAccess(ctx, procedure.getCatalogName(), procedure.getSchemaName(),
                "execute_procedure")) {
            denyExecuteProcedure(procedure.toString());
        }
    }

    @Override
    public void checkCanExecuteTableProcedure(SystemSecurityContext context, CatalogSchemaTableName table,
            String procedure)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkTableAccess(ctx, table.getCatalogName(), table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName(), "execute_procedure")) {
            denyExecuteTableProcedure(table.toString(), procedure);
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext securityContext, String catalogName, String propertyName)
    {
        OpenFgaContext ctx = contextFromSecurity(securityContext);
        if (!fgaClient.check(ctx, "catalog_property", catalogName + "." + propertyName, "set")) {
            throw new AccessDeniedException(String.format("Cannot set catalog session property %s.%s", catalogName, propertyName));
        }
    }

    /**
     * Returns row filters applicable to the specified table for the current user.
     * The OpenFGA client retrieves filter expressions that should be applied to the table.
     * These expressions are converted into ViewExpression objects that Trino can apply during query execution.
     */
    @Override
    public List<ViewExpression> getRowFilters(SystemSecurityContext context, CatalogSchemaTableName tableName)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        String catalogName = tableName.getCatalogName();
        String schemaName = tableName.getSchemaTableName().getSchemaName();
        String tableNameStr = tableName.getSchemaTableName().getTableName();

        // Get applicable row filters for this user and table
        List<String> filters = fgaClient.getApplicableRowFilters(ctx, catalogName, schemaName, tableNameStr);

        // If no filters apply, return empty list
        if (filters.isEmpty()) {
            return List.of();
        }

        // Create ViewExpression for each filter
        List<ViewExpression> viewExpressions = new ArrayList<>();
        for (String filter : filters) {
            viewExpressions.add(ViewExpression.builder()
                    .identity(context.getIdentity().getUser())
                    .catalog(catalogName)
                    .schema(schemaName)
                    .expression(filter)
                    .build());
        }

        return viewExpressions;
    }

    @Override
    public Map<ColumnSchema, ViewExpression> getColumnMasks(SystemSecurityContext context, CatalogSchemaTableName tableName, List<ColumnSchema> columns)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        String catalogName = tableName.getCatalogName();
        String schemaName = tableName.getSchemaTableName().getSchemaName();
        String tableNameStr = tableName.getSchemaTableName().getTableName();

        Map<ColumnSchema, ViewExpression> masks = new HashMap<>();

        for (ColumnSchema column : columns) {
            String columnName = column.getName();

            // Check column access first
            if (!fgaClient.checkColumnAccess(ctx, catalogName, schemaName, tableNameStr, columnName, "viewer")) {
                // If user doesn't have access to the column, mask it completely (return null)
                masks.put(column, ViewExpression.builder()
                        .identity(context.getIdentity().getUser())
                        .catalog(catalogName)
                        .schema(schemaName)
                        .expression("null")
                        .build());
                continue;
            }

            // Get column mask if one applies
            Optional<String> mask = fgaClient.getApplicableColumnMask(ctx, catalogName, schemaName, tableNameStr, columnName);

            // If a specific mask exists, apply it
            if (mask.isPresent()) {
                masks.put(column, ViewExpression.builder()
                        .identity(context.getIdentity().getUser())
                        .catalog(catalogName)
                        .schema(schemaName)
                        .expression(mask.get())
                        .build());
            }
        }

        return masks;
    }

    @Override
    public Map<SchemaTableName, Set<String>> filterColumns(SystemSecurityContext context, String catalogName, Map<SchemaTableName, Set<String>> tableColumns)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        Map<SchemaTableName, Set<String>> filteredColumns = new HashMap<>();

        for (Map.Entry<SchemaTableName, Set<String>> entry : tableColumns.entrySet()) {
            SchemaTableName tableName = entry.getKey();
            String schemaName = tableName.getSchemaName();
            String tableNameStr = tableName.getTableName();
            Set<String> columns = entry.getValue();

            ImmutableSet.Builder<String> allowedColumns = ImmutableSet.builder();
            for (String column : columns) {
                if (fgaClient.checkColumnAccess(ctx, catalogName, schemaName, tableNameStr, column, "viewer")) {
                    allowedColumns.add(column);
                }
            }

            Set<String> allowedColumnsSet = allowedColumns.build();
            if (!allowedColumnsSet.isEmpty()) {
                filteredColumns.put(tableName, allowedColumnsSet);
            }
        }

        return filteredColumns;
    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        OpenFgaContext ctx = contextFromSecurity(context);
        if (!fgaClient.checkSchemaAccess(ctx, view.getCatalogName(), view.getSchemaTableName().getSchemaName(), "admin")) {
            denyCreateView(view.toString());
        }
    }

    @Override
    public void shutdown()
    {
        lifeCycleManager.stop();
    }
}
