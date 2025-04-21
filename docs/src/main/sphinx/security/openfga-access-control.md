# OpenFGA access control

The OpenFGA access control plugin enables the use of [OpenFGA](https://openfga.dev/)
(Fine-Grained Authorization) as the authorization engine for implementing
fine-grained access control to catalogs, schemas, tables, and more in Trino.
Policies are defined in OpenFGA's relationship-based authorization model, and
Trino checks access control privileges against OpenFGA.

## Requirements

* A running [OpenFGA server](https://openfga.dev/docs/getting-started)
* Network connectivity from the Trino cluster to the OpenFGA server
* An OpenFGA store with an appropriate authorization model

With the requirements fulfilled, you can proceed to set up Trino and OpenFGA with
your desired access control configuration.

## Trino configuration

To use OpenFGA for access control, create the file `etc/access-control.properties`
with the following minimal configuration:

```properties
access-control.name=openfga
openfga.api.url=http://openfga-server:8080
openfga.store.id=YOUR_STORE_ID
openfga.model.id=YOUR_MODEL_ID
```

The following table lists the configuration properties for the OpenFGA access control:

:::{list-table} OpenFGA access control configuration properties
:widths: 40, 60
:header-rows: 1

* - Name
  - Description
* - `openfga.api.url`
  - The **required** URL for the OpenFGA API server, for example,
    `http://openfga-server:8080`.
* - `openfga.store.id`
  - The **required** OpenFGA store ID where authorization data is stored.
* - `openfga.model.id`
  - The **required** OpenFGA authorization model ID to use for access decisions.
* - `openfga.api.token`
  - Optional API token for OpenFGA if authentication is required.
* - `openfga.auth.authority`
  - Optional OAuth authority for OpenFGA API authentication.
* - `openfga.auth.client-id`
  - Optional OAuth client ID for OpenFGA API authentication.
* - `openfga.auth.client-secret`
  - Optional OAuth client secret for OpenFGA API authentication.
* - `openfga.log-requests`
  - Configure if request details are logged prior to sending them to OpenFGA.
    Defaults to `false`.
* - `openfga.log-responses`
  - Configure if OpenFGA response details are logged. Defaults to `false`.
* - `openfga.allow-permission-management-operations`
  - Configure if permission management operations are allowed. Find more details in
    [](openfga-permission-management). Defaults to `false`.
* - `openfga.filter-expressions`
  - Map of filter IDs to SQL expressions for row-level filtering.
* - `openfga.mask-expressions`
  - Map of mask IDs to SQL expressions for column-level masking.
* - `openfga.model.custom-path`
  - Path to a custom OpenFGA authorization model definition file (YAML format).
* - `openfga.model.schema-version`
  - Schema version for OpenFGA authorization models (default: 1.1).
* - `openfga.model.auto-create`
  - Whether to automatically create an authorization model if none exists or the
    configured model doesn't exist. Defaults to `false`.
:::

### Logging

When request or response logging is enabled, details are logged at the `DEBUG`
level under the `io.trino.plugin.openfga.OpenFgaClient` logger. The Trino logging
configuration must be updated to include this class to ensure log entries are
created.

Note that enabling these options produces very large amounts of log data.

(openfga-permission-management)=
### Permission management

The following operations are allowed or denied based on the setting of
`openfga.allow-permission-management-operations`. If set to `true`, these operations are
allowed. If set to `false`, they are denied. In both cases, no request is sent
to OpenFGA.

- `GrantSchemaPrivilege`
- `DenySchemaPrivilege`
- `RevokeSchemaPrivilege`
- `GrantTablePrivilege`
- `DenyTablePrivilege`
- `RevokeTablePrivilege`
- `CreateRole`
- `DropRole`
- `GrantRoles`
- `RevokeRoles`

The setting defaults to `false` due to the complexity and potential unexpected
consequences of having SQL-style grants and roles together with OpenFGA.

You must enable permission management if another custom security system in Trino
is capable of grant management and used together with OpenFGA access control.

Additionally, users are always allowed to show information about roles (`SHOW
ROLES`), regardless of this setting. The following operations are _always_
allowed:

- `ShowRoles`
- `ShowCurrentRoles`
- `ShowRoleGrants`

## OpenFGA configuration

OpenFGA is a relationship-based authorization system built on the concept of a
relationship tuple containing `<user, relation, object>`.

### Authorization model

OpenFGA requires an authorization model that defines the types of objects and the
relationships between them. The Trino OpenFGA plugin includes a default model
suitable for controlling access to Trino resources, but you can also provide a
custom model if needed.

The default model includes the following types:

- `user` - Represents individual users in the system
- `role` - Enables role-based access control with nested roles
- `system` - Controls global Trino system permissions
- `catalog` - Represents Trino catalogs
- `schema` - Represents database schemas within catalogs
- `table` - Represents database tables within schemas
- `column` - Represents table columns
- `row_filter` - Type for implementing row-level security
- `mask_filter` - Type for implementing column masking

These types establish a hierarchy of permissions (catalog -> schema -> table -> column)
where permissions can be granted at any level and flow down to lower levels.

### Relationship tuples

Access permissions in OpenFGA are granted by creating relationship tuples. Here are
some examples of relationship tuples for Trino access control:

```
# Grant user 'alice' system-level execute permission
user:alice member role:analysts
role:analysts execute_query system:trino

# Grant user 'bob' admin access to a catalog
user:bob admin catalog:hive

# Grant role 'finance' view access to a specific schema
role:finance viewer schema:hive/finance_data

# Grant user 'charlie' view access to a specific table
user:charlie viewer table:hive/finance_data/balance_sheet
```

### Row filtering

Row filtering allows Trino to remove some rows from the result before returning
it to the caller, controlling what data different users can see. The OpenFGA plugin
supports row filtering through the `row_filter` type in the authorization model.

Row filters are defined by creating relationship tuples in OpenFGA and mapping
filter expressions in the plugin configuration:

```properties
# In access-control.properties
openfga.filter-expressions={\
  "sensitive_data": "department_id = current_user",\
  "limited_view": "created_date > CURRENT_DATE - INTERVAL '30' DAY"\
}
```

Then in OpenFGA, you would create relationship tuples to apply these filters:

```
# Apply the 'sensitive_data' filter to finance table for analyst role
row_filter:sensitive_data applies_to role:analysts
row_filter:sensitive_data target_table table:hive/finance_data/employees
```

### Column masking

Column masking allows Trino to obscure data in one or more columns of the result
set for specific users, without outright denying access. The OpenFGA plugin
supports column masking through the `mask_filter` type in the authorization model.

Column masks are defined in the plugin configuration with expressions and applied
through relationship tuples in OpenFGA:

```properties
# In access-control.properties
openfga.mask-expressions={\
  "hash_pii": "sha256(to_utf8(ssn))",\
  "partial_view": "substr(phone_number, 1, length(phone_number)-4) || '****'"\
}
```

Then in OpenFGA:

```
# Apply the 'hash_pii' mask to the SSN column for analyst role
mask_filter:hash_pii applies_to role:analysts
mask_filter:hash_pii target_column column:hive/finance_data/employees/ssn
```

## Authorization checks

The OpenFGA access control plugin in Trino performs authorization checks by
checking relationship tuples in the OpenFGA server. For example, to determine
if a user can access a table, Trino checks if the user has the `viewer` or
`admin` relationship with the table or any of its parent objects (schema
or catalog).

For each query, multiple authorization checks may be performed depending on
the resources accessed by the query. These checks ensure that users only
access resources they have permission to use.

## Best practices

1. **Role-based access control**: Create roles for different access patterns and
   assign users to roles instead of granting permissions directly to users.

2. **Hierarchical permissions**: Take advantage of the hierarchical structure of Trino
   objects by granting permissions at higher levels when appropriate.

3. **Least privilege**: Grant users and roles the minimum permissions needed to
   perform their tasks.

4. **Regular audits**: Periodically review the authorization tuples in OpenFGA
   to ensure they reflect your current access control requirements.

5. **Test thoroughly**: Test access control policies in a non-production
   environment before deploying to production.

## Example configuration

Here's a complete example of setting up the OpenFGA plugin for Trino:

1. Install and run OpenFGA server:
   ```
   # Follow OpenFGA's installation instructions
   ```

2. Create an OpenFGA store:
   ```
   curl -X POST http://localhost:8080/stores \
     -H 'Content-Type: application/json' \
     -d '{"name":"trino-access-control"}'
   ```

3. Configure Trino's `access-control.properties`:
   ```properties
   access-control.name=openfga
   openfga.api.url=http://openfga-server:8080
   openfga.store.id=YOUR_STORE_ID
   openfga.model.id=YOUR_MODEL_ID
   openfga.filter-expressions={"sensitive":"department_id = current_department"}
   openfga.mask-expressions={"hash_pii":"sha256(to_utf8(ssn))"}
   ```

4. Create relationship tuples in OpenFGA for access control:
   ```
   # System-level permissions
   user:admin execute_query system:trino
   role:users execute_query system:trino
   
   # Catalog permissions
   user:admin admin catalog:hive
   role:data_analysts viewer catalog:hive
   
   # Schema permissions
   role:finance viewer schema:hive/finance
   
   # Table permissions
   role:hr viewer table:hive/hr/employees
   
   # Assign users to roles
   user:alice member role:data_analysts
   user:bob member role:finance
   user:charlie member role:hr
   
   # Row filters
   row_filter:sensitive applies_to role:data_analysts
   row_filter:sensitive target_table table:hive/hr/employees
   
   # Column masks
   mask_filter:hash_pii applies_to role:data_analysts
   mask_filter:hash_pii target_column column:hive/hr/employees/ssn
   ```

With this configuration, each user will have appropriate access to Trino resources
as defined by the relationship tuples in OpenFGA.
