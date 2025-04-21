# trino-openfga

This plugin enables Trino to use [OpenFGA (Fine Grained Authorization)](https://openfga.dev/) as an authorization engine.

## Overview

This plugin integrates OpenFGA with Trino's access control system, allowing administrators to use OpenFGA's relationship-based authorization model for controlling access to Trino resources.

## Trino and OpenFGA Integration

Trino's authorization model includes support for:

- Catalog, schema, table, and column-level access control
- Row filtering and column masking for fine-grained data access control
- Permission management operations (GRANT, REVOKE, etc.)

The OpenFGA plugin maps these Trino concepts to OpenFGA's object types, relations, and relationship tuples. This mapping enables the implementation of complex authorization policies using OpenFGA's relationship-based model.

## Configuration

The plugin supports the following configuration properties in the `etc/access-control/openfga.properties` file:

```properties
# Required: URI for the OpenFGA API server
openfga.api.url=http://localhost:8080

# Required: OpenFGA store ID
openfga.store.id=01EXAMPLESTORID

# Required: OpenFGA authorization model ID
openfga.model.id=01EXAMPLEMODELID

# Optional: OpenFGA API key (if authentication is required)
openfga.api.token=your-api-key

# Optional: Whether to log requests (default: false)
openfga.log-requests=false

# Optional: Whether to log responses (default: false)
openfga.log-responses=false

# Optional: Whether to allow permission management operations (default: false)
openfga.allow-permission-management-operations=false
```

## Usage Example

After configuring the plugin and setting up your OpenFGA authorization model, you can add relationship tuples to grant access. For example:

```bash
# Grant user 'alice' admin access to catalog 'hive'
curl -X POST http://localhost:8080/stores/01EXAMPLESTORID/write \
  -H "Content-Type: application/json" \
  -d '{
    "writes": {
      "tuple_keys": [
        {
          "user": "user:alice",
          "relation": "admin",
          "object": "catalog:hive"
        }
      ]
    }
  }'
```

Consult the OpenFGA documentation for more detailed information on setting up and managing relationships.
