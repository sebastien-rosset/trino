#!/bin/bash
# This script helps set up basic OpenFGA tuples for Trino authorization

# Configuration variables
OPENFGA_URL=${OPENFGA_URL:-"http://localhost:8080"}
OPENFGA_STORE_ID=${OPENFGA_STORE_ID:-"your-store-id"}
TRINO_USER=${TRINO_USER:-"your-trino-user"}
CATALOG_NAME=${CATALOG_NAME:-"hive"}
SCHEMA_NAME=${SCHEMA_NAME:-"default"}

# Function to create basic tuples
create_basic_access() {
  echo "Creating basic tuples for user $TRINO_USER to access Trino..."
  
  # System-level permissions
  curl -X POST "$OPENFGA_URL/stores/$OPENFGA_STORE_ID/write" \
    -H "Content-Type: application/json" \
    -d '{
      "writes": {
        "tuple_keys": [
          {
            "user": "user:'$TRINO_USER'",
            "relation": "execute_query",
            "object": "system:system"
          },
          {
            "user": "user:'$TRINO_USER'",
            "relation": "read",
            "object": "system:system"
          }
        ]
      }
    }'
  
  # Catalog access
  curl -X POST "$OPENFGA_URL/stores/$OPENFGA_STORE_ID/write" \
    -H "Content-Type: application/json" \
    -d '{
      "writes": {
        "tuple_keys": [
          {
            "user": "user:'$TRINO_USER'",
            "relation": "viewer",
            "object": "catalog:'$CATALOG_NAME'"
          }
        ]
      }
    }'
  
  # Schema access
  curl -X POST "$OPENFGA_URL/stores/$OPENFGA_STORE_ID/write" \
    -H "Content-Type: application/json" \
    -d '{
      "writes": {
        "tuple_keys": [
          {
            "user": "user:'$TRINO_USER'",
            "relation": "viewer",
            "object": "schema:'$CATALOG_NAME'/'$SCHEMA_NAME'"
          },
          {
            "user": "user:'$TRINO_USER'",
            "relation": "show_tables",
            "object": "schema:'$CATALOG_NAME'/'$SCHEMA_NAME'"
          }
        ]
      }
    }'
  
  echo "Basic tuples created successfully!"
}

create_admin_access() {
  echo "Creating admin tuples for user $TRINO_USER to administer Trino..."
  
  # System-level admin
  curl -X POST "$OPENFGA_URL/stores/$OPENFGA_STORE_ID/write" \
    -H "Content-Type: application/json" \
    -d '{
      "writes": {
        "tuple_keys": [
          {
            "user": "user:'$TRINO_USER'",
            "relation": "admin",
            "object": "system:system"
          }
        ]
      }
    }'
  
  # Catalog admin
  curl -X POST "$OPENFGA_URL/stores/$OPENFGA_STORE_ID/write" \
    -H "Content-Type: application/json" \
    -d '{
      "writes": {
        "tuple_keys": [
          {
            "user": "user:'$TRINO_USER'",
            "relation": "admin",
            "object": "catalog:'$CATALOG_NAME'"
          }
        ]
      }
    }'
  
  echo "Admin tuples created successfully!"
}

check_tuples() {
  echo "Checking tuples for user $TRINO_USER..."
  
  # Check system-level access
  curl -X POST "$OPENFGA_URL/stores/$OPENFGA_STORE_ID/check" \
    -H "Content-Type: application/json" \
    -d '{
      "tuple_key": {
        "user": "user:'$TRINO_USER'",
        "relation": "execute_query",
        "object": "system:system"
      }
    }'
  
  # Check catalog access
  curl -X POST "$OPENFGA_URL/stores/$OPENFGA_STORE_ID/check" \
    -H "Content-Type: application/json" \
    -d '{
      "tuple_key": {
        "user": "user:'$TRINO_USER'",
        "relation": "viewer",
        "object": "catalog:'$CATALOG_NAME'"
      }
    }'
}

# Main script
echo "OpenFGA Trino Authorization Setup"
echo "================================="
echo "OpenFGA URL: $OPENFGA_URL"
echo "Store ID: $OPENFGA_STORE_ID"
echo "Trino User: $TRINO_USER"
echo "Catalog: $CATALOG_NAME"
echo "Schema: $SCHEMA_NAME"
echo

# Menu
echo "Select an option:"
echo "1. Create basic access for user"
echo "2. Create admin access for user"
echo "3. Check existing tuples"
echo "4. Exit"
read -p "Option: " option

case $option in
  1) create_basic_access ;;
  2) create_admin_access ;;
  3) check_tuples ;;
  4) exit 0 ;;
  *) echo "Invalid option" ;;
esac
