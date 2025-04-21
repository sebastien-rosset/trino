# Use the official Trino image as base
FROM trinodb/trino:425

# Set working directory
WORKDIR /usr/lib/trino/plugin

# Copy the locally built trino-openfga plugin
# This assumes the plugin JAR exists at this relative path
COPY ../target/trino-openfga-*/ ./openfga/

# The container assumes that the following directory structure exists:
# /usr/lib/trino/plugin/openfga/
#   ├── <various JAR files for the plugin and dependencies>