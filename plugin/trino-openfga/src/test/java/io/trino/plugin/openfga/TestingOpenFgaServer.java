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

import com.google.common.io.Closer;
import io.airlift.log.Logger;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;

/**
 * Manages a OpenFGA Docker container for integration testing.
 * <p>
 * This class starts a real OpenFGA server in a Docker container using TestContainers.
 * It is designed for integration tests that need to verify behavior with an actual
 * OpenFGA server instance, as opposed to unit tests which should use {@link TestingOpenFgaClient}
 * to avoid external dependencies.
 * <p>
 * The server uses an in-memory backend and exposes both HTTP and gRPC endpoints.
 * <p>
 * Usage example:
 * <pre>
 *   // In a JUnit test
 *   try (TestingOpenFgaServer server = new TestingOpenFgaServer()) {
 *       // Create a config that points to the test server
 *       OpenFgaConfig config = new OpenFgaConfig()
 *           .setApiUrl(server.getApiUrl())
 *           .setStoreId(storeId);  // Store ID needs to be created after server starts
 *
 *       // Create a real client that connects to the test server
 *       OpenFgaClient client = new OpenFgaClient(config);
 *
 *       // Run integration tests with the real client
 *       ...
 *   } // Server is automatically stopped when the try block exits
 * </pre>
 */
public class TestingOpenFgaServer
        implements Closeable
{
    private static final Logger log = Logger.get(TestingOpenFgaServer.class);

    private static final int OPENFGA_HTTP_PORT = 8080;
    private static final int OPENFGA_GRPC_PORT = 8081;
    // Use a specific version instead of latest to ensure test stability
    private static final String OPENFGA_IMAGE = "openfga/openfga:v1.8.9";

    private final GenericContainer<?> dockerContainer;
    private final boolean dockerAvailable;

    /**
     * Creates and starts a new OpenFGA container with memory backend for testing.
     */
    public TestingOpenFgaServer()
    {
        // Check if Docker is available before attempting to start container
        boolean isDockerAvailable;
        try {
            DockerClientFactory.instance().client();
            isDockerAvailable = true;
        }
        catch (Throwable e) {
            log.warn("Docker not available for testing: %s", e.getMessage());
            isDockerAvailable = false;
        }
        this.dockerAvailable = isDockerAvailable;

        // Only create and start the container if Docker is available
        if (isDockerAvailable) {
            try {
                // Initialize container with memory backend (default for OpenFGA)
                dockerContainer = new GenericContainer<>(OPENFGA_IMAGE)
                        .withCommand("run", "--log-level", "debug")
                        .withExposedPorts(OPENFGA_HTTP_PORT, OPENFGA_GRPC_PORT)
                        .withStartupTimeout(Duration.ofMinutes(2))
                        .waitingFor(Wait.forHttp("/healthz").forPort(OPENFGA_HTTP_PORT).forStatusCode(200));

                log.info("Starting OpenFGA container using %s", OPENFGA_IMAGE);
                dockerContainer.start();
                log.info("OpenFGA container started at %s:%d", dockerContainer.getHost(), dockerContainer.getMappedPort(OPENFGA_HTTP_PORT));
            }
            catch (Throwable e) {
                log.warn("Failed to start OpenFGA container: %s", e.getMessage());
                throw new RuntimeException("Failed to start OpenFGA container", e);
            }
        }
        else {
            dockerContainer = null;
        }
    }

    /**
     * Checks if Docker is available and container was successfully started.
     */
    public boolean isDockerAvailable()
    {
        return dockerAvailable && dockerContainer != null && dockerContainer.isRunning();
    }

    /**
     * Gets the HTTP API URL for the OpenFGA server.
     * Returns a fake URL if Docker is not available.
     */
    public String getApiUrl()
    {
        if (!isDockerAvailable()) {
            return "http://localhost:8080"; // Return a dummy URL when Docker is unavailable
        }
        return String.format("http://%s:%d",
                dockerContainer.getHost(),
                dockerContainer.getMappedPort(OPENFGA_HTTP_PORT));
    }

    /**
     * Gets the gRPC API URL for the OpenFGA server.
     * Returns a fake URL if Docker is not available.
     */
    public String getGrpcUrl()
    {
        if (!isDockerAvailable()) {
            return "localhost:8081"; // Return a dummy URL when Docker is unavailable
        }
        return String.format("%s:%d",
                dockerContainer.getHost(),
                dockerContainer.getMappedPort(OPENFGA_GRPC_PORT));
    }

    /**
     * Stops and removes the OpenFGA container.
     */
    @Override
    public void close()
    {
        if (dockerContainer != null) {
            log.info("Stopping OpenFGA container");
            try (Closer closer = Closer.create()) {
                // Explicitly stop the container
                closer.register(() -> {
                    try {
                        dockerContainer.stop();
                        log.info("OpenFGA container stopped successfully");
                    }
                    catch (Exception e) {
                        log.warn("Error stopping OpenFGA container: %s", e.getMessage());
                    }
                });
            }
            catch (IOException e) {
                log.warn("Exception during container cleanup: %s", e.getMessage());
            }
        }
    }
}
