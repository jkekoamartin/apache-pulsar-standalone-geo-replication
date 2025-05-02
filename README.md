Beeing new to [Apache Pulsar](https://pulsar.apache.org/) I wanted to test out [geo replication](https://pulsar.apache.org/docs/en/administration-geo/) as this is a prime requirement for the cluster we are planning at work. Although [testing it in the cloud](https://streaml.io/blog/pulsar-distributed-quickstart) could be feasible I prefer having something running locally so I don't need to be too concerned about cost or giving out my credit card.


After some fiddling and with the help of the [Pulsar Users mailing list](https://lists.apache.org/list.html?users@pulsar.apache.org) I managed to get it to work. (Thanks to Sijie Guo and Brian Chandler for the help!)

As the configuration was not evident (at least to me) I thought it could be intersting to share it, please find the result below!


# Prerequisites
This example requires Docker with Docker Compose to run the demo:
- [Docker](https://docs.docker.com/) installed on your machine
- [Docker Compose](https://docs.docker.com/compose/install/) installed on your machine


# Deploy Apache Pulsar
To simulate a geographically distributed Pulsar cluster we are using multiple [standalone brokers](https://pulsar.apache.org/docs/en/standalone/) running as Docker containers.

Admittedly this is overly simplistic, but given resource constraints on a local workstation a full-blown cluster with separate zookeeper, bookies, proxies etc is probably infeasible.

From a purely functional perspective it should not matter, however. We can still play around with the different clusters as-if they were deployed in different geographic locations.

**In order for the setup to work properly, the default broker name (`standalone`) must be globally unique or the cluster replication will not work properly. This is done by configuring the `clusterName` option.**

## Deployment
To deploy the clusters using Docker, you can either use the automated script or follow the manual steps.

### Automated Deployment
Run the provided `deploy.sh` script to automate the deployment process:

```
chmod +x deploy.sh
./deploy.sh
```

This script will:
1. Start the Docker containers
2. Wait for them to be ready
3. Set up the aliases
4. Configure geo replication
5. Provide instructions for testing

### Manual Deployment
Alternatively, you can follow these manual steps:

1. Start the containers using Docker Compose:

        docker-compose up -d

   This will start three Pulsar standalone brokers named **alpha**, **beta**, and **gamma**.

2. Verify that all containers are running:

        docker-compose ps

   All three containers should be in the "Up" state.

3. Set up the aliases:

        source alias.sh

4. Configure geo replication:

        ./configure.sh

> **Note**: The Docker Compose setup uses the `apachepulsar/pulsar-all:2.10.0` image with specific port mappings:
> - alpha: 8080:8080 (HTTP) and 6650:6650 (Pulsar)
> - beta: 8081:8080 (HTTP) and 6651:6650 (Pulsar)
> - gamma: 8082:8080 (HTTP) and 6652:6650 (Pulsar)

# Create alias
Apache Pulsar provides `pulsar-admin` for administration and `pulsar-client` for producing and consuming test messages. These utilities are part of the base Pulsar image and can be executed by attaching to the container.

To work efficiently, it is convenient to use aliases for these commands. The following aliases are used throughout this guide:

* `{cluster}-admin` - administration of a cluster
* `{cluster}-client` - produce/consume messages for a cluster

Execute the following in each shell where you want to work with the clusters:

```
source alias.sh
```

This creates aliases that use `docker exec` to run commands in the Pulsar containers, for example:

```
docker exec -it {container} bin/pulsar-admin
```

**The next steps assume that you are running a shell that has been configured with the appropriate aliases!**

# Configure geo replication
To configure [geo replication](https://pulsar.apache.org/docs/en/administration-geo/) we need to:

* Tell each standalone cluster (**alpha**, **beta** and **gamma**) that the other clusters exist
* Configure a tenant (`acme`) and namespace (`acme/test`) that uses all three clusters for replication

You can run the provided `configure.sh` script to automate these steps:

```
./configure.sh
```

## Manual Configuration Steps
Alternatively, you can follow these manual steps (assuming you've sourced the alias file):

1. Configure the **alpha** cluster

    Create the beta cluster in alpha:
    ```
    alpha-admin clusters create --url http://beta:8080 --broker-url pulsar://beta:6650 beta
    ```
    Create the gamma cluster in alpha:
    ```
    alpha-admin clusters create --url http://gamma:8080 --broker-url pulsar://gamma:6650 gamma
    ```
    Create the **acme** tenant in alpha and allow it to use clusters **alpha**, **beta** and **gamma**:
    ```
    alpha-admin tenants create --allowed-clusters alpha,beta,gamma acme
    ```
    Create `acme/test` namespace in alpha:
    ```
    alpha-admin namespaces create --clusters alpha,beta,gamma acme/test
    ```

1. Configure the **beta** cluster
    ```
    beta-admin clusters create --url http://alpha:8080 --broker-url pulsar://alpha:6650 alpha

    beta-admin clusters create --url http://gamma:8080 --broker-url pulsar://gamma:6650 gamma

    beta-admin tenants create --allowed-clusters alpha,beta,gamma acme

    beta-admin namespaces create --clusters alpha,beta,gamma acme/test
    ```

1. Configure the **gamma** cluster

    ```
    gamma-admin clusters create --url http://alpha:8080 --broker-url pulsar://alpha:6650 alpha

    gamma-admin clusters create --url http://beta:8080 --broker-url pulsar://beta:6650 beta

    gamma-admin tenants create --allowed-clusters alpha,beta,gamma acme

    gamma-admin namespaces create --clusters alpha,beta,gamma acme/test
    ```

# Test geo replication

## Basic CLI Test
To test geo replication we create three different consumers, one for each standalone cluster and then produce messages. On successful execution all three consumers should see all messages.

Note that subscriptions are *exclusive* per default but that only applies to consumers within the same cluster.

**Important**: Always start the consumers before producing messages to ensure no messages are missed. Use the `-p Earliest` flag to make sure consumers receive all messages, including those that were sent before the consumer started.

## Python Demo Application
A Python demo application is included that demonstrates:

1. Producer failover from alpha to beta cluster
2. Shared subscription usage
3. Duplicate and out-of-order message handling
4. Live visualization of message flow

To run the Python demo:

```
./run_demo.sh
```

### Chaos Testing Demo

For a more realistic demonstration of cluster failures, you can use the chaos testing script that randomly shuts down and restarts clusters:

```
./run_chaos_demo.sh
```

This script:
- Starts the demo application with real-time cluster health monitoring
- Runs a chaos script in the background that randomly stops and starts the Pulsar clusters
- Shows health indicators for all three clusters in the visualization
- Demonstrates how the system handles real cluster failures and recoveries

See [PYTHON_README.md](PYTHON_README.md) for more details.

1. Open three shells and make sure that each shell is initialized with `source alias.sh` or else the aliases will not work.
2. In each shell start a consumer on the `acme/test/hello` topic with the `-p Earliest` flag to receive messages from the beginning:
    ```
    alpha-client consume -n 10 -s hello -p Earliest acme/test/hello

    beta-client consume -n 10 -s hello -p Earliest acme/test/hello

    gamma-client consume -n 10 -s hello -p Earliest acme/test/hello
    ```
3. In a fourth shell produce 10 messages on the `acme/test/hello` topic
    ```
    alpha-client produce -n 10 -m hello acme/test/hello
    ```
4. Verify that each cluster has consumed its ten messages and then exited!

## Cleanup
To clean up the Docker resources:

```
docker-compose down -v
```

This will stop and remove the containers, networks, and volumes created by Docker Compose.
