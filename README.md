Beeing new to [Apache Pulsar](https://pulsar.apache.org/) I wanted to test out [geo replication](https://pulsar.apache.org/docs/en/administration-geo/) as this is a prime requirement for the cluster we are planning at work. Although [testing it in the cloud](https://streaml.io/blog/pulsar-distributed-quickstart) could be feasible I prefer having something running locally so I don't need to be too concerned about cost or giving out my credit card.


In the first attempt I followed the [Kubernetes instructions](https://pulsar.apache.org/docs/en/deploy-kubernetes/) on the pulsar web site, but quickly realised that deploying three full blown clusters on my machine, well, it made it unresponsive. So instead I set out to use a [standalone](https://pulsar.apache.org/docs/en/standalone/) condfiguration but quickly ran into trouble.

After some fiddling and with the help of the [Pulsar Users mailing list](https://lists.apache.org/list.html?users@pulsar.apache.org) I managed to get it to work. (Thanks to Sijie Guo and Brian Chandler for the help!)

As the configuration was not evident (at least to me) I thought it could be intersting to share it, please find the result below!


# Prerequisites
This example can be run using either Kubernetes or Docker. 

## Kubernetes Setup
If you want to use Kubernetes, an up-and-running cluster is a prerequisite. If you don't already have one, installing [minikube](https://kubernetes.io/docs/setup/learning-environment/minikube/) is probably the easiest.

## Docker Setup
Alternatively, you can use Docker with Docker Compose to run the demo without Kubernetes. This requires:
- [Docker](https://docs.docker.com/) installed on your machine
- [Docker Compose](https://docs.docker.com/compose/install/) installed on your machine

# Install the Kubernetes dashboard (optional)

The default Kubernetes dashboard can be useful to visualize and manage Kubernetes artifacts. It can be useful as a complement to `kubectl` on the command line.

The Kubernetes web site provides [detailed instructions](https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/) on how to deploy the Kubernetes dashboard, but if you feel confident the following summary should do the trick:

1. Deploy the dashboard specified in the Kubernetes GitHub repository:
    ```
    kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.0.0-beta4/aio/deploy/recommended.yaml
    ```

1. Create [default user and role binding](https://github.com/kubernetes/dashboard/blob/master/docs/user/access-control/creating-sample-user.md) by executing:
    ```
    cat <<EOF | kubectl apply -f -
    apiVersion: v1
    kind: ServiceAccount
    metadata:
      name: admin-user
      namespace: kubernetes-dashboard
    EOF
    ```
    ... followed by:
    ```
    cat <<EOF | kubectl apply -f -
    apiVersion: rbac.authorization.k8s.io/v1
    kind: ClusterRoleBinding
    metadata:
      name: admin-user
    roleRef:
      apiGroup: rbac.authorization.k8s.io
      kind: ClusterRole
      name: cluster-admin
    subjects:
    - kind: ServiceAccount
      name: admin-user
      namespace: kubernetes-dashboard
    EOF
    ```

1. Get the auth token (one line)
    ```
    kubectl -n kubernetes-dashboard describe secret $(kubectl -n kubernetes-dashboard get secret | grep admin-user | awk '{print $1}')
    ```
    Copy the token and save it for the step below

1. Start proxy
    ```
    kubectl proxy
    ```

1. [Access the dashboard](http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/.) and nter the token saved in the step above!

# Deploy Apache Pulsar
To simulate a geographically distributed Pulsar cluster we are using multiple [standalone brokers](https://pulsar.apache.org/docs/en/standalone/) running either in the same Kubernetes namespace or as Docker containers.

Admittedly this is overly simplistic, but given resource constraints on a local workstation a full-blown cluster with separate zookeeper, bookies, proxies etc is probably infeasible.

From a purely functional perspective it should not matter, however. We can still play around with the different clusters as-if they were deployed in different geographic locations.

**In order for the setup to work properly, the default broker name (`standalone`) must be globally unique or the cluster replication will not work properly. This is done by configuring the `clusterName` option.**

## Deploy with Kubernetes
To deploy the clusters using Kubernetes, run `deploy.sh` - or - do the following manually:

1. Define a test namespace `pulsar`

        kubectl apply -f spec/namespace.yaml

1. Create the config resource shared by all standalone clusters

        kubectl apply -f spec/config.yaml

1. Download the [mo](https://github.com/tests-always-included/mo) bash script that replaces [moustache](https://mustache.github.io/) placeholders with envar values

        curl -sSL https://git.io/get-mo -o mo
        chmod +x mo

1. Deploy the **alpha**, **beta** and **gamma** clusters by applying [spec/standalone.yaml] once for each cluster name:

        for cluster in alpha beta gamma
        do
          cat spec/standalone.yaml | name=${cluster} ./mo | kubectl -n pulsar apply -f -
        done

> **Note**: The deployment uses the `apachepulsar/pulsar-all:latest` image. If you encounter issues, you might want to modify the spec/standalone.yaml file to use a specific version tag instead of "latest" to ensure compatibility.

The [spec/standalone.yaml] file defines a Kubernetes Service and Deployment with a `{{name}}` placeholder. If you prefer, you can create three different files and do a `kubectl -n pulsar apply -f {filename}` three times instead.

## Deploy with Docker
To deploy the clusters using Docker, you can either use the automated script or follow the manual steps.

### Automated Deployment
Run the provided `deploy.sh.docker` script to automate the deployment process:

```
chmod +x deploy.sh.docker
./deploy.sh.docker
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

        source alias.sh.docker

4. Configure geo replication:

        ./configure.sh.docker

> **Note**: The Docker Compose setup uses the `apachepulsar/pulsar-all:2.10.0` image with specific port mappings:
> - alpha: 8080:8080 (HTTP) and 6650:6650 (Pulsar)
> - beta: 8081:8080 (HTTP) and 6651:6650 (Pulsar)
> - gamma: 8082:8080 (HTTP) and 6652:6650 (Pulsar)

# Create alias
Apache Pulsar provides `pulsar-admin` for administration and `pulsar-client` for producing and consuming test messages. These utilities are part of the base Pulsar image and can be executed by attaching to the container.

To work efficiently, it is convenient to use aliases for these commands. The following aliases are used throughout this guide:

* `{cluster}-admin` - administration of a cluster
* `{cluster}-client` - produce/consume messages for a cluster

## Kubernetes Aliases
For Kubernetes deployment, execute the following in each shell where you want to work with the clusters:

```
source alias.sh
```

This creates aliases that use `kubectl exec` to run commands in the Pulsar containers, for example:

```
kubectl -n pulsar exec {pod} -it -- bin/pulsar-admin
```

## Docker Aliases
For Docker deployment, execute the following in each shell where you want to work with the clusters:

```
source alias.sh.docker
```

This creates aliases that use `docker exec` to run commands in the Pulsar containers, for example:

```
docker exec -it {container} bin/pulsar-admin
```

Note that in the official Pulsar documentation for [deploying Pulsar on Kubernetes](https://pulsar.apache.org/docs/en/deploy-kubernetes/), a `pulsar-admin` alias is configured to execute the `bin/pulsar-admin` binary in a container separate from the brokers. As we have a highly simplified setup with only one standalone broker per cluster, we don't need this and can instead attach directly to the running broker containers. This reduces resource consumption on the local workstation or laptop.

**The next steps assume that you are running a shell that has been configured with the appropriate aliases!**

# Configure geo replication
To configure [geo replication](https://pulsar.apache.org/docs/en/administration-geo/) we need to:

* Tell each standalone cluster (**alpha**, **beta** and **gamma**) that the other clusters exist
* Configure a tenant (`acme`) and namespace (`acme/test`) that uses all three clusters for replication

## Configure with Kubernetes
For Kubernetes deployment, you can either run the provided `configure.sh` script to automate these steps:

```
./configure.sh
```

## Configure with Docker
For Docker deployment, you can run the provided `configure.sh.docker` script to automate these steps:

```
./configure.sh.docker
```

## Manual Configuration Steps
Alternatively, you can follow these manual steps (the commands are the same for both Kubernetes and Docker, assuming you've sourced the appropriate alias file):

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
To test geo replication we create three different consumers, one for each standalone cluster and then produce messages. On successful execution all three consumers should see all messages.

Note that subscriptions are *exclusive* per default but that only applies to consumers within the same cluster.

**Important**: Always start the consumers before producing messages to ensure no messages are missed. Use the `-p Earliest` flag to make sure consumers receive all messages, including those that were sent before the consumer started.

## Test with Kubernetes
For Kubernetes deployment:

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

## Test with Docker
For Docker deployment:

1. Open three shells and make sure that each shell is initialized with `source alias.sh.docker` or else the aliases will not work.
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

### Kubernetes Cleanup
To clean up the Kubernetes resources:

```
kubectl delete namespace pulsar
```

### Docker Cleanup
To clean up the Docker resources:

```
docker-compose down -v
```

This will stop and remove the containers, networks, and volumes created by Docker Compose.
