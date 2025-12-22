To set up your self-hosted agent using Docker, we can create an image that automatically handles the Azure CLI installation and sets the capability. This is often the most reliable "on-demand" method because every time a container starts, it ensures the environment is exactly how you want it.

### Dockerfile for Azure DevOps Agent with Azure CLI

This Dockerfile installs the agent prerequisites and the Azure CLI. If the CLI installation fails during the build, the build process will stop, ensuring you never deploy a "broken" agent.

```dockerfile
FROM ubuntu:22.04

# Prevent interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# 1. Install Dependencies and Azure CLI
RUN apt-get update && apt-get install -y \
    curl \
    git \
    jq \
    libicu70 \
    unzip \
    && curl -sL https://aka.ms/InstallAzureCLIDeb | bash \
    && rm -rf /var/lib/apt/lists/*

# 2. Verify installation and set internal flag
RUN az --version && echo "Az=yes" > /agent_capabilities.txt

WORKDIR /azp
COPY ./start.sh .
RUN chmod +x start.sh

CMD ["./start.sh"]

```

---

### The Startup Script (`start.sh`)

This script registers the agent with your Azure DevOps pool. To pass the "Az" capability to Azure DevOps, we use the `--env` flag during configuration.

```bash
#!/bin/bash
set -e

# Validate required environment variables
if [ -z "$AZP_URL" ]; then echo "Error: AZP_URL is missing"; exit 1; fi
if [ -z "$AZP_TOKEN" ]; then echo "Error: AZP_TOKEN is missing"; exit 1; fi

# Download and extract the agent
AZP_AGENT_PACK=$(curl -sL "https://vstsagentpackage.azureedge.net/agent/3.225.0/vsts-agent-linux-x64-3.225.0.tar.gz")
curl -Ls "$AZP_AGENT_PACK" | tar -xz

# Configure the agent and add the Az capability
# The --env flag allows you to pass "Az=yes" directly to the server
./config.sh --unattended \
  --url "$AZP_URL" \
  --auth pat \
  --token "$AZP_TOKEN" \
  --pool "${AZP_POOL:-Default}" \
  --agent "${AZP_AGENT_NAME:-docker-agent}" \
  --replace \
  --acceptTeeEula \
  --env "Az=yes" 

./run.sh

```

---

### How to Run the Agent

Once you have your Docker image built, you can run it using the command below. If the Azure CLI is successfully baked into the image, the `--env "Az=yes"` flag ensures the DevOps portal sees it immediately.

```bash
docker build -t azdevops-agent-cli .

docker run -e AZP_URL="https://dev.azure.com/YourOrg" \
           -e AZP_TOKEN="YourPersonalAccessToken" \
           -e AZP_POOL="MySelfHostedPool" \
           -e AZP_AGENT_NAME="CLI-Agent-01" \
           azdevops-agent-cli

```

### Verification in Azure DevOps

1. Go to **Project Settings** > **Agent Pools**.
2. Select your pool and click on **Agents**.
3. Click on your specific agent and select the **Capabilities** tab.
4. You will see **Az** listed with the value **yes** under the User-defined capabilities section.

---

**Would you like me to show you how to set up a Cron job or a Pipeline to automatically rebuild this image whenever a new version of the Azure CLI is released?**
