Below is a **clear, production-ready design** for setting up a **self-hosted Azure DevOps agent** that **installs Azure CLI on demand** and **reports its availability using Agent Capabilities**.

This matches how Azure DevOps actually schedules jobs and avoids unsupported “runtime capability changes”.

---

## Architecture overview

**What we want**

* Self-hosted agent checks whether **Azure CLI** exists
* If missing → attempts installation
* If install succeeds → capability `AZ=true`
* If install fails → capability `AZ=false`
* Pipelines run **only** on healthy agents

**Key constraint**

> Agent capabilities are evaluated **before** job assignment
> So the capability must be set **before** pipelines run.

---

## Step 1: Install and configure the self-hosted agent

On the VM (Linux example):

```bash
mkdir azagent && cd azagent
wget https://vstsagentpackage.azureedge.net/agent/3.238.0/vsts-agent-linux-x64-3.238.0.tar.gz
tar zxvf vsts-agent-linux-x64-3.238.0.tar.gz
./config.sh
```

Run the agent **as a service**:

```bash
sudo ./svc.sh install
sudo ./svc.sh start
```

---

## Step 2: Bootstrap script to install Azure CLI & set capability

Create a script that runs **before the agent starts**.

### `bootstrap-az-cli.sh`

```bash
#!/bin/bash
set -e

ENV_FILE="$AGENT_HOME/.env"
rm -f "$ENV_FILE"

echo "🔍 Checking Azure CLI availability..."

if command -v az >/dev/null 2>&1; then
  echo "AZ=true" >> "$ENV_FILE"
  exit 0
fi

echo "⬇️ Azure CLI not found. Installing..."

if curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash; then
  if command -v az >/dev/null 2>&1; then
    echo "AZ=true" >> "$ENV_FILE"
  else
    echo "AZ=false" >> "$ENV_FILE"
  fi
else
  echo "AZ=false" >> "$ENV_FILE"
fi
```

Make it executable:

```bash
chmod +x bootstrap-az-cli.sh
```

---

## Step 3: Wire bootstrap into agent startup (systemd)

Edit the agent service file:

```bash
sudo nano /etc/systemd/system/vsts.agent.*.service
```

Add **before ExecStart**:

```ini
ExecStartPre=/bin/bash /home/azureuser/azagent/bootstrap-az-cli.sh
```

Reload and restart:

```bash
sudo systemctl daemon-reexec
sudo systemctl restart vsts.agent.*
```

---

## Step 4: How Azure DevOps picks up the capability

Azure DevOps automatically converts **environment variables** into **agent capabilities**.

After restart:

```
AZ = true   (or false)
```

Verify in:

```
Project Settings → Agent Pools → Agents → Capabilities
```

---

## Step 5: Use the capability in pipelines

### Azure DevOps YAML

```yaml
pool:
  name: SelfHostedPool
  demands:
    - AZ -equals true
```

**Result**

* ✅ Pipelines run only on agents with working Azure CLI
* ❌ Broken or misconfigured agents are skipped
* 🚫 No runtime failures due to missing CLI

---

## Step 6: Failure & recovery behavior

| Scenario                               | Result               |
| -------------------------------------- | -------------------- |
| Azure CLI already installed            | AZ=true              |
| Install succeeds                       | AZ=true              |
| Install fails (proxy, key, repo issue) | AZ=false             |
| Agent reboot                           | Bootstrap runs again |
| Fix repo / proxy                       | Agent auto-heals     |

---

## Why this approach is correct

✔ Fully supported by Azure DevOps
✔ No race conditions during job assignment
✔ Works with large agent pools
✔ Zero pipeline YAML hacks
✔ Clear agent health signal

---

## Optional enhancements

If you want to go further:

* Windows PowerShell version
* Proxy-aware Azure CLI install
* Central agent health dashboard
* Auto-quarantine unhealthy agents
* Multiple tool capabilities (`AZ`, `TF`, `DBX`, etc.)
