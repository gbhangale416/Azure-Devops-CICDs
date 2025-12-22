Setting up a self-hosted agent that can dynamically install the Azure CLI and report its status via **Capabilities** is a great way to ensure your pipeline environment stays up to date.

To achieve this, we will use a **PowerShell startup script** that runs when the agent boots. This script will check for the CLI, install it if missing, and then use the agent's environment variables to update its capability list.

---

## 1. The Startup & Installation Script

Save the following script as `config-agent.ps1` on your host machine. This script handles the "on-demand" installation and logic for the capability assignment.

```powershell
# 1. Check if Azure CLI is installed
$azPath = Get-Command az -ErrorAction SilentlyContinue

if ($azPath) {
    Write-Host "Azure CLI is already installed."
    $env:Az_Capability = "yes"
} else {
    Write-Host "Azure CLI not found. Installing..."
    try {
        # 2. Install Azure CLI via MSI (Silent)
        $msiUrl = "https://aka.ms/installazurecliwindows"
        Start-Wait -Wait -FilePath "msiexec.exe" -ArgumentList "/i $msiUrl /quiet /wait"
        
        # Refresh Path for the current session
        $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
        
        if (Get-Command az -ErrorAction SilentlyContinue) {
            $env:Az_Capability = "yes"
        } else {
            $env:Az_Capability = "no"
        }
    } catch {
        Write-Error "Installation failed."
        $env:Az_Capability = "no"
    }
}

# 3. Set the User-Defined Capability for the Agent
# This tells Azure DevOps to add "Az" to the capabilities list
Write-Host "##vso[agent.setvariable variable=Az]$env:Az_Capability"

```

---

## 2. Setting Up the Agent

Follow these steps to ensure the agent recognizes the installation:

1. **Download & Extract:** Download the agent package from your Azure DevOps portal (**Project Settings > Agent Pools > New Agent**).
2. **Configure:** Run `.\config.cmd`. When prompted for the server URL and PAT, enter your details.
3. **Run with Environment Variables:** Before starting the agent, run the script above.
4. **Manual Capability (Optional):** If you want the capability to be permanent and visible in the UI without running a script every time, navigate to:
* **Project Settings** > **Agent Pools** > **[Your Pool]** > **Agents**.
* Select your agent and click the **Capabilities** tab.
* Add a new capability: `Az` with the value `yes`.



---

## 3. Using the Capability in your Pipeline

Now that your agent reports `Az = yes`, you can ensure your YAML pipelines only run on agents where the CLI installation succeeded by using **demands**.

```yaml
pool:
  name: 'Your-Self-Hosted-Pool'
  demands:
    - Az -equals yes

steps:
- script: az --version
  displayName: 'Verify Azure CLI'

```

---

### Important Considerations

* **Permissions:** The user account running the agent service must have **Administrative privileges** to install software like the Azure CLI.
* **Restart:** After a fresh installation of the CLI, the agent process usually needs to be restarted to "see" the new System Path variables.
* **Linux/Docker:** If you are running this on **Linux**, you would replace the PowerShell script with a `bash` script using `apt-get install azure-cli`.

**Would you like me to help you convert this into a Dockerfile if you plan on running your agents as containers?**
