# Databricks notebook source
# MAGIC %md
# MAGIC ##### 1. Create a Databricks workspace 
# MAGIC - Create a Databricks workspace on AWS and open the workspace. 
# MAGIC - This will connect to the Databricks UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Enable web terminal in your databricks account
# MAGIC   
# MAGIC - Open "Settings" option from Accounts menu (top-right)
# MAGIC - 2.2 Go to "Setting" -> Compute -> Enable 'Web Terminal' and refresh the page.
# MAGIC 		
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Create a single node cluster
# MAGIC  
# MAGIC - Open the **Compute** menu option and click on **Create Cluster** button.
# MAGIC - Fill in the details as follows:	
# MAGIC   - Name: Demo Cluster
# MAGIC   - Policy: Unrestricted - Single Node
# MAGIC   - Use photon acceleration: Uncheck
# MAGIC   - Node type: m4.large (8 GB Memory, 2 Cores)
# MAGIC   - Terminate after 15 minutes of inactivity
# MAGIC   - Leave all other options as defaults
# MAGIC - Click on **Create Cluster** button
# MAGIC - Wait until the cluster state comes to 'running' (green tick mark)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Work with Web terminal
# MAGIC - Open the cluster details page by clicking on the cluster link
# MAGIC - Click on **Apps** tab
# MAGIC - Click on **Web Terminal** button to launch web terminal.	 
# MAGIC
# MAGIC - **Here we can do any standard operation that are allowed under Ubuntu Linux.**
# MAGIC - Run some sample commands
# MAGIC   - `uname -a`   (OS & version info)
# MAGIC   - `python`     (Python terminal)

# COMMAND ----------


