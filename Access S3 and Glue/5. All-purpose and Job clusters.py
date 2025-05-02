# Databricks notebook source
# MAGIC %md
# MAGIC ##### 1. Create and launch Databricks workspace on AWS

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Create a single node 'All-purpose compute' cluster	

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Create a new notebook and run it using all-purpose cluster.
# MAGIC
# MAGIC - Write a sample Spark program by reading some CSV files from your catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Create a new Job and run it using all-purpose cluster.
# MAGIC - Click on **New** -> **Job** menu option and create a job using the following options	
# MAGIC 	- Task name: **My_Job**
# MAGIC 	- Type: Notebook
# MAGIC 	- Source: Workspace
# MAGIC 	- Path: Browse and select a notebook (that you created earlier)
# MAGIC 	- Cluster: Select the cluster you created above  (ex: CTSDemoCluster)		
# MAGIC 	- Save the job		
# MAGIC - Click on **Run now** button to run the job.
# MAGIC - Click on **job runs** tab (under Workflows) to monitor the status of the job run
# MAGIC - Wait until the job is completed. The status should show 'Succeeded'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Terminate the all-purpose cluster you created earlier (in step 2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6. Run the job using 'job cluster'  
# MAGIC - Click on **Workflows** menu option and select the job.	
# MAGIC - Under **Compute** section select **Swap** and select **Add new job cluster** option	
# MAGIC - Select appropriate cluster options (Single node, Node type etc.) and **Confirm**	
# MAGIC - Cick on **Update** to update your job to use the job cluster.	
# MAGIC - Click on **Run now** to run the job.	
# MAGIC - Go to **Job runs** tab to monitor the status of the job run. 
# MAGIC - **Note**: Observe the amount of time it has taken to complete the job (for me it look almost 6 minutes, and of that about 5 minutes to spin up the cluster)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7. Create an instance pool
# MAGIC   
# MAGIC - Click on **pools** tab under **Compute** menu option with the following details:
# MAGIC   - Name: Demo pool
# MAGIC   - Min Idle: 1
# MAGIC   - Max Capacity: 2
# MAGIC   - Idle Instance Auto Termination: 15 minutes
# MAGIC   - Instance Type: Standard_F4
# MAGIC   - Preloaded Databricks Runtime Version: 12.2 LTS

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 8. Create a new single node 'All-purpose compute' cluster	in the pool
# MAGIC   
# MAGIC - Create a new single node cluster	
# MAGIC - Select **_Demo pool_** created in the previous step as **Node type**

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 9. Run the job using 'job cluster' using the cluster in the pool
# MAGIC   
# MAGIC - Click on **Workflows** menu option and select the job.
# MAGIC - Under **Compute** section, select **Swap** and select the cluster created in the above step.
# MAGIC - Cick on **Update** to update your job to use the job cluster.
# MAGIC - Click on **Run now** to run the job.
# MAGIC - Go to **Job runs** tab to monitor the status of the job run. 
# MAGIC 	     
# MAGIC The job should now run much faster, as there is no cluster provisioning step here.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 10. Cleanup/terminate your resources
# MAGIC - Delete the instance pool. 
# MAGIC - Delete the all purpose cluster
# MAGIC - Delete the jobs.
