# Databricks notebook source
# MAGIC %md
# MAGIC ##### 1. Make sure you have a valid AWS account.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Sign-up for a Databricks account with 'AWS' as the cloud provider
# MAGIC
# MAGIC -  Sign-up using the following URL: https://www.databricks.com/try-databricks
# MAGIC - Fill the details with a valid email account and click on 'Continue' button
# MAGIC - Select 'Amazon Web Services' as the cloud provider
# MAGIC - Complete the sign-up process by following the instructions sent in the email.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Sign in to the databricks account console
# MAGIC
# MAGIC - sing-in using the URL: https://accounts.cloud.databricks.com/login

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Create a Databricks workspace
# MAGIC
# MAGIC - Click on workspaces menu option
# MAGIC - Click on **Create Workspace** drop-down and select **Quickstart**
# MAGIC - Provide a workspace name (use only lower-case) and select a region (ex: us-east-1)
# MAGIC - Click on **Start Quickstart**		
# MAGIC 	- Name: databricksdemows1
# MAGIC 	- Region: us-east-1
# MAGIC - Sign in to your AWS account (if not already signed-in)	
# MAGIC - This will launch a CloudFormation stack and provisions all the required resources.
# MAGIC 	- Verify the details
# MAGIC 	- Provide your databricks account password
# MAGIC 	- Check acknowledgement checkbox
# MAGIC 	- Click on 'Create stack' button
# MAGIC - Wait until the CloudFormation stack completes provisioning your resources.	

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Launch you Databricks workspace
# MAGIC
# MAGIC - Go to **Workspaces** page on Databricks (refresh if required) and click on **Open** link
# MAGIC - Sign in (again) to your Databricks account associated with this workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6. Cleanup your resources (if you are not continuing with other labs)
# MAGIC
# MAGIC - Go to 'Workspaces' page in your Databricks account.	
# MAGIC - Select 'Delete' option from the menu (verticle dots icon)
# MAGIC 	- This deletes the workspace
# MAGIC - Login to your AWS account 
# MAGIC 	- Empty the S3 buckets created for this workspace
# MAGIC 	- Delete the Cloudformation stack

# COMMAND ----------


