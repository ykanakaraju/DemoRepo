# Databricks notebook source
# MAGIC %md
# MAGIC ##### 1. Setup an S3 bucket to store our datasets and upload `retail_csv` dataset
# MAGIC - Setup an S3 bucket to store our datasets using Admin account.
# MAGIC 	- Bucket Name: `cts-databricks`
# MAGIC 	- Data to be uploaded: `retail_csv` folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Create AWS IAM Role for EC2 instances. 
# MAGIC
# MAGIC - Open IAM console and create an EC2 Role
# MAGIC   - AWS Service Use-case: `EC2`
# MAGIC   - Policy to be added: _No need to attach any policies/permissions at this time_
# MAGIC   - Role name: `DBWorkspaceEC2Role`
# MAGIC
# MAGIC - Click on the 'View role' button and make a note of 'Instance profile ARN' & 'Role ARN'
# MAGIC
# MAGIC   - **Note:** If you want to attach an IAM role to an ec2 instance you need to use Instance Profile.When you use the CLI or SDK, you need to create the instance profile separately and attach it to the IAM role and then attach the instance profile to the ec2 instance.
# MAGIC
# MAGIC   - Instance profile ARN: `arn:aws:iam::<YOUR-ACCOUNT-ID>:instance-profile/DBWorkspaceEC2Role`
# MAGIC   - IAM role ARN: `arn:aws:iam::<YOUR-ACCOUNT-ID>:role/DBWorkspaceEC2Role`
# MAGIC 		   
# MAGIC - Attach an inline policy to the above role to access '`cts-databricks`' S3 bucket
# MAGIC   - JSON: As shown towards the end of this document
# MAGIC   - Name: `DBWorkspaceS3Policy`
# MAGIC 		
# MAGIC - Attach `AWSGlueServiceRole` policy to the above role.
# MAGIC 	
# MAGIC - **NOTE:** After these steps, the '`Permissions policies`' of the role should have two policies:
# MAGIC   - `AWSGlueServiceRole`
# MAGIC   - `DBWorkspaceS3Policy`

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Create and login to a Databricks workspace. 
# MAGIC 	
# MAGIC - Login to your Databricks account and create a workspace. (or open an existing workspace)
# MAGIC 	- **Create workspace** -> **Quick start (recommended)**
# MAGIC 	- Workspace Name: AWS Databricks WS
# MAGIC 	- AWS Region: us-east-1
# MAGIC 	- **This will launch an AWS Cloud Formation stack**
# MAGIC
# MAGIC - Enter your account password in AWS Cloud Formation stack and click on **Create stack**
# MAGIC 	- This will provision the workspace resources on AWS.
# MAGIC - Refresh your Databricks workspace page and wait until the status is 'Running'
# MAGIC - Open the workspace and sign-in to the Databricks UI using your databricks credentials.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Add 'PassRole' policy (on EC2 role) to the Databricks IAM role
# MAGIC  
# MAGIC - Open the databricks workspace page (from Account Console) and click on the workspace.
# MAGIC 	
# MAGIC - Make a note of ARN key under credentials card. <br/>
# MAGIC 		(for example: `arn:aws:iam::<YOUR-AWS-ACCOUNT-ID>:role/databricks-workspace-stack-51969-role` )
# MAGIC 		
# MAGIC - Open your AWS account, open IAM and search for the above role (i.e databricks-workspace-stack-51969-role)
# MAGIC 	
# MAGIC - Click on the role and click on the policy name to edit.
# MAGIC 	
# MAGIC - Append the pass-role JSON script provided below and save changes. 
# MAGIC 	
# MAGIC 		{
# MAGIC 			"Effect": "Allow",
# MAGIC 			"Action": "iam:PassRole",
# MAGIC 			"Resource": "arn:aws:iam::<YOUR-AWS-ACCOUNT-ID>:role/DBWorkspaceEC2Role"
# MAGIC 		}		
# MAGIC
# MAGIC - NOTE: Make sure your account name and policy name are correct. 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Register AWS IAM Instance Profile with Databricks Account.
# MAGIC 	
# MAGIC - Login to your databricks UI with the workspace you created earlier	
# MAGIC - Click on **Account** -> **Settings** -> **Security** menu option	
# MAGIC - Click on **Instance profiles** -> **Manage** button -> **Add instance profile** button.	
# MAGIC - Provide the **Instance profile ARN** ('IAM role ARN' will be populated) and click on **Add**

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6. Create a databricks cluster and configure Glue Data Catalog as the metastore on your cluster
# MAGIC
# MAGIC - Create a single node cluster with the following details:
# MAGIC   - Name: CTS Demo Cluster
# MAGIC   - Policy: Unrestricted - Single Node
# MAGIC   - Use photon acceleration: Uncheck
# MAGIC   - Node type: m5d.large (8 GB Memory, 2 Cores)
# MAGIC   - Terminate after 15 minutes of inactivity
# MAGIC   - Instance profile: **DBWorkspaceEC2Role**
# MAGIC   - Expand **Advanced Options** section and select **Spark** tab
# MAGIC 		- Add the following line to **Spark Config** section	
# MAGIC 			- `spark.databricks.hive.metastore.glueCatalog.enabled true`
# MAGIC - Leave all other options as defaults
# MAGIC - Click on **Create compute** button

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7. Create a Glue crawler to crawl the 'retail_csv' s3 bucket.
# MAGIC
# MAGIC Open Glue service console and create a crawler as follows:
# MAGIC
# MAGIC - database: retail_csv
# MAGIC - crawler: retail_csv crawler
# MAGIC - S3 bucket to crawl: `s3://cts-databricks/retail_csv/`
# MAGIC - IAM Role: AWSGlueServiceRole-retail_csv		
# MAGIC - Run the crawler and wait until the tables are created.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 8. Query the Glue tables (which are linked to S3 buckets) using spark
# MAGIC  
# MAGIC - Create a new notebook and run the following commands in separate cells.<br/><br/>
# MAGIC
# MAGIC 	
# MAGIC 		df1 = spark.read.csv("s3://cts-databricks/retail_csv/orders/")
# MAGIC 		display(df1)
# MAGIC
# MAGIC 		spark.sql("show databases").show()		
# MAGIC 		spark.sql("use retail_csv")		
# MAGIC 		spark.sql("select * from orders").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Inline Policy JSON Script
# MAGIC - Replace `<YOUR_BUCKET-NAME>` with your bucket name

# COMMAND ----------

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
     "Resource": [
        "arn:aws:s3:::<YOUR_BUCKET-NAME>"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:PutObjectAcl"
      ],
      "Resource": [
         "arn:aws:s3:::<YOUR_BUCKET-NAME>/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListAllMyBuckets"
      ],
     "Resource": [
        "arn:aws:s3:::*"
      ]
    }
  ]
}

# COMMAND ----------


