# Step 5: Complete Project Write Up
## What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?
The goal of my project is to migrate my crontab jobs at AT&T into Airflow. The output are flat files of the data pulled and a single table of the transformed data pushed into an Oracle database. The flat files are needing to archiving the data for troubleshooting or ad hoc deep dives. The Oracle table allows other teams to access the data using PowerBI dashbaords.


## Clearly state the rationale for the choice of tools and technologies for the project.
I chose to migrate my crontab jobs into Airflow for my work at AT&T. Airflow will be able to handle the automation, logging, and alarms. Airflow also allows an easy way to modify my data pipeline or add addition data quality checks. Finally the UI is a big plus!
 
The Vertica and Oracle databases were requirements from other teams and was not a choice.

## Document the steps of the process.
The code pulls AT&T's cell site throughput and number of user data from Vertica Database. Two queries are ran for each of the vendors. Then, it calculates weighted percentile throughput values aggregated by Nation, Market, Submarket, and Vendor. Finally, it pushes new KPI into Oracle database to visual on PowerBI front end. This new KPI will be used to measure customer experience.

## Propose how often the data should be updated and why.
The pipeline is ran daily during maintenance window (1AM-5AM) since the output is aggregated at a daily level

## Post your write-up and final data model in a GitHub repo.
Can't publish project due to sensitive proprietary AT&T data

## Include a description of how you would approach the problem differently under the following scenarios:
### If the data was increased by 100x.
Use postgres instead of oracle database for faster write speeds. Store flat files                                          in S3 instead of locally. Transform the data using hadoop cluster to maximize parallel compute.

### If the pipelines were run on a daily basis by 7am.
This pipeline runs daily currently. It runs during maintenence window from 12am-4am with multiple retries. If all retries fail, then the newst date will not be viewed on the frontend. I would have to troubleshoot, debug, and deploy quality checks to ensure the pipeline does not fail again.

### If the database needed to be accessed by 100+ people.
I would migrate the Oracle database to AWS RDS to handle the scaling. 

