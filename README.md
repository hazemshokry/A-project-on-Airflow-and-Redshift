A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to.

Data source is divided by two main sources, songs meta-data and logs of what users are listening (user activity) which they are currently stored as JSON files.

#### Let's see an example for both sources:
##### Songs meta-data:
![Songs meta-data!](/images/songs.png "Songs meta-data")

##### Logs:
![Logs!](/images/logs.png "Logs")


The company goal is to query this data sets and find some analytics and insights. One example is to counts songs that are played this month for a specific artist.

## Solution
![Architecture!](/images/arch.png "Architecture")

### Here's the Airflow orchestration will look like:
![Airflow!](/images/airflow.png "Airflow")



##### Step1: Design  star schema 
![Entity Relationship Diagram!](/images/diagram.png "Entity Relationship Diagram")

##### Step2: Copying data from S3 Bucket to Amazon Redshift staging area

##### Step3: Processing data from in Amazon Redshift and load them into my star schema designed tables to be ready for analytics.

##### Step4: Processing Data Quality Checks for Number of records and NULL values


#### Project Files:
1. dags folder: Contains dags and sub dags for my workflow.
2. plugins folder: Contains implemnted custom operators and helper functions.
3. docker-compose yml: Helps you setup your docker enviroment if you would like to run it on your local


#### How to run the project:
1. Put the requiered credentials (AWS and REDSHIFT) in your airflow connections page.
2. Run your webserver weather you're running it on your local or on your docker.
3. Start the dag and watch out the pipeline is being completed.
