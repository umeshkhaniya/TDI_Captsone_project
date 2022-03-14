# Solar Market Analysis in New York City And Albany
This project involves the potential location for the solar market in New York City And Albany based on zip code. The data used in the project are from the solar radiation dataset and photovoltaic rooftop data set. The data used here is from the year 2013 since we only have common data for both sets for the year 2013. This project will accurately predict the potential location of the solar industry.

# Introduction: 
Solar power is a clean and renewable source of energy. As the world is moving to clean energy, the urban solar market has a great future in the world. Therefore, analyzing the solar power market is very important for solar industries. By keeping in mind, this project is investigating the zip code location for the solar panel market.

# Data Source 
- Solar radiation data for year 2013: 1.6 TB
- PhotoVoltaic Data Source for year 2013: 6 GB

# ETL Pipleline
- Used the Pyspark for analysis data in aws EMR cluster.

# Bootstrapping Script
The software and dependencies requiremnts can be found in bootstrap_cap.sh file
 sudo python3 -m pip install \
  awscli \
 boto3 \
 ec2-metadata \
 s3fs \
 cython \
 h5py \
 pygeohash \
 shapely \
 uszipcode

# Scripts
The jupyter notebook is provided here is used  in running  EMR cluster to setup the system.
Python Script can directly used to run in the EC2 instance.
1. Creating the S3 bucket using boto3: aws_bucket_creation.ipynb
2. Processing 1.6 TB of data using the partition in EMR jupyter Notebook: solar_ingest_input.ipynb
3. Processing Photovoltic data using EMR jupyter Notebook and saved into local S3 bucket: pv_rooftopinput_processing.ipynb
4. Final processing of data by joining the two data using the Geohash and final data is stored in S3 bucket for analysis. final_processing_data.ipynb
5. Visualization in Local computer taking few data points: visualization.ipynb
6. Heroku is used for deployement. Only few Samples of data are taken:Folder: heroku_deploy

# Frontend Deployment using Heroku and Streamlit: heroku_deploy


