# Solar Market Analysis in New York City And Albany
This project explores potential locations for the solar market in New York City and Albany based on latitude and longitude. The data used in the project comes from a solar radiation dataset and a photovoltaic rooftop dataset. We use data from the year 2013, as it is the only year for which both datasets have common records. This project aims to accurately identify promising locations for solar industry development.

# Introduction: 
Solar power is a clean and renewable source of energy. As the world transitions to clean energy, the urban solar market holds great potential globally. Therefore, analyzing the solar power market is crucial for the growth of the solar industry. With this in mind, this project investigates zip code locations for potential solar panel markets.

# Data Source 
- Solar radiation data for year 2013: 1.6 TB
- PhotoVoltaic Data Source for year 2013: 6 GB

# ETL Pipeline
![alt text](https://github.com/umeshkhaniya/TDI_Captsone_project/blob/main/image/captsone_sketch.jpg)


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

Python scripts can be executed directly on an EC2 instance, provided that all essential IAM user permissions are in place.
The Jupyter Notebooks provided here are used to run an EMR cluster and set up the system.


# Workflow Overview Using Standalone Python Scripts
- src/spark_script

# Workflow Overview Using Jupyter Notebooks

- Creating the S3 bucket using Boto3: aws_bucket_creation.ipynb
- Processing 1.6 TB of data using partitioning in EMR Jupyter Notebook: solar_ingest_input.ipynb
- Processing photovoltaic data using EMR Jupyter Notebook and saving it to a local S3 bucket: pv_rooftopinput_processing.ipynb
- Final data processing by joining the two datasets using Geohash; final data is stored in an S3 bucket for analysis: final_processing_data.ipynb
- Visualization on a local computer using a subset of data: visualization.ipynb
  
- Deployment using Heroku (only a small sample of the data is used): Folder: heroku_deploy

# Deploying the Frontend with Heroku and Streamlit
Folder: heroku_deploy
Link: https://tdi-captsone-uk.herokuapp.com


