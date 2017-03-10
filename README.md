Data_Engineering_Final_Project
==============================

Architecture for collecting, processing, storing, and managing GDELT data.

Project Organization
------------

    ├── LICENSE
    ├── README.md          <- The top-level README for developers using this project.
    ├── data               <- Bucket to hold data created by ./src/get_gdelt.py
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── notebooks          <- Jupyter notebooks. 
    │   ├── GDELT_Architecture.ipynb <- overview of architecture and data demo.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   ├── example_report.html <- Example output of Spark analysis that sits on S3.
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         create with `pip freeze > requirements.txt`,  
    |                         execute with "sudo `which pip` install -r requirements.txt"
    │
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    |   ├── spinup_ec2.sh  <- Launches an rx.large EC2 instance.
    |   ├── bootstrap_ec2.sh <- Install dependencies on EC2
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── get_gdelt.py <- Requests most recent batch of gdelt data and saves to S3 as parquet. [AWS EC2]
    │   │   └── gkg_cooccurences_pyspark.py <-Spark processing and output analysis to S3 and Redshift. [AWS EMR & Redshift]

--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
