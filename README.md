# DataTalks Capstone Project
![imdb logo](https://upload.wikimedia.org/wikipedia/commons/thumb/6/69/IMDB_Logo_2016.svg/575px-IMDB_Logo_2016.svg.png?20200406194337)

## :movie_camera: Internet Movie Database (IMDB)

#### IMDB is an online database of information related to films, TV series, podcasts, home videos, video games, etc. 

#### To access data files, click [here](https://datasets.imdbws.com/). To read the dataset documentation, click [here](https://www.imdb.com/interfaces/).

## :grey_question: Questions We're All Asking
1. What are the top voted TV shows?
2. What is the hottest genre year over year?
3. 

## :twisted_rightwards_arrows: Data Pipeline
![pipeline](images/data_pipeline.png)

## :sparkles: Technologies Used
- Cloud: `Google Cloud Storage`
- Infrastructure as code (IaC): `Terraform`
- Workflow Orchestration: `Prefect`
- Data Warehouse: `BigQuery`
- Transformation: `DBT`
- Batch Processing: `Spark`
- Data Visualization: `Looker Studio`

## :bar_chart: Dashboard

## :notes: Join the Club!
1. Fork and Clone the repository
   
3. Steps to reproduce the project onto your machine (assuming you have a [Google Cloud account](https://cloud.google.com/free)):
    a. Create a new [GCP project](https://console.cloud.google.com/cloud-resource-manager)
    b. Setup [service account & authentication](https://cloud.google.com/docs/authentication/getting-started) for this project
       * Download service-account-keys (.json) for auth.
    c. Download [SDK](https://cloud.google.com/sdk/docs/quickstart) for local setup
    d. Set environment variable to point to your downloaded GCP keys:
      ```shell
      export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
      
      # Refresh token/session, and verify authentication
      gcloud auth application-default login
      ```

4. Setup for Access
    a. [IAM Roles](https://cloud.google.com/storage/docs/access-control/iam-roles) for Service account:
    * Go to the *IAM* section of *IAM & Admin* https://console.cloud.google.com/iam-admin/iam
    * Click the *Edit principal* icon for your service account.
    * Add these roles : **Storage Admin** + **BigQuery Admin**
    
    b. Enable these APIs for your project:
    * https://console.cloud.google.com/apis/library/iam.googleapis.com
    * https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com
    
    c. Please ensure `GOOGLE_APPLICATION_CREDENTIALS` env-var is set.
    ```shell
    export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
    ```

5. Setup Terraform
    a. On OS, run the following commands:
    ```
    brew tap hashicorp/tap
    brew install hashicorp/tap/terraform
    ```
    b. Remaining instructions [here](https://github.com/Trizzlenova/datatalks-imdb-capstone/tree/main/terraform) in the terraform directory

6. Setup Prefect
    a. Sign-up for [Prefect](https://app.prefect.cloud/auth/login) start a workspace
    b. Create the [prefect blocks](https://docs.prefect.io/latest/concepts/blocks/)
    C. 

