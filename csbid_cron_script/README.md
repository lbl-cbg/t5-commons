# CSBID Cron Script


A Python script that automates data retrieval from CSBID, mailin_SAXS, and simplescattering API. The script then pushes this data to the Atlassian taskforce5 Asset. It's currently running as a Spin CronJob.


## Installation

1. Get API Keys from .env file at [https://drive.google.com/file/d/1H4Mxbo6sc3ZDC4OthUwUdDcW9LN-w3Qz/view?usp=drive_link]
2. build with tag:
    ```sh
    docker buildx build --platform linux/amd64 --no-cache -t registry.nersc.gov/m4521/brave-jira:1 .
    ```
    or

    mac chip: --platform linux/rm64
3. run locally:
    ```sh
    docker run -it registry.nersc.gov/m4521/brave-jira:1
    ```

4. push image to Harbor:
    ```sh
    docker login registry.nersc.gov
    docker push registry.nersc.gov/m4521/brave-jira:1
    ```