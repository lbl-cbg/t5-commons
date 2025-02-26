# t5-data


## BilboMD 

BilboMD is an example of storing an analysis output in JAMO. This is accomplished using JAT, a JAMO
feature for grouping files together. To use JAT, a schema (a.k.a. *template*) is needed for defining what
a collection of files and their associated metadata should look like. Currently, there is a single template
that defines bilbomd results: `bilbomd/bilbomd_classic_results.yaml`. This template is used by specifying it
in the JAMO config files.

The script `bilbomd/bilbomd_release.py` can be used to put BilboMD data into JAMO using the JAT. This script
takes two arguments: the [Jira BilboMD](https://taskforce5.atlassian.net/jira/software/c/projects/BILBOMD/boards/11)
issue and the directory containing the BilboMD outputs. This script will use the Jira issue to retrieve necessary
metadata for saving analysis results in JAMO. After saving the analysis results in JAMO, this script will 
create a [Jira asset](https://taskforce5.atlassian.net/jira/servicedesk/assets/object-schema/3?typeId=34) for the 
BilboMD results and close out the Jira issue.

To run this script, the following environment variables must be set:

- `JIRA_HOST` - the hostname for Jira
- `JIRA_USER` - the Jira user to use for authenticating with Jira
- `JIRA_TOKEN` - the Jira PAT to use for authenticating with Jira
- `JAMO_HOST` - the hostname for JAMO
- `BILBOMD_JAMO_TOKEN` - the BilboMD application token for JAMO
