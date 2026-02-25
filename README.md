# S3 Pusher

Utility to watch directory for changes and move modified files to AWS S3.


## Usage

```
usage: s3pusher [-h] [--bucket BUCKET] [--hostname HOSTNAME] [--log-json] [--debug] directory [directory ...]

S3 Pusher

positional arguments:
  directory            Directory to watch for changes

options:
  -h, --help           show this help message and exit
  --bucket BUCKET      S3 bucket name
  --hostname HOSTNAME  Hostname to include in S3 object key
  --log-json           Log in JSON format
  --debug              Enable debugging
```


## Authentication

Environment variables used for authentication can be found in the [Boto3 documentation](https://docs.aws.amazon.com/boto3/latest/guide/configuration.html#using-environment-variables).


## Object Names

Files will be uploaded to the specified bucket in the following format:

```
year=YYYY/month=MM/day=DD/hour=HH/minute=MM/second=SS/hostname=HOSTNAME/uuid=UUID/FILENAME
```
