import argparse
import datetime
import json
import googleapiclient.discovery
import sys

class GCPTransfer():
    __doc__ = """
Command-line sample that creates a one-time transfer from Amazon S3 to 
Google Cloud Storage.

This sample is used on this page:
https://cloud.google.com/storage/transfer/create-transfer
"""

    @staticmethod
    def create(**kwargs):
        print(kwargs)

        date = datetime.datetime.strptime(kwargs.get('date'), '%Y/%m/%d')
        time = datetime.datetime.strptime(kwargs.get('time'), '%H:%M')

        description = kwargs.get('description') 
        project_id = kwargs.get('project_id') 
        year = date.year
        month = date.month
        day = date.day
        hours = time.hour 
        minutes = time.minute
        source_bucket = kwargs.get('source_bucket') 
        access_key = kwargs.get('access_key') 
        secret_access_key = kwargs.get('secret_access_key')
        sink_bucket = kwargs.get('sink_bucket')


        """Create a one-off transfer from Amazon S3 to Google Cloud Storage."""
        storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1')

        # Edit this template with desired parameters.
        # Specify times below using US Pacific Time Zone.
        transfer_job = {
            'description': description,
            'status': 'ENABLED',
            'projectId': project_id,
            'schedule': {
                'scheduleStartDate': {
                    'day': day,
                    'month': month,
                    'year': year
                },
                'scheduleEndDate': {
                    'day': day,
                    'month': month,
                    'year': year
                },
                'startTimeOfDay': {
                    'hours': hours,
                    'minutes': minutes
                }
            },
            'transferSpec': {
                'awsS3DataSource': {
                    'bucketName': source_bucket,
                    'awsAccessKey': {
                        'accessKeyId': access_key,
                        'secretAccessKey': secret_access_key
                    }
                },
                'gcsDataSink': {
                    'bucketName': sink_bucket
                }
            }
        }

        result = storagetransfer.transferJobs().create(body=transfer_job).execute()
        print('Returned transferJob: {}'.format(
            json.dumps(result, indent=4)))

    @staticmethod
    def check(**kwargs):

        project_id = kwargs.get('project_id')
        job_name = kwargs.get('job_name')
        """Review the transfer operations associated with a transfer job."""
        storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1')

        filterString = (
            '{{"project_id": "{project_id}", '
            '"job_names": ["{job_name}"]}}'
        ).format(project_id=project_id, job_name=job_name)

        result = storagetransfer.transferOperations().list(
            name="transferOperations",
            filter=filterString).execute()
        print('Result of transferOperations/list: {}'.format(
            json.dumps(result, indent=4, sort_keys=True)))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=GCPTransfer.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)


    subparsers = parser.add_subparsers(
        help='Choose one of functions.'
    )
    parser_create = subparsers.add_parser(
        'create',
        help='create a transfer job'
    )
    parser_create.add_argument('description', help='Transfer description.')
    parser_create.add_argument('project_id', help='Your Google Cloud project ID.')
    parser_create.add_argument('date', help='Date YYYY/MM/DD.')
    parser_create.add_argument('time', help='Time (24hr) HH:MM.')
    parser_create.add_argument('source_bucket', help='Source bucket name.')
    parser_create.add_argument('access_key', help='Your AWS access key id.')
    parser_create.add_argument('secret_access_key', help='Your AWS secret access '
                        'key.')
    parser_create.add_argument('sink_bucket', help='Sink bucket name.')
    parser_create.set_defaults(func=GCPTransfer.create)

    parser_check = subparsers.add_parser(
        'check',
        help='check a transfer job'
    )
    parser_check.add_argument('project_id', help='Your Google Cloud project ID.')
    parser_check.add_argument('job_name', help='Your job name.')
    parser_check.set_defaults(func=GCPTransfer.check)


    args = parser.parse_args()
    inputs = vars(args)
    print(inputs)
    args.func(**inputs) 
