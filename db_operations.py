import inspect
import os
import boto3
import csv
import param_config as c

sys_path = os.path.dirname(os.path.realpath(__file__))
folder_path = os.path.join(sys_path, 'test')
csv_path = os.path.join(folder_path, 'experiments.csv')
sub_path = os.path.join(folder_path, 'exp_files/')


def create_bucket(s3):
    is_exist = False
    try:
        s3.create_bucket(Bucket=c.BUCKET_NAME, CreateBucketConfiguration={'LocationConstraint': c.REGION})
    except Exception as e:
        # print(traceback.format_exc())
        print('FUNC {0} - {1}'.format(inspect.stack()[0][3], e))
        is_exist = True
    bucket = s3.Bucket(c.BUCKET_NAME)
    bucket.Acl().put(ACL='public-read')
    return not is_exist


def create_table(dyndb):
    try:
        table = dyndb.create_table(
            TableName='ExpTable848',
            KeySchema=[
                {'AttributeName': 'PartitionKey', 'KeyType': 'HASH'},
                {'AttributeName': 'RowKey', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'PartitionKey', 'AttributeType': 'S'},
                {'AttributeName': 'RowKey', 'AttributeType': 'S'},
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
    except Exception as e:
        if e.__class__.__name__ == 'ResourceInUseException':
            print('FUNC {0} - WAR: table already exists.'.format(inspect.stack()[0][3]))
            table = dyndb.Table('ExpTable848')
    table.meta.client.get_waiter('table_exists').wait(TableName='ExpTable848')


def upload_bucket(s3, dyndb, path=csv_path):
    table = dyndb.Table('ExpTable848')
    with open(csv_path, "r") as file:
        csvf = csv.reader(file, delimiter=',', quotechar='|')
        next(csvf)
        for item in csvf:
            body = open(sub_path + item[4], 'rb')
            s3.Object(c.BUCKET_NAME, item[4]).put(Body=body)
            s3.Object(c.BUCKET_NAME, item[4]).Acl().put(ACL='public-read')
            url = c.URL_BASE + item[4]
            metadata_item = {'PartitionKey': item[0], 'RowKey': item[0], 'Temp': item[1],
                             'Conductivity': item[2], 'Concentration': item[3], 'url': url}
            try:
                table.put_item(Item=metadata_item)
            except Exception as e:
                print('FUNC {0} - {1}'.format(inspect.stack()[0][3], e))


def main():
    s3 = boto3.resource('s3', aws_access_key_id=c.ACCESS_ID,
                        aws_secret_access_key=c.ACCESS_KEY)
    dyndb = boto3.resource('dynamodb', region_name=c.REGION, aws_access_key_id=c.ACCESS_ID,
                           aws_secret_access_key=c.ACCESS_KEY)
    create_bucket(s3)
    create_table(dyndb)
    upload_bucket(s3, dyndb, csv_path)

    table = dyndb.Table('ExpTable848')
    response = table.get_item(
        Key={
            'PartitionKey': '3',
            'RowKey': '3'
        }
    )
    print(response)


if __name__ == '__main__':
    main()
