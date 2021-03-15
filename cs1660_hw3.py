import boto3
import csv
s3 = boto3.resource('s3', aws_access_key_id = '', aws_secret_access_key = '') 
s3.create_bucket(Bucket = 'datacont-tjc98', CreateBucketConfiguration = {'LocationConstraint': 'us-east-2'})
bucket = s3.Bucket("datacont-tjc98")
bucket.Acl().put(ACL = 'public-read')
dyndb = boto3.resource('dynamodb', region_name = 'us-east-2', aws_access_key_id = '', aws_secret_access_key = '')
table = dyndb.create_table(TableName = 'DataTable', KeySchema = [{'AttributeName': 'PartitionKey', 'KeyType': 'HASH'}, {'AttributeName': 'RowKey', 'KeyType': 'RANGE'}], AttributeDefinitions = [{'AttributeName': 'PartitionKey', 'AttributeType': 'S'}, {'AttributeName': 'RowKey', 'AttributeType': 'S'}], ProvisionedThroughput = {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5})
table.meta.client.get_waiter('table_exists').wait(TableName = 'DataTable')
print(table.item_count)
urlbase = "https://s3-us-west-2.amazonaws.com/datacont-tjc98/"
with open("C:\\Users\\timmychr\\1660\\exps.csv", 'r') as csvfile:
	csvf = csv.reader(csvfile, delimiter = ',', quotechar = '|')
	for item in csvf:
		body = open("C:\\Users\\timmychr\\1660\\datafiles\\"+item[3]+".csv", 'rb')
		s3.Object('datacont-tjc98', item[3]).put(Body = body)
		md = s3.Object('datacont-tjc98', item[3]).Acl().put(ACL = 'public-read')
		url = urlbase + item[3]
		metadata_item = {'PartitionKey': item[0], 'RowKey': item[1], 'description': item[4], 'data': item[2], 'url': url}
		table.put_item(Item = metadata_item)
response = table.get_item(Key = {'PartitionKey': 'test2', 'RowKey': '2'})
item = response['Item']
print(item)