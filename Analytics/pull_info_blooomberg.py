__author__ = 'Brett'
#!/usr/bin/env python

#!/usr/bin/env python

#!/usr/bin/env python

import boto3
import os
from boto3.dynamodb.conditions import Key

os.environ["AWS_ACCESS_KEY_ID"] = 'none of your business'
os.environ["AWS_SECRET_ACCESS_KEY"] = 'none of your business'
# Remember to set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in
# the environment before running this program.

AWS_REGION = 'us-east-1'

db = boto3.resource('dynamodb',
                    region_name = AWS_REGION)

table = db.Table('2012')
data = table.query(KeyConditionExpression = Key('Ticker').eq('UKRPI Index') & Key('Date').between('2012-01-01', '2012-06-30'))

print data['Items']