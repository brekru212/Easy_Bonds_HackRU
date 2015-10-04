__author__ = 'Brett'

import boto3
import csv
import os
import sys
from boto3.dynamodb.conditions import Key

os.environ["AWS_ACCESS_KEY_ID"] = 'AKIAJ7ZMQ3Q3KTZIVD6Q'
os.environ["AWS_SECRET_ACCESS_KEY"] = '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'
# Remember to set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in
# the environment before running this program.

AWS_REGION = 'us-east-1'

db = boto3.resource('dynamodb',
                    region_name = AWS_REGION)

from oauth2client.client import GoogleCredentials
from googleapiclient.discovery import build

credentials = GoogleCredentials.get_application_default()

def csv_builder(country, list_of_indexs, actual_gdp_growth):
    '''
    builds a csv to be used
    :param indexes: all the indexes of a country
    :param country: country to build csv
    :param actual_gdp_growth: a list of the actual gdp growth over the years
    :return: returns a csv with the data points
    '''
    with open('%s.csv' %country, 'wb') as fp:
        a = csv.writer(fp, delimiter=',')
        data = []
        index_data = []
        for x in range(2005, 2013):
            for y in range(len(list_of_indexs)):
                index = list_of_indexs[y]
                table_arg = db.Table(str(x))
                data_from_index = table_arg.query(KeyConditionExpression = Key('Ticker').eq('%s Index' % index))
                year_index = {}
                for z in range(len(data_from_index['Items'])):
                    year_index.update({data_from_index['Items'][z][u'Date'] : data_from_index['Items'][z][u'Value']})
                data.append(['%s Index' % index, year_index])
            #data.append([actual_gdp_growth[x-2005], index_data])
        a.writerows(data)


service = build('prediction', 'v1.6', credentials=credentials)

#result = service.hostedmodels().predict(project='1093905650860', hostedModelName='sample.sentiment', body={'input': {'csvInstance': ['hello']}}).execute()
result = service.hostedmodels().predict(project='414649711441', hostedModelName='regression', body={'input': {'csvInstance': [csv_builder('Australia', ['ACRDTPV', 'AUCABAL', 'AUCANXP', 'AUFRA', 'AUITGSB', 'AUNAGDPC', 'AUNAGDPY',
                                      'AUPGOP', 'AURSTSA', 'NABSCOND', 'NABSCONF', 'AUBAC', 'AICIPCI', 'AIGISMI',
                                     'AIGPMI', 'AUCNQTOT', 'AUHFILM', 'AULFPART', 'AURBA$', 'AUVHMOM%'],
            [1,1,1,1,1,-1,1,-1,1])]}}).execute()


print('Result: %s ' % (repr(result)))
