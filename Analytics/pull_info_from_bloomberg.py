__author__ = 'Brett'
#!/usr/bin/env python

#!/usr/bin/env python

#!/usr/bin/env python

import boto3
import csv
import os
import sys
from boto3.dynamodb.conditions import Key

os.environ["AWS_ACCESS_KEY_ID"] = 'HaHaHa'
os.environ["AWS_SECRET_ACCESS_KEY"] = 'HaHaHa'
# Remember to set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in
# the environment before running this program.

AWS_REGION = 'us-east-1'

db = boto3.resource('dynamodb',
                    region_name = AWS_REGION)


def slope_with_data(data_points):
    '''
    only works with indexes whose graphs are good when the slope is poitive
    :param data_points: List of data points
    :return: slope of the data points
    '''
    previous_point = 0
    slope = 0
    for y in range(len(data_points)):
        slope = data_points[y][u'Value'] - previous_point
        previous_point = data_points[y][u'Value']
    if slope < 0:
        return -1
    else:
        return 1

def years_in_review(list_of_indexs, year):
    '''
    how did the country do based on the data from the indices
    :param indexes: all the indexes of a country
    :param year: year to look at
    :return: how the country's indexes did from 2008 to 2013 1 for good, -1 for bad
    '''
    x = year
    year_in_review = 0
    for y in range(len(list_of_indexs)):
        index = list_of_indexs[y]
        table_arg = db.Table(str(x))
        data_from_index = table_arg.query(KeyConditionExpression = Key('Ticker').eq('%s Index' % index))
        year_in_review += slope_with_data(data_from_index['Items'])
    return year_in_review

def numbers_first_csv_builder(countries, list_of_indexs):
    '''
    builds a csv to be used however the feature is the strength of that economy
    :param indexes: all the indexes of a country
    :param countries: list of countries to build csv
    :param actual_gdp_growth: a list of the actual gdp growth over the years
    :return: returns a csv with the data points
    '''
    with open('countries.csv', 'wb') as fp:
        a = csv.writer(fp, delimiter=',')
        data = {}
        all_data = []
        for country in range(len(countries)):
            list_of_index_country = list_of_indexs[country]
            for x in range(2005, 2013):
                gdp_growth = years_in_review(list_of_index_country, x)
                for y in range(len(list_of_index_country)):
                    index = list_of_index_country[y]
                    table_arg = db.Table(str(x))
                    data_from_index = table_arg.query(KeyConditionExpression = Key('Ticker').eq('%s Index' % index))
                    year_index = {}
                    for z in range(len(data_from_index['Items'])):
                        year_index.update({data_from_index['Items'][z][u'Date'] : data_from_index['Items'][z][u'Value']})
                    data.update({'%s Index' % index: year_index})
                all_data.append([gdp_growth, {countries[country]:data}])
        a.writerows(all_data)

def countires_first_csv_builder(countries, list_of_indexs):
    '''
    builds a csv to be used
    :param indexes: all the indexes of a country
    :param countries: list of countries to build csv
    :param actual_gdp_growth: a list of the actual gdp growth over the years
    :return: returns a csv with the data points
    '''
    with open('countries_year_rating.csv', 'wb') as fp:
        a = csv.writer(fp, delimiter=',')
        all_data = []
        for country in range(len(countries)):
            list_of_index_country = list_of_indexs[country]
            for x in range(2005, 2013):
                gdp_growth = years_in_review(list_of_index_country, x)
                all_data.append([countries[country], [gdp_growth, x]])
        a.writerows(all_data)

def csv_builder(countries, list_of_indexs):
    '''
    builds a csv to be used however the feature is the strength of that economy
    :param indexes: all the indexes of a country
    :param countries: list of countries to build csv
    :param actual_gdp_growth: a list of the actual gdp growth over the years
    :return: returns a csv with the data points
    '''
    with open('year_rating_country.csv', 'wb') as fp:
        a = csv.writer(fp, delimiter=',')
        all_data = []
        for country in range(len(countries)):
            list_of_index_country = list_of_indexs[country]
            for x in range(2005, 2013):
                gdp_growth = years_in_review(list_of_index_country, x)
                all_data.append([x, [gdp_growth, countries[country]]])
        a.writerows(all_data)

csv_builder(['Argentina','Australia','Brazil','Canada','China','Eurozone','France','Germany','India','Indonesia',
             'Italy','Japan','Mexico','Russia','South Africa','South Korea','Turkey','United Kingdom','United States'],
            #argentina
            [['ARADTOTQ', 'ARBABAL', 'ARBPCURR', 'ARCCIND', 'ARDMSUMM',
                                     'ARTXTOTL', 'ARWPIMOM','ARCOMOM','ARIPSAMO', 'AREMDEMO', 'ARGQPYOX',
                                     'ARVHTOTL', 'ARVSARTL'],

            #australia
            ['ACRDTPV', 'AUCABAL', 'AUCANXP', 'AUFRA', 'AUITGSB', 'AUNAGDPC', 'AUNAGDPY',
                          'AUPGOP', 'AURSTSA', 'NABSCOND', 'NABSCONF', 'AUBAC', 'AICIPCI', 'AIGISMI',
                          'AIGPMI', 'AUCNQTOT', 'AUHFILM', 'AULFPART', 'AURBA$', 'AUVHMOM%'],

            #brazil,
            ['BPPICM','BRCOCMOM','BRLDDEBT','BSRFTOFD','BZASSUBT','BZCACURR','BZBGPRIM','BZCCI','BZCNCNIS','BZDPNDT%',
              'BZEAMOM%','BZFDTMON','BZLNTMOM','BZLNTOTA','BTIPTL%','BZJCGTOT','BZRTAMPM','BZRTRETM','BZTBBALM',
              'BZPBNDOM','BZPBPRDM','BZTWBALW','BZVPTLVH','BZVXETL'],

            #canada
            ['CACPAMOM','CAGDPMOM','CAHUMOM','CAIPMOM','CALPPROD', 'CAMFCHNG', 'CANLPRTR','CARSCHNG','CATBTOTB',
                 'CAWTMOM'],

            #China
            ['CHBNINDX','CHEFTYOY','CHVAICY','CNCILI','CNCPIYOY','CNDIINRY','CNEVDF','CNEVREPD','CNFAYOY','CNFRBAL$',
             'CNFREXPY','CNFRIMPY','CNPRETLY','CNRSACMY','CNRSCYOY','CNTSICNY','CNTSTCN'],

            #Eurozone
            ['CPEXEMUY','ECCPEMUM','ECCPEMUY','ECCPEST','EUCATLBA','EUCCEMU','EUGNEMUQ','EUGNEMUY','GRZEEUEX',
             'RSSAEMUM','RSWAEMUY','UMRTEMU','XTSBEZ','XTTBEZ'],

            #France
            ['FFCAB12S','FPIPMOM','FREGEGDPQ','FRBDEURO','FRJSTCHG','FRMPMOM','FRPRTOTQ','FRSNTTLM','FRTEBAL',
             'INSECOMP'],

            #Germany
            ['GDPB95YY','GEINYY','GEIOYY','GRCAEU','GRCP20MM','GRFRIAMM','GRGDDDQQ','GRGDEXQ','GRGDGCQ','GRGDICQ',
             'GRIORTMM','GRIPIMOM','GRPFIMOM','GRTBALE','GRWPMOMI'],

            #India
            ['EC10INMS','IBOPCURR','IGDRYOY','INFRIDXY','INFUTOTY','INMTBAL$','INMTEXUY','RBICRRP'],

            #Indonesia
            ['EC13IDCC','IDBALTOL','IDCCI','IDCPIM','IDEXPY','IDGFA','IDVHCLOC','IDVHMTLC'],

            #Italy
            ['ITBCI','ITBDNETE','ITCAEUR','ITCPEM','ITISTSAM','ITNHMOM','ITNSSTN','ITORTSAM','ITPIRLQS','ITPNIMOM',
             'ITPRSANM','ITPSSA','ITTRALEE','ITVHYOY'],

            #Japan
            ['EC10JNMS','JBSIBCLA','JBTARATE','JCOMSHCF','JGDOQOQ','JNBPTRD','JNCRPYOY','JNCSTOTY','JNCVSSY','JNDSNYOY',
             'JNFRTOTL', 'JNNETMOM','JNTBALA','JNTIAIAM','JNVHSYOY','JNVNYOYS','JNWSDOM','JSIABOND','JSIASTCK',
             'JSIHBOND','JSIHSTCK'],

            #Mexico
            ['IGAEYOY','IMEFMAIN','IMEFNMIN','MINVTYOY','MXCFCONF','MXCPCHNG','MXDSPYTD','MXSATOTL','MXSDSUYO',
             'MXTBBAL','MXVPTOTL','MXWRTREM'],

            #russia
            ['RMSNM1','RUAUTTYY','RUBUCFBA','RUCPBIYY','RUFGGFML','RUMERDIY','RUMERAL','RUPPNEWM','RURFUSD','RURSRMOM'
             'RUTBAL','RUZICANY'],

            #south Africa
            ['SABBBAL','SACPIMOM','SACTGDP','SAGDPANN','SAMPTTSM','SANOFP$','SANOGR$','SAPRFMGM','SFPMMOM'],

            #South Korea
            ['KOBLHHD','KOBPCB','KOBPTB','KOBSMC','KOBSNMC','KOCPIMOM','KODSDEPT','KODSDISC','KOEXPTIM','KOFETOT',
             'KOGDPQOQ','KOTRBAL'],

            #Turkey
            ['TUCALNEW','TUCDCONF','TUCOREAL','TUCPIM','TUDPMOM','TUGPCOQS','TUIOSAMM','TUTBALBN','TYCOLEV'],

            #United Kingdom
            ['UKBINPEQ','UKCCI','UKCNALSM','UKCR','UKHBSAMM','UKIPIMOM,','UKISTCMM','UKMPIMOM','UKMSM41M','UKRPI',
             'UKRPMOM','UKTBLGDT','UKTBTTBA'],

            #United States
            ['AHE MOM%','AWH TOTL','COMFCOMF','CPI CHNG','CPTICHNG','DGNOCHNG','DOETFETH','ETSLMOM','FDDSSD','FDIDFDMO',
             'FRNTTNET','FRNTTOL','GDP PIQQ','GDPCPCEC','IP CHNG','JOLTTOTL','NFP TCH','NHSLCHNG','PCE CHNC','PITLCHNG',
             'PRODNFR%','PRUSTOT','SAARTOTL','TMNOCHNG','USCABAL','USEMNCHG','USMMMNCH','USPHTMOM','USTBTOT'],
             ])