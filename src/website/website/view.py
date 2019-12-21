from django.http import HttpResponse
from django.shortcuts import render
import pandas_gbq
from google.oauth2 import service_account
import os

# Make sure you have installed pandas-gbq at first;
# You can use the other way to query BigQuery.
# please have a look at
# https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-nodejs
# To get your credential
app_dir = os.path.dirname(__file__)
service_path = os.path.join(app_dir,'static/eecs-e6893-edu-56ce9c449829.json')

project_id = "eecs-e6893-edu"
credentials = service_account.Credentials \
                .from_service_account_file(service_path)

def map(request):
    import json

    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = project_id

    dataset = 'usheatmap'         # the name of dataset in BigQuery
    # table = 'final'   # the name of table in BigQuery
    table = 'training_data'   # the name of table in BigQuery
    table_id = '{0}.{1}'.format(dataset,table)
    # cols = ['ai', 'data','good','movie','spark'] # column names
    data = {}
    data['data'] = []

   # query the table, return as pandas df.
    # SQL = "SELECT * FROM `{}` ORDER BY date ASC LIMIT 8".format(table_id)
    SQL = "SELECT * FROM `{}` ORDER BY date ASC".format(table_id)
    df = pandas_gbq.read_gbq(SQL)
    df.fillna(-1,inplace=True) # fill NaN/Na with -1
    # iterate each row of the dataframe

    def getGeoid(row):
        statefp = row['state'].split("_")[0]
        countyfp = row['county'].split("_")[0]
        return "{}{}".format(statefp,countyfp)

    # add the geoid (FIXME: this should have been included in original processing)
    df['geoid'] = df.apply (lambda row: getGeoid(row), axis=1)
    # j = (df.groupby(['date','geoid'], as_index=False)
    #              .apply(lambda x: x[['vci','tci','vhi','tasmin','tasmax','pr']].to_dict('r'))
    #              .reset_index()
    #              .rename(columns={0:'Test'})
    #              .to_json(orient='records'))

    # pprint.pprint(j)
    # sys.exit()
    tmp = {}
    for index, row in df.iterrows():
        dt_date = row['date'].to_pydatetime().strftime('%Y-%m-%d')
        if (dt_date != "2018-01-08"):
            continue;
        # statefp = row['state'].split("_")[0]
        # countyfp = row['county'].split("_")[0]
        # geoid = "{}{}".format(statefp,countyfp)
        # geoid = SSCCC, SS = State FIPS, CCC = County FIPS
        # BQ has state = SS_Name, county = CCC_Name

        tmp = { 'date' : dt_date, \
                'geoid': row['geoid'], \
                'value' : { 'vci' : row['vci'], \
                            'tci':row['tci'], \
                            'vhi':row['vhi'], \
                            'tasmin':row['tasmin'], \
                            'tasmax':row['tasmax'], \
                            'pr':row['pr'] \
                        } \
            }

        data['data'].append(tmp)

    # return render(request, 'map.html', {'results':data,'geojson':geojson_data})
    return render(request, 'map.html', data)

# Notes:
# desired output, to organize by date
        # 'date1': [
        #   {
        #       'geoid': geoid1,
        #       'values':   {
        #                       'vci': vci,
        #                       'tci': tci,
        #                       ...
        #                   }
        #   },
        #   {
        #       'geoid': geoid2,
        #       'values':   {
        #                       'vci': vci,
        #                       'tci': tci,
        #                       ...
        #                   }
        #   }, ...
        # ]
        #
        #  # 'date2': [
        #   {
        #       'geoid': geoid1,
        #       'values':   {
        #                       'vci': vci,
        #                       'tci': tci,
        #                       ...
        #                   }
        #   },
        #   {
        #       'geoid': geoid2,
        #       'values':   {
        #                       'vci': vci,
        #                       'tci': tci,
        #                       ...
        #                   }
        #   }, ...
        # ]


# hello world page
def hello(request):
    context = {}
    context['content1'] = 'Hello World!'
    return render(request, 'helloworld.html', context)

# deprecated
# def dashboard(request):
#     pandas_gbq.context.credentials = credentials
#     pandas_gbq.context.project = project_id

#     dataset = 'usheatmap'         # the name of dataset in BigQuery
#     table = 'final'   # the name of table in BigQuery
#     table_id = '{0}.{1}'.format(dataset,table)
#     # cols = ['ai', 'data','good','movie','spark'] # column names
#     data = {}
#     data['data'] = []

#     # query the table, return as pandas df.
#     SQL = "SELECT * FROM `{}` ORDER BY date ASC LIMIT 8".format(table_id)
#     df = pandas_gbq.read_gbq(SQL)
#     # iterate each row of the dataframe
#     tmp = {}
#     for index, row in df.iterrows():
#         dt_date = row['date'].to_pydatetime().strftime('%Y-%m-%d')

#         tmp = { 'date' : dt_date, \
#                 'count' : { 'vci' : row['vci'], \
#                             'tci':row['tci'], \
#                             'vhi':row['vhi'], \
#                             'tasmin':row['tasmin'], \
#                             'tasmax':row['tasmax'], \
#                             'pr':row['pr'] \
#                         } \
#             }
#         data['data'].append(tmp)

#     return render(request, 'dashboard.html', data)


# def connection(request):
#     pandas_gbq.context.credentials = credentials
#     pandas_gbq.context.project = "Your-Project"
#     SQL1 = ''
#     df1 = pandas_gbq.read_gbq(SQL1)

#     SQL2 = ''
#     df2 = pandas_gbq.read_gbq(SQL2)

#     data = {}

#     '''
#         TODO: Finish the SQL to query the data, it should be limited to 8 rows.
#         Then process them to format below:
#         Format of data:
#         {'n': [xxx, xxx, xxx, xxx],
#          'e': [{'source': xxx, 'target': xxx},
#                 {'source': xxx, 'target': xxx},
#                 ...
#                 ]
#         }
#     '''
#     return render(request, 'connection.html', data)
