import pandas as pd
import boto3

accessKey = 'Your Wasabi Access Key here'
secretKey = 'Your Wasabi Secret Key here'

session = boto3.Session(
    aws_access_key_id=accessKey, ### Found at: https://catalog.safegraph.io/app/browse -- under start here and 'Reveal Access Key'
    aws_secret_access_key=secretKey, ### Found at: https://catalog.safegraph.io/app/browse -- under start here and 'Reveal Access Key'
    region_name = 'us-east-1' ### Note this used to be us-west-2 --- it has changed to us-east-1
)

base = 'social-distancing/v2/{}/{}/{}/' #.format('2020','01','31')

bucket = 'sg-c19-response'
subdirectory = '/social-distancing/v2'

s3 = session.client('s3', endpoint_url = 'https://s3.wasabisys.com')
file_sub =  [x.split('-') for x in
    pd.date_range(start='2020-02-15', end = '2020-02-28').to_frame().astype(str)[0].to_list()
        ] +  [x.split('-') for x in
    pd.date_range(start='2020-05-01', end = '2020-05-14').to_frame().astype(str)[0].to_list()] # limit list
for f in file_sub:
    dest = '{}-{}-{}-social-distancing.csv.gz'.format(f[0],f[1],f[2])
    s3.download_file(
        Bucket = bucket,
        Key = base.format(f[0],f[1],f[2])+'{}-{}-{}-social-distancing.csv.gz'.format(f[0],f[1],f[2]),
        Filename = dest)
    print(f'download for {f} complete')