import base64
import requests
import io
import os
import gzip
import json
import psycopg2
from datetime import date
from datetime import timedelta
from zipfile import ZipFile 

endpoint = 'https://amplitude.com/api/2/export'
end_date = date.today()
start_date = end_date - timedelta(days=8)

secret_string = os.environ['AMPLITUDE_KEY'] + ':' + os.environ['AMPLITUDE_SECRET']
secret_string_encoded = secret_string.encode('ascii')
secret_string_encoded_base64 = base64.b64encode(secret_string_encoded)

headers = {
        "Authorization": "Basic %s" % secret_string_encoded_base64.decode("ascii")
        }

con = psycopg2.connect(
        database=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT']
        )
cur = con.cursor()


while (start_date < end_date):
    end_partial = start_date + timedelta(days=3)
    start_string = '{year:0004d}{month:02d}{day:02d}T00'.format(year = start_date.year, month = start_date.month, day = start_date.day)
    end_string = '{year:0004d}{month:02d}{day:02d}T23'.format(year = end_partial.year, month = end_partial.month, day = end_partial.day)
    start_date = end_partial
    print("Começando o dump da data de inicio: "+start_string+" e final: "+end_string)
    url = endpoint + '?start='+start_string+'&end='+end_string
    response = requests.get(url, headers = headers)
    data = response.content
    contt = io.BytesIO(data)
    with ZipFile(contt, 'r') as zip: 
        zip.extractall() 
    all_directories = [x[0] for x in os.walk('.')]
    jsons_array = []
    for i in all_directories:
        if len(i) > 2 and i[2] == '.':
            continue
        if (i not in ('.', './.ipynb_checkpoints', './upload_okrs', './upload_okrs/legacy') and 'checkpoint' not in i):
            files = os.listdir(i)
            for j in files:
                if('checkpoint' not in j):
                    with gzip.open(i+'/'+j, 'rb') as file_in:
                        for line in file_in:
                            jsons_array.append(json.loads(line.decode().strip()))
    print("Dados puxados do amplitude e extraídos")
    for i in jsons_array:
        for j in i:
            if(type(i[j]) == type({})):
                i[j] = json.dumps(i[j])
            try:
                i[j] = str(i[j]).replace("'","")
            except:
                print(i[j])
                print(type(i[j]))
            finally:
                if(i[j] == 'None'):
                    i[j] = None
    tuples = []
    for i in range(0, len(jsons_array)):
        tuples.append((
            jsons_array[i]['app'],
            jsons_array[i]['dma'],
            jsons_array[i]['adid'],
            jsons_array[i]['city'],
            jsons_array[i]['data'],
            jsons_array[i]['idfa'],
            jsons_array[i]['uuid'],
            jsons_array[i]['groups'],
            jsons_array[i]['paying'],
            jsons_array[i]['region'],
            jsons_array[i]['country'],
            jsons_array[i]['library'],
            jsons_array[i]['os_name'],
            jsons_array[i]['user_id'],
            jsons_array[i]['event_id'],
            jsons_array[i]['language'],
            jsons_array[i]['platform'],
            jsons_array[i]['device_id'],
            jsons_array[i]['event_time'],
            jsons_array[i]['event_type'],
            jsons_array[i]['ip_address'],
            jsons_array[i]['os_version'],
            jsons_array[i]['session_id'],
            jsons_array[i]['device_type'],
            jsons_array[i]['sample_rate'],
            jsons_array[i]['amplitude_id'],
            jsons_array[i]['device_brand'],
            jsons_array[i]['device_model'],
            jsons_array[i]['location_lat'],
            jsons_array[i]['location_lng'],
            jsons_array[i]['version_name'],
            jsons_array[i]['device_family'],
            jsons_array[i]['start_version'],
            jsons_array[i]['device_carrier'],
            jsons_array[i]['user_properties'],
            jsons_array[i]['event_properties'],
            jsons_array[i]['group_properties'],
            jsons_array[i]['client_event_time'],
            jsons_array[i]['client_upload_time'],
            jsons_array[i]['server_upload_time'],
            jsons_array[i]['user_creation_time'],
            jsons_array[i]['device_manufacturer'],
            jsons_array[i]['amplitude_event_type'],
            jsons_array[i]['is_attribution_event'],
            jsons_array[i]['server_received_time'],
            jsons_array[i]['amplitude_attribution_ids']
            )
                      )
    args_str = b','.join(cur.mogrify("('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % x) for x in tuples)
    args_str = args_str.decode('utf-8').replace("'None'", 'NULL')
    args_str = args_str.replace("None", 'NULL')
    print("Inserindo os dados no banco")
    cur.execute("""
                INSERT INTO conformed.amplitude__events 
                (app,
                 dma,
                 adid,
                 city,
                 data,
                 idfa,
                 uuid,
                 groups,
                 paying,
                 region,
                 country,
                 library,
                 os_name,
                 user_id,
                 event_id,
                 language,
                 platform,
                 device_id,
                 event_time,
                 event_type,
                 ip_address,
                 os_version,
                 session_id,
                 device_type,
                 sample_rate,
                 amplitude_id,
                 device_brand,
                 device_model,
                 location_lat,
                 location_lng,
                 version_name,
                 device_family,
                 start_version,
                 device_carrier,
                 user_properties,
                 event_properties,
                 group_properties,
                 client_event_time,
                 client_upload_time,
                 server_upload_time,
                 user_creation_time,
                 device_manufacturer,
                 amplitude_event_type,
                 is_attribution_event,
                 server_received_time,
                 amplitude_attribution_ids)
                VALUES """ + args_str + " ON CONFLICT (uuid) DO NOTHING")
    con.commit()
    print("Limpando a pasta e finalizando para esse período")
    files = os.scandir('./324172')
    for file in files:
        if file.name.endswith(".json.gz"):
            os.unlink(file.path)



print("Finalizado")
