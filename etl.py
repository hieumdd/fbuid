import json
from datetime import datetime
from google.cloud import bigquery
import re
from tqdm import tqdm


def transform(result):
    result['_id'] = int(result['_id'])
    result['_source']['facebook_id'] = int(result['_source']['facebook_id'])
    if type(result.get('_source').get('hometown_location')) == dict:
        result['_source']['hometown_location']['id'] = int(
            result['_source']['hometown_location']['id'])
    else:
        result['_source']['hometown_location'] = None
    if type(result.get('_source').get('current_location')) == dict:
        result['_source']['current_location']['id'] = int(
            result['_source']['current_location']['id'])
    else:
        result['_source']['current_location'] = None
    if re.search('\d{4}-\d{2}-\d{2}', str(result['_source']['profile_updated_time'])):
        result['_source']['profile_updated_time'] = int(datetime.strptime(
            result['_source']['profile_updated_time'], '%Y-%m-%d').timestamp())
    elif re.search('\d{10}', str(result['_source']['profile_updated_time'])):
        result['_source']['profile_updated_time'] = int(
            result['_source']['profile_updated_time'])
    return result


def main():
    client = bigquery.Client()
    with open('schema.json') as sch:
        schema = json.load(sch)
    with open('fbvn100w.json', 'r', encoding='utf8') as f:
        with open('result.json', 'a') as f2:
            job_config = bigquery.LoadJobConfig(
                ignore_unknown_values=True,
                write_disposition='WRITE_APPEND',
                schema=schema
            )
            i = 0
            results = []
            for line in tqdm(f):
                result = transform(json.loads(line))
                results.append(result)
                json.dump(result, f2)
                i = i + 1
                if i % 10000 == 0:
                    print(i)
                    '''
                    errors = client.load_table_from_json(
                        destination='Facebook.fbuid',
                        json_rows=results,
                        job_config=job_config
                        # ignore_unknown_values=True
                    )
                    '''
                    print(i)
                    #print(errors)
                    results = []
                    print(results)
                if i == 10000:
                    break


if __name__ == '__main__':
    main()
