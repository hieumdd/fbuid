from google.cloud import bigquery
import os
from flask import jsonify, abort


def main(request):
    if (request.get_json() != None):
        lookup_query = build_lookup_query(request.get_json())
        client = bigquery.Client()
        results = client.query(lookup_query).to_dataframe()
        responses = {
            'num_requested': len(request.get_json().get('uids')),
            'num_matched': int(results['phone'].isna().sum()),
            'results': results.to_json(orient='records')
        }
        return jsonify(responses, 200)
    else:
        return jsonify({'errors': 'No UIDS'}), 400
    


def build_lookup_query(request_json):
    uids = request_json.get('uids')
    rows = ['SELECT ' + str(uid) + ' AS uid' for uid in uids]
    rows = ' UNION ALL \n '.join(rows)
    query = f'''WITH request AS
        (
            {rows}
        )
        SELECT
            request.*,
            fbuid._source.phone_number AS phone
        FROM request
        LEFT JOIN {os.environ.get('FBUID_TABLE_ID')} fbuid
        ON request.uid = fbuid._id
        '''
    return query
