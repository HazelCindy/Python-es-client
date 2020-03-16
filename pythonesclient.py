from elasticsearch import Elasticsearch
import datetime

es = Elasticsearch([{
    'host': "192.168.0.17",
    'port': 9500
}])
timezone = 'EAT'


def utc_to_local(utc_str):
    """
    Convert UTC time format to local time format
    :param utc_str: UTC time string
    :return: local time string
    """
    temp = datetime.datetime.strptime(utc_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    utc_time = temp.replace(tzinfo=datetime.timezone.utc)
    local_time = utc_time.astimezone()
    return local_time.strftime('%Y-%m-%d %H:%M:%S')


def test_match_phrase_query():
    body = {
        "sort": [
            {
                "@timestamp": {
                    "order": "desc",
                    "unmapped_type": "boolean"
                }
            }
        ],
        "query": {
            "bool": {
                "must": [
                    # {"match_phrase": {"host.name"}}
                ]
            }
        }
    }

    response = es.search(index="auditbeat*", body=body, scroll="1m", size=10)
    sid = response['_scroll_id']
    print(sid)
    # es.clear_scroll(sid)
    for x in range(len(response['hits']['hits'])):
        response['hits']['hits'][x]["_source"]["timestamp"] = utc_to_local(response['hits']['hits'][x]["_source"]
                                                                           ["@timestamp"])
    # es.clear_scroll(sid)
    # self.assertEqual(response['hits']['hits'])
    return response['hits']['hits']


# print(test_match_phrase_query())


def get_packets():
    """
    Fetches  user information for specified user.
    :return: user information
    """
    user_data = []
    search_query = {
        "sort": [
            {
                "@timestamp": {
                    "order": "desc",
                    "unmapped_type": "boolean"
                }
            }
        ],
        "query": {
            # {"match_phrase": {"user.name": {"query": "network.packets"}}},
        },
        "query": {
            "range": {
                "@timestamp": {
                    "gte": "now-25d",
                    "lt": "now"
                }
            }
        }

    }

    resp = es.search(index="auditbeat*", body=search_query, scroll="1m", size=10)
    sid = resp['_scroll_id']
    # es.clear_scroll(sid)

    for x in range(len(resp['hits']['hits'])):
        data = {
            'timestamp': utc_to_local(resp['hits']['hits'][x]["_source"]["@timestamp"]),
            'packets': resp['hits']['hits'][x]["_source"]["network"]["packets"]
        }
        user_data.append(data)
    return user_data


print(get_packets())

