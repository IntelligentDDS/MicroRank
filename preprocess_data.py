import re
import json
import time
import requests
import numpy as np
from copy import deepcopy
import warnings
import paramiko
from elasticsearch import Elasticsearch, helpers
warnings.filterwarnings("ignore")

es_url = 'http://11.11.11.24:9200'
root_index = 'root'
client = Elasticsearch([es_url])


'''
  Query the initial trace data from elasticsearch by scroll(1 min)
  :arg
      date: format 2020-08-14 or 2020-08-*
      start: the timestamp of start time (ms)
      end:  the timestamp of end time (ms)
  :return
      all span between start time and end time except jaeger-query service 
'''


def get_span(start=None, end=None):
    local_time = time.localtime(start/1000)
    day = time.strftime('%Y-%m', local_time)
    index_name = 'jaeger-span-' + day + '-*'
    scroll_api = es_url + "/" + index_name + "/_search?scroll=1m"
    based_api = es_url + "/_search/scroll?filter_path=hits.hits._source"
    headers = {"Content-Type": "application/json"}

    query_data = {
        "size": 10000,
        "query": {
            "bool": {
                "must_not": [
                    {
                        "terms": {
                            "process.serviceName": [
                                "jaeger-query"
                            ]}
                    }
                ],
                "filter": {
                    "range": {
                        "startTimeMillis": {
                            "lte": str(end),
                            "gte": str(start)
                        }
                    }
                }
            }
        },
        "sort": {
            "traceID": {
                "order": "asc"
            },
            "startTime": {
                "order": "asc"
            }
        }
    }
    data = requests.post(scroll_api, json=query_data, headers=headers).json()

    for i in range(10):
        if '_scroll_id' not in data:
            print("query error, restart query scroll")
            time.sleep(10)
            data = requests.post(
                scroll_api, json=query_data, headers=headers).json()
        else:
            break

    scroll_data = {
        "scroll": "1m",
        "scroll": data['_scroll_id']
    }
    span_list = []
    while 'hits' in data and len(data['hits']['hits']) > 0:
        span_list += data['hits']['hits']
        data = requests.post(based_api, json=scroll_data,
                             headers=headers).json()

    print('\nSpan Length:', len(span_list))
    return span_list


'''
  Query all the service_operation from the input span_list
  :arg
     span_list: should be a long time span_list to get all operation
  :return
       the operation list and operation list dict
'''


def get_service_operation_list(span_list):
    operation_list = []

    for doc in span_list:
        doc = doc['_source']
        operation_name = doc['operationName']
        operation_name = operation_name.split('/')[-1]

        # Currencyservice_Convert
        operation = doc['process']['serviceName'] + '_' + operation_name
        if operation not in operation_list:
            operation_list.append(operation)

    return operation_list


"""
   Calculate the mean of duration and variance for each span_list 
   :arg
       operation_list: contains all operation
       span_list: should be a long time span_list
   :return
       operation dict of the mean of and variance 
       {
           # operation: {mean, variance}
           "Currencyservice_Convert": [600, 3]}
       }   
"""


def get_operation_slo(service_operation_list, span_list):
    template = {
        'parent': '',  # parent span
        'operation': '',  # current servicename_operation
        'duration': 0  # duration of current operation
    }

    traceid = span_list[0]['_source']['traceID']
    filter_data = {}
    temp = {}
    normal_trace = True

    def check_filter_data():
        for spanid in temp:
            if temp[spanid]['parent'] == root_index:
                if temp[spanid]['duration'] > 1000000:
                    print("filter data because duration > 1000ms")
                    print(temp)
                    return False
        return True

    def server_client_determined():
        """
        :return span.kind
        tags: [{"key": "span.kind",
            "type": "string",
            "value": "server"}]
        """
        for tag in doc['tags']:
            if tag['key'] == "span.kind":
                return tag['value']

    def get_operation_name():
        operation_name = doc['operationName']
        operation_name = operation_name.split('/')[-1]
        operation_name = doc['process']['serviceName'] + '_' + operation_name
        return operation_name

    for doc in span_list:
        doc = doc['_source']
        if traceid == doc['traceID']:
            spanid = doc['spanID']
            temp[spanid] = deepcopy(template)
            temp[spanid]['duration'] = doc['duration']
            temp[spanid]['operation'] = get_operation_name()

            if server_client_determined() == 'server' and doc['process']['serviceName'] == "frontend":
                temp[spanid]['parent'] = root_index
            else:
                """
               "references" : [{"refType" : "CHILD_OF",
                "traceID" : "0000658f4e42f8674d2e36630a9ca2b8",
                "spanID" : "83438897471cc41a"}],
                """
                if len(doc['references']) == 0:
                    print(doc)
                    normal_trace = False
                else:
                    parentId = doc['references'][0]['spanID']
                    temp[spanid]['parent'] = parentId
                    if parentId in temp:
                        temp[parentId]['duration'] -= temp[spanid]['duration']
                    else:
                        normal_trace = False

        elif traceid != doc['traceID'] and len(temp) > 0:
            if check_filter_data() and normal_trace:
                filter_data[traceid] = temp

            traceid = doc['traceID']
            normal_trace = True
            spanid = doc['spanID']
            temp = {}
            temp[spanid] = deepcopy(template)
            temp[spanid]['duration'] = doc['duration']
            temp[spanid]['operation'] = get_operation_name()
            if server_client_determined() == 'server' and doc['process']['serviceName'] == "frontend":
                temp[spanid]['parent'] = root_index
            else:
                if len(doc['references']) == 0:
                    normal_trace = False
                    print(
                        "filter data because it is not frontend and its references is null ")
                    print(traceid)
                else:
                    parentId = doc['references'][0]['spanID']
                    temp[spanid]['parent'] = parentId
                if parentId in temp:
                    temp[parentId]['duration'] -= temp[spanid]['duration']
                else:
                    normal_trace = False
    # The last trace
    if len(temp) > 1:
        if check_filter_data() and normal_trace:
            filter_data[traceid] = temp

    duration_dict = {}
    """
    {'frontend_Recv.': [1961, 1934, 1316, 1415, 1546, 1670, 1357, 2099, 2789, 1832, 1270, 1242, 2230, 1386],
      'recommendationservice_ListProducts': [3576, 7127, 4387, 19657, 5158, 4563, 4167, 8822, 4507],
    """
    for operation in service_operation_list:
        duration_dict[operation] = []

    for traceid in filter_data:
        single_trace = filter_data[traceid]

        for spanid in single_trace:
            duration_dict[single_trace[spanid]['operation']].append(
                single_trace[spanid]['duration'])

    operation_slo = {}
    """
    {'frontend_Recv.': [2.903, 10.0949], 'frontend_GetSupportedCurrencies': [8.1019, 16.2973], }
    """
    for operation in service_operation_list:
        operation_slo[operation] = []

    for operation in service_operation_list:
        operation_slo[operation].append(
            round(np.mean(duration_dict[operation]) / 1000.0, 4))
        #operation_slo[operation].append(round(np.percentile(duration_dict[operation], 90) / 1000.0, 4))
        operation_slo[operation].append(
            round(np.std(duration_dict[operation]) / 1000.0, 4))

    return operation_slo


'''
   Query the operation and duration in span_list for anormaly detector 
   :arg
       operation_list: contains all operation
       operation_dict:  { "operation1": 1, "operation2":2 ... "operationn": 0, "duration": 666}
       span_list: all the span_list in one anomaly detection interval (1 min or 30s)
   :return
       { 
          traceid: {
              operation1: 1
              operation2: 2
          }
       }
'''


def get_operation_duration_data(operation_list, span_list):
    operation_dict = {}

    trace_id = span_list[0]['_source']['traceID']

    def server_client_determined():
        for tag in doc['tags']:
            if tag['key'] == "span.kind":
                return tag['value']

    def get_operation_name():
        operation_name_tmp = doc['operationName']
        operation_name_tmp = operation_name_tmp.split('/')[-1]
        operation_name_tmp = doc['process']['serviceName'] + \
            '_' + operation_name_tmp
        return operation_name_tmp

    def init_dict(trace_id):
        if trace_id not in operation_dict:
            operation_dict[trace_id] = {}
            for operation in operation_list:
                operation_dict[trace_id][operation] = 0
            operation_dict[trace_id]['duration'] = 0

    length = 0
    for doc in span_list:
        doc = doc['_source']
        tag = server_client_determined()
        operation_name = get_operation_name()

        init_dict(doc['traceID'])

        if trace_id == doc['traceID']:
            operation_dict[trace_id][operation_name] += 1
            length += 1

            if doc['process']['serviceName'] == "frontend" and tag == "server":
                operation_dict[trace_id]['duration'] += doc['duration']

        else:
            if operation_dict[trace_id]['duration'] == 0:
                if length > 45:
                    operation_dict.pop(trace_id)

                else:
                    operation_dict.pop(trace_id)

            trace_id = doc['traceID']
            length = 0
            operation_dict[trace_id][operation_name] += 1

            if doc['process']['serviceName'] == "frontend" and tag == "server":
                operation_dict[trace_id]['duration'] += doc['duration']

    return operation_dict


'''
   Query the pagerank graph
   :arg
       trace_list: anormaly_traceid_list or normaly_traceid_list
       span_list:  异常点前后两分钟 span_list
   
   :return
       operation_operation 存储子节点 Call graph
       operation_operation[operation_name] = [operation_name1 , operation_name1 ] 

       operation_trace 存储trace经过了哪些operation, 右上角 coverage graph
       operation_trace[traceid] = [operation_name1 , operation_name2]

       trace_operation 存储 operation被哪些trace 访问过, 左下角 coverage graph
       trace_operation[operation_name] = [traceid1, traceid2]  
       
       pr_trace: 存储trace id 经过了哪些operation，不去重
       pr_trace[traceid] = [operation_name1 , operation_name2]
'''


def get_pagerank_graph(trace_list, span_list):
    template = {
        'parent': '',  # parent span
        'operation': '',  # current servicename_operation
    }

    if len(trace_list) > 0:
        traceid = trace_list[0]
    else:
        traceid = span_list[0]
    filter_data = {}
    temp = {}

    def get_operation_name():
        """
        有时pod_name在 tags 中，有时在process的tags中
        "process": {"tags": [{"key": "name",
                    "type": "string",
                    "value": "frontend-7dbb469cd9-lkv68"}]}
        "tags" : [{"key" : "name",
              "type" : "string",
              "value" : "adservice-7688bd74f6-7qkvl"}]

        operation = pod_name + operation_name
        :return operation
        """
        pod_name = ""

        for tag in doc['process']['tags']:
            if tag['key'] == "name":
                pod_name = tag['value']

        for tag in doc['tags']:
            if tag['key'] == "name":
                pod_name = tag['value']

        operation = pod_name + "_" + doc['operationName']
        return operation

    operation_operation = {}
    operation_trace = {}
    trace_operation = {}
    pr_trace = {}

    for doc in span_list:
        doc = doc['_source']
        operation_name = get_operation_name()
        if doc['traceID'] in trace_list:
            if traceid == doc['traceID']:
                spanid = doc['spanID']
                temp[spanid] = deepcopy(template)
                temp[spanid]['operation'] = get_operation_name()

                if len(doc['references']) > 0:
                    parentId = doc['references'][0]['spanID']
                    temp[spanid]['parent'] = parentId

            elif traceid != doc['traceID'] and len(temp) > 0:
                filter_data[traceid] = temp

                traceid = doc['traceID']
                spanid = doc['spanID']
                temp = {}
                temp[spanid] = deepcopy(template)
                temp[spanid]['operation'] = get_operation_name()

                if len(doc['references']) > 0:
                    parentId = doc['references'][0]['spanID']
                    temp[spanid]['parent'] = parentId

            if len(temp) > 1:
                filter_data[traceid] = temp

            """
            operation_operation 
            operation_operation[operation_name] = [operation_name1 , operation_name1 ] 

            operation_trace
            operation_trace[traceid] = [operation_name1 , operation_name1]

            trace_operation
            trace_operation[operation_name] = [traceid1, traceid2]
            """
            if operation_name not in operation_operation:
                operation_operation[operation_name] = []
                trace_operation[operation_name] = []

            if doc['traceID'] not in operation_trace:
                operation_trace[doc['traceID']] = []
                pr_trace[doc['traceID']] = []

            pr_trace[doc['traceID']].append(operation_name)

            if operation_name not in operation_trace[doc['traceID']]:
                operation_trace[doc['traceID']].append(operation_name)
            if doc['traceID'] not in trace_operation[operation_name]:
                trace_operation[operation_name].append(doc['traceID'])

    for traceid in filter_data:
        single_trace = filter_data[traceid]
        if traceid in trace_list:
            for spanid in single_trace:
                parent_id = single_trace[spanid]["parent"]
                if parent_id != "":
                    if parent_id not in single_trace:
                        continue
                    if single_trace[spanid]["operation"] not in operation_operation[
                            single_trace[parent_id]["operation"]]:
                        operation_operation[single_trace[parent_id]["operation"]].append(
                            single_trace[spanid]["operation"])

    return operation_operation, operation_trace, trace_operation, pr_trace


if __name__ == '__main__':
    def timestamp(datetime):
        timeArray = time.strptime(datetime, "%Y-%m-%d %H:%M:%S")
        ts = int(time.mktime(timeArray)) * 1000
        # print(ts)
        return ts

    start = '2020-08-28 14:56:43'
    end = '2020-08-28 14:57:44'

    span_list = get_span(start=timestamp(start), end=timestamp(end))
    # print(span_list)
    operation_list = get_service_operation_list(span_list)
    print(operation_list)
    operation_slo = get_operation_slo(
        service_operation_list=operation_list, span_list=span_list)
    print(operation_slo)
