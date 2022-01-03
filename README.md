# MicroRank
MicroRank is a novel system to locate root causes that lead to latency issues in microservice environments. 

MicroRank extracts service latency from tracing data then conducts the anomaly detection procedure.

By combining PageRank and spectrum analysis, the service instances that lead to latency issues are ranked with high scores. 

![image](./fig/framwork.png)

## Paper Download
Our paper has been published at WWW'2021.

The paper can be downloaded as below:

[MicroRank: End-to-End Latency Issue Localization with Extended Spectrum Analysis in Microservice Environments](https://dl.acm.org/doi/10.1145/3442381.3449905)

## Reference
Please cite our paper if you find this work is helpful. 

```
@inproceedings{microrank,
  title={MicroRank: End-to-End Latency Issue Localization with Extended Spectrum Analysis in Microservice Environments},
  author={Guangba Yu, Pengfei Chen, Hongyang Chen, Zijie Guan, Zicheng Huang, Linxiao Jing, TianjunWeng, Xinmeng Sun, and Xiaoyun Li},
  booktitle={Proceedings of the Web Conference 2021 (WWW’2021)},
  year={2021},
  organization={ACM},
  page = {3087-3098},
  doi={https://doi.org/10.1145/3442381.3449905}
}
```

## Running MicroRank

### Notices
If you want to use MicroRank to production system, some notices below should be considered. 
- Our anomaly detetion module is not always suitable for each microservice system. If you have more excellent anomaly detection module for your system, we recommend that replacing the anomaly detetion module with your approach before RCA.
- Microrank needs more iterations in PageRank if your system is a large microservice system. The accuracy of RCA may decline in a large microservice system.
- We acknowledge that the accuracy of RCA may be degraded when intermittent failures and broken traces  are encountered.

### Replace Database

Line 12 in the file [preprocess_data.py](preprocess_data.py) 
```
// ES address
es_url = 'http://11.11.11.24:9200'
root_index = 'root'
```

### Replace Normal Window
Line 32 in [online_rca.py](online_rca.py).

We need to set a normal window to calculate the normal avarge latency and variance for each microservice.

Longer window is prefered.

```
# need to replace 
normal_start = '2020-08-28 14:56:43'
normal_end = '2020-08-28 14:57:44'

span_list = get_span(start=timestamp(start), end=timestamp(end))
# print(span_list)
operation_list = get_service_operation_list(span_list)
print(operation_list)
operation_slo = get_operation_slo(
    service_operation_list=operation_list, span_list=span_list)
print(operation_slo)
```

### Start MicroRank
```
python online_rca.py
```

## File content
```
- anomaly_detector
  - get_slo                                 # get the average latency and variance for each operation
  - system_anormaly_detect                  # determine whether the system is abnormal 
  - trace_anormaly_detect                   # determine whether the single trace is abnormal 
  - trace_list_partition                    # divide traces into normal and abnormal traces
- online_rca.py
  - calculate_spectrum_without_delay_list   # calculate spectrum reuslt
  - online_anomaly_detect_RCA               # running microrank
- pagerank.py                               # calculate pagerank result
- preporcess_data.py
  - get_span 
  - get_service_operation_list 
  - get_operation_slo 
  - get_operation_duration_data 
  - get_pagerank_graph 
  







