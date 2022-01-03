import numpy as np

a = np.array([[0, 1, 1 / 2, 0, 1 / 4, 1 / 2, 0],
              [1 / 5, 0, 1 / 2, 1 / 3, 0, 0, 0],
              [1 / 5, 0, 0, 1 / 3, 1 / 4, 0, 0],
              [1 / 5, 0, 0, 0, 1 / 4, 0, 0],
              [1 / 5, 0, 0, 1 / 3, 0, 1 / 2, 1],
              [0, 0, 0, 0, 1 / 4, 0, 0],
              [1 / 5, 0, 0, 0, 0, 0, 0]], dtype=float)

pr = np.array([[1 / 2],
               [1 / 3],
               [1 / 4],
               [0],
               [0],
               [0],
               [0]], dtype=float)


# print Pagerank matrix
def show_matrix(matrix, pr):
    print()
    print('Metrix:')
    n = len(pr)
    for i in range(n):
        for j in range(n):
            print(matrix[i][j], ' ', end=' ')
        print()

    print()
    print('Pr:')
    for i in range(n):
        print(pr[i][0], ' ', end=' ')
    print('\nSize:', len(pr))


def normalization(a):
    sumCol = np.sum(a, axis=0)

    for i in range(a.shape[0]):
        if sumCol[i] == 0:
            print('col: %d, sum: %.5f' % (i, sumCol[i]))
            continue
        for j in range(a.shape[1]):
            a[j][i] = a[j][i] / sumCol[i]
    return a


def firstPr(c):
    pr = np.zeros((c.shape[0], 1), dtype=float)

    # sum = np.sum(c, axis=0)[0]
    # print(sum)
    for i in range(c.shape[0]):
        pr[i] = c[i][0] / c.shape[0]
    # print pr,"\n==================================================="
    return pr


'''
   Calculate pagerank weight of anormaly_list or normal_list
   :arg 
   :return
       operation weight:
       weight[operation][0]: operation
       weight[operation][1]: weight
'''


def trace_pagerank(operation_operation, operation_trace, trace_operation, pr_trace, anomaly):
    operation_length = len(operation_operation)
    trace_length = len(operation_trace)

    p_ss = np.zeros((operation_length, operation_length), dtype=np.float32)
    p_sr = np.zeros((operation_length, trace_length), dtype=np.float32)
    p_rs = np.zeros((trace_length, operation_length), dtype=np.float32)

    # matrix = np.zeros((n, n), dtype=np.float32)
    pr = np.zeros((trace_length, 1), dtype=np.float32)

    node_list = []
    for key in operation_operation.keys():
        node_list.append(key)

    trace_list = []
    for key in operation_trace.keys():
        trace_list.append(key)

    # matrix node*node
    for operation in operation_operation:
        child_num = len(operation_operation[operation])

        for child in operation_operation[operation]:
            p_ss[node_list.index(child)][node_list.index(
                operation)] = 1.0 / child_num

    # matrix node*request
    for trace_id in operation_trace:
        child_num = len(operation_trace[trace_id])
        for child in operation_trace[trace_id]:
            p_sr[node_list.index(child)][trace_list.index(trace_id)] \
                = 1.0 / child_num

    # matrix request*node
    for operation in trace_operation:
        child_num = len(trace_operation[operation])

        for child in trace_operation[operation]:
            p_rs[trace_list.index(child)][node_list.index(operation)] \
                = 1.0 / child_num

    kind_list = np.zeros(len(trace_list))
    p_srt = p_sr.T
    for i in range(len(trace_list)):
        index_list = [i]
        if kind_list[i] != 0:
            continue
        n = 0
        for j in range(i, len(trace_list)):
            if (p_srt[i] == p_srt[j]).all():
                index_list.append(j)
                n += 1
        for index in index_list:
            kind_list[index] = n

    num_sum_trace = 0
    kind_sum_trace = 0
    if not anomaly:
        for trace_id in pr_trace:
            num_sum_trace += 1.0 / kind_list[trace_list.index(trace_id)]
        for trace_id in pr_trace:
            pr[trace_list.index(trace_id)] = 1.0 / \
                kind_list[trace_list.index(trace_id)] / num_sum_trace
    else:
        for trace_id in pr_trace:
            kind_sum_trace += 1.0 / kind_list[trace_list.index(trace_id)]
            num_sum_trace += 1.0 / len(pr_trace[trace_id])
        for trace_id in pr_trace:
            pr[trace_list.index(trace_id)] = 1.0 / (kind_list[trace_list.index(trace_id)] / kind_sum_trace * 0.5
                                                    + 1.0 / len(pr_trace[trace_id])) / num_sum_trace * 0.5

    if anomaly:
        print('\nAnomaly_PageRank:')
    else:
        print('\nNormal_PageRank:')
    result = pageRank(p_ss, p_sr, p_rs, pr, operation_length, trace_length)

    weight = {}
    sum = 0
    for operation in operation_operation:
        sum += result[node_list.index(operation)][0]

    trace_num_list = {}
    for operation in operation_operation:
        trace_num_list[operation] = 0
        i = node_list.index(operation)
        for j in range(len(trace_list)):
            if p_sr[i][j] != 0:
                trace_num_list[operation] += 1

    for operation in operation_operation:
        weight[operation] = result[node_list.index(
            operation)][0] * sum / len(operation_operation)

    # for score in sorted(weight.items(), key=lambda x: x[1], reverse=True):
    #     print('%-50s: %.5f' % (score[0], score[1]))

    return weight, trace_num_list


# calculate pageRank vaule
def pageRank(p_ss, p_sr, p_rs, v, operation_length, trace_length, d=0.85, alpha=0.01):
    iteration = 25
    service_ranking_vector = np.ones(
        (operation_length, 1)) / float(operation_length + trace_length)
    request_ranking_vector = np.ones(
        (trace_length, 1)) / float(operation_length + trace_length)

    for i in range(iteration):
        updated_service_ranking_vector = d * \
            (np.dot(p_sr, request_ranking_vector) +
             alpha * np.dot(p_ss, service_ranking_vector))
        updated_request_ranking_vector = d * \
            np.dot(p_rs, service_ranking_vector) + (1.0 - d) * v
        service_ranking_vector = updated_service_ranking_vector / \
            np.amax(updated_service_ranking_vector)
        request_ranking_vector = updated_request_ranking_vector / \
            np.amax(updated_request_ranking_vector)

    normalized_service_ranking_vector = service_ranking_vector / \
        np.amax(service_ranking_vector)
    return normalized_service_ranking_vector


if __name__ == "__main__":
    p_ss = np.zeros((3, 4), dtype=np.float32)
    print(p_ss)
    p_ss[0][1] = 1
    p_ss[1][0] = 2
    print(p_ss)
    print(p_ss.T)
    print(p_ss.T[1])
    print((p_ss.T[1] == p_ss.T[3]).all())
    print((p_ss.T[2] == p_ss.T[3]).all())
    # ap_ss = np.array([[0, 0, 0, 0],
    #          [1 / 3, 0, 0, 0],
    #          [1 / 3, 0, 0, 0],
    #          [1 / 3, 1, 1, 0]], dtype=float)
    #
    # ap_sr = np.array([[1 / 2, 1 / 3, 1 / 3],
    #          [0, 0, 1 / 3],
    #          [0, 1 / 3, 0],
    #          [1 / 2, 1 / 3, 1 / 3]], dtype=float)
    # print(ap_ss)
    # print(ap_ss[0])

    # ap_rs = np.array([[1 / 3, 0, 0, 1 / 3],
    #          [1 / 3, 0, 1, 1 / 3],
    #          [1 / 3, 1, 0, 1 / 3]], dtype=float)
    #
    # a_v = np.array([[1], [1 / 3], [1 / 3]], dtype=float)
    #
    # p_ss = np.array([[0, 0, 0],
    #         [1, 0, 0],
    #         [0, 1, 0]], dtype=float)
    #
    # p_sr = np.array([[1 / 3], [1 / 3], [1 / 3]], dtype=float)
    #
    # p_rs = np.array([1, 1, 1], dtype=float)
    #
    # v = np.array([1 / 3], dtype=float)
    #
    # anomaly_result = pageRank(ap_ss, ap_sr, ap_rs, a_v, 4, 3)
    # print(anomaly_result)
    #
    # normal_result = pageRank(p_ss, p_sr, p_rs, v, 3, 1)
    # print(normal_result)
    #
    # spectrum = {}
    # anomaly_list_len = 3
    # normal_list_len = 1
    #
    # for node in range(4):
    #     spectrum[node] = {}
    #     spectrum[node]['ef'] = anomaly_result[node] * anomaly_list_len
    #     spectrum[node]['nf'] = anomaly_list_len - anomaly_result[node] * anomaly_list_len
    #     if node in normal_result:
    #         spectrum[node]['ep'] = normal_result[node] * normal_list_len
    #         spectrum[node]['np'] = normal_list_len - normal_result[node] * normal_list_len
    #     else:
    #         spectrum[node]['ep'] = 0.0000001
    #         spectrum[node]['np'] = 0.0000001
    #
    # for node in range(3):
    #     if node not in spectrum:
    #         spectrum[node] = {}
    #         spectrum[node]['ep'] = normal_result[node] * normal_list_len
    #         spectrum[node]['np'] = normal_list_len - normal_result[node] * normal_list_len
    #         if node in anomaly_result:
    #             spectrum[node]['ef'] = anomaly_result[node] * anomaly_list_len
    #             spectrum[node]['nf'] = anomaly_list_len - anomaly_result[node] * anomaly_list_len
    #         else:
    #             spectrum[node]['ef'] = 0.0000001
    #             spectrum[node]['nf'] = 0.0000001
    #
    # # print('\n Micro Rank Spectrum raw:')
    # # print(json.dumps(spectrum))
    # result = {}
    #
    # for node in spectrum:
    #     # Dstar2
    #     result[node] = spectrum[node]['ef'] * spectrum[node]['ef'] / (spectrum[node]['ef'] + spectrum[node]['nf'])
    #
    # print(result)
