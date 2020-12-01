import copy
import itertools
import sys
import time

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from collections import defaultdict


def make_pairs(line):

    res = line.split(',')
    return res
    
def individual_vert(partition):

    for vert_partition in partition:
        for vertex in vert_partition:
            yield vertex
            
            
def bfs(start_node, adj_list):

    parentInfo = dict()
    depthInfo = dict()
    visited = []
    
    q = []
    q .append((start_node, None, 0))
    parentInfo[start_node] = list()
    depthInfo[start_node] = 0
    
    while len(q) > 0:
        node, parent, depth = q.pop(0)
        visited.append(node)
        
        for child in adj_list[node]:
        
            if child in depthInfo:
                if depthInfo[child] == depth+1:
                    parentInfo[child].append(node)
            else:
                parentInfo[child] = [node]
                depthInfo[child] = depth+1
                q.append((child, node, depth+1))
    
    
    return depthInfo, parentInfo, visited


def compute_betweeness(vertices, adj_list):

    #print("completed compute_betweeness")
    for top_node in vertices:
        depthInfo, parentInfo, visited = bfs(top_node, adj_list)
        
        bhara_wt = defaultdict(int)
        bhara_wt[top_node] = 1
        
        node_crt = defaultdict(lambda: 1)
        
        for vtx in visited:
            for p_vtx in parentInfo[vtx]:
                bhara_wt[vtx] += bhara_wt[p_vtx]
                
                
        for vtx in visited[::-1]:
            if vtx in parentInfo:
                par_list = parentInfo[vtx]
                
                for each_parent in par_list:
                
                    n1 = vtx
                    n2 = each_parent
                    
                    if n1 > n2:
                        n1, n2 = n2, n1

                    val = (bhara_wt[each_parent]/bhara_wt[vtx]) * node_crt[vtx]
                    node_crt[each_parent] += val
                    
                    yield ((n1, n2), val)
            
            
def calculate_betweeness(v_rdd, adj_list):

    btw_calc = v_rdd.mapPartitions(lambda vertices: compute_betweeness(vertices, adj_list)).reduceByKey(lambda a, b: a+b)
    collect_output = sorted(btw_calc.collect(), key = lambda op: (-op[1], op[0][0]))
    
    return collect_output
    
    
def write_between_output(between_res, between_file1):

    with open(between_file1, "w") as fp:
 
        for line in between_res:
            fp.write("('" + line[0][0] + "', '" + line[0][1] + "'), " + str(line[1]/2))
            fp.write("\n")
            
def swap_pairs(pair):

    return (pair[1], [pair[0]])


def dfs(vtx, mut_a_mtx, beeti):

    stk = []
    stk.append(vtx)
    reachable_edges = []
    
    while(len(stk) > 0):

        vertex = stk.pop()
        
        if vertex not in beeti:
            beeti.add(vertex)
            reachable_edges.append(vertex)
            stk += mut_a_mtx[vertex]
        
    return reachable_edges


def search_intact_edges(mut_a_mtx):

    all_vertices = mut_a_mtx.keys()
    different_comp = []
    beeti = set()
    
    for vtx in all_vertices:
        if vtx not in beeti:
            different_comp.append(dfs(vtx, mut_a_mtx, beeti))
        
    return different_comp

def calculate_modularity(individual_comp, orig_adj_list, m):

    Q = float(0)

    for a in individual_comp:
        k_ath = len(orig_adj_list[a])
        for b in individual_comp:
            k_bth = len(orig_adj_list[b])
            
            A_ab = 0
            if b in orig_adj_list[a]:
                A_ab = 1
                
            Q = Q + (A_ab - ((k_ath * k_bth) / m))
            
    Q = Q/(2*m)
    
    return Q
    
def calculate_modularity_case2(individual_comp):

    Q = float(0)

    for a in individual_comp:
        k_ath = len(orig_adj_list.value[a])
        for b in individual_comp:
            k_bth = len(orig_adj_list.value[b])
            
            A_ab = 0
            if b in orig_adj_list.value[a]:
                A_ab = 1
                
            Q = Q + (A_ab - ((k_ath * k_bth)/(2 * m.value)))
            
    Q = Q/(2*m.value)
    
    return Q


def detect_community(mut_a_mtx, v_rdd, orig_adj_list, m, case):

    total_possible = [v_rdd.collect()]
    modularity_Q = -1

    # initally there is 1 huge cluster. We have to do divisive hierarchical clustering
    len_total_possible = 1
    
    while len_total_possible < len(mut_a_mtx.keys()):
    
        btw_calc = v_rdd.mapPartitions(lambda vertices: compute_betweeness(vertices, mut_a_mtx)).reduceByKey(lambda a, b: a+b)
        removable_edges = btw_calc.map(swap_pairs).reduceByKey(lambda a,b: a+b).sortByKey(ascending=False).top(1)
        
        #print("Removable_edges:", len(removable_edges))
        
        if len(removable_edges) == 0:
            break
            
        for edge in removable_edges[0][1]:
            mut_a_mtx[edge[0]].remove(edge[1])
            mut_a_mtx[edge[1]].remove(edge[0])
            
        new_comm = search_intact_edges(mut_a_mtx)
        if case == 1:
            Q_val_list = sc.parallelize(new_comm).map(lambda line: calculate_modularity(line, orig_adj_list, m)).collect()
        else:
            Q_val_list = sc.parallelize(new_comm).map(calculate_modularity_case2).collect()

        total_Q_val = sum(Q_val_list)
        
        if total_Q_val > modularity_Q:
            modularity_Q = total_Q_val
            total_possible = new_comm
            len_total_possible = len(total_possible)
            
    return total_possible

def dump_to_file(commts, community_file2):

    fp = open(community_file2, "w")
    
    for each_c in commts:
        res = ""
        for idx, val in enumerate(each_c):
        
            if idx != len(each_c) - 1:
                res += "'" + val + "', " 
            else:
                res += "'" + val + "'"
        res += "\n"
        
        fp.write(res)
    
    fp.close()
    
def jaccard_similarity(pair, user_state_dict):

    user1 = pair[0]
    user2 = pair[1]
    
    sim = len(user_state_dict[user1].intersection(user_state_dict[user2]))/len(user_state_dict[user1].union(user_state_dict[user2]))
    
    return (pair,sim)
    
def gen_pair_uids(a_list):
    
    #print("a_list is", a_list)
    return tuple(itertools.combinations(a_list[1], 2))
    
def construct_adj_mtx(uid_pairs):

    first_edge = uid_pairs.map(lambda uid: (uid[0], set((uid[1],)))).reduceByKey(lambda a,b: a|b)
    
    second_edge = uid_pairs.map(lambda uid: (uid[1], set((uid[0],)))).reduceByKey(lambda a,b: a|b)
    
    alist = first_edge.union(second_edge).reduceByKey(lambda a,b: a|b)
    
    return alist.collectAsMap()


if __name__ == '__main__':

    
    start = time.time()

    case_no = int(sys.argv[1])
    input_file = sys.argv[2]
    between_file1 = sys.argv[3]
    community_file2 = sys.argv[4]
    
    
    conf = SparkConf().setAppName("hw4task2")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    
    input_data = sc.textFile(input_file)
    first_line = input_data.first()
    input_data = input_data.filter(lambda line: line != first_line)
    
    # User-State girvan newmann
    if case_no == 1:
    
        #uid = input_data.map(lambda line: (line.split(',')[0])).distinct().collect()
        #states = input_data.map(lambda line: (line.split(',')[1])).distinct().collect()
        
        vert_rdd = input_data.map(make_pairs)
        vertices = vert_rdd.mapPartitions(individual_vert).distinct()

        #print("vertices:" , vertices.collect())
        
        uid_state_edges = input_data.map(lambda line: (line.split(',')[0], line.split(',')[1]))\
                        .groupByKey().mapValues(lambda states: sorted(list(states))).collectAsMap()
        
        state_uid_edges = input_data.map(lambda line: (line.split(',')[1], line.split(',')[0]))\
                        .groupByKey().mapValues(lambda uids: sorted(list(uids))).collectAsMap()
                        
        #print("USERID::STATES::", uid_state_edges)
        #print("STATE::USERID::", state_uid_edges)
        
        # Combine the two maps as the edges
        edges = {**uid_state_edges, **state_uid_edges}
        #vertex_set = frozenset(states) | frozenset(uid)
        
        #print("EDGES:", edges)
        
        between_res = calculate_betweeness(v_rdd = vertices, adj_list = edges)
        
        write_between_output(between_res, between_file1)
        
        orig_adj_list = copy.deepcopy(edges)
        m = vert_rdd.count()
        
        lis_comms = detect_community(mut_a_mtx = edges, v_rdd = vertices, orig_adj_list = orig_adj_list, m = m, case = 1)
        
        for each_c in lis_comms:
            each_c.sort()
            
        lis_comms = sorted(lis_comms, key = lambda x:(len(x), x[0]))
        
        dump_to_file(lis_comms, community_file2)
        
        
    elif case_no == 2:
    
        uid_states_dict = input_data.map(lambda line: (line.split(',')[0], line.split(',')[1])) \
                          .groupByKey().mapValues(lambda states: set(states)).collectAsMap()

        uid_only = input_data.map(lambda line: (1,line.split(',')[0])).distinct().groupByKey().map(gen_pair_uids).flatMap(lambda x: sorted(x)).distinct()
        
        uid_jaccard = uid_only.map(lambda line: jaccard_similarity(line, uid_states_dict)).filter(lambda x: x[1] >= 0.5).map(lambda x: x[0]).distinct()

        vertex_rdd = uid_jaccard.mapPartitions(individual_vert).distinct()

        adj_matrix = construct_adj_mtx(uid_jaccard)
        
        between_res = calculate_betweeness(v_rdd = vertex_rdd, adj_list = adj_matrix)
        
        write_between_output(between_res, between_file1)
        
        orig_adj_list = sc.broadcast(adj_matrix)
        m = sc.broadcast(uid_jaccard.count())
        
        lis_comms = detect_community(mut_a_mtx = adj_matrix, v_rdd = vertex_rdd, orig_adj_list = orig_adj_list, m = m, case = 2)
        
        for each_c in lis_comms:
            each_c.sort()
            
        lis_comms = sorted(lis_comms, key = lambda x:(len(x), x[0]))        
            
        dump_to_file(lis_comms, community_file2)
        
    end = time.time()
    
    print("Duration:", (end-start))
    
    