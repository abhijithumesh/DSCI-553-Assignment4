import sys
import time
import itertools

from operator import add
from graphframes import GraphFrame
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


def make_pair(list_pair):

    all_user_pairs = list(itertools.combinations(list_pair, 2))
    
    ret = []
    
    for pair in all_user_pairs:
    
        #print("Debugging:", pair)
    
        if pair[1] < pair[0]:
            ret.append((pair[1], pair[0]))
        else:
            ret.append((pair[0], pair[1]))
    
    return ret
    
def dumpResult(output_file, result_community):

    communities = result_community.collect()
    #print(communities)
    
    with open(output_file, "w") as fp:
    
        for community in communities:
        
            fp.writelines(str(community)[1:-1])
            fp.write("\n")
            

if __name__ == "__main__":

    start = time.time()

    filter_threshold = int(sys.argv[1])
    input_file = sys.argv[2]
    output_file = sys.argv[3]
    
    conf = SparkConf().setAppName("HW4task1").setMaster("local[3]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("OFF")
    
    sourceRdd = sc.textFile(input_file)
    first_line = sourceRdd.first()
    
    uid_bid = sourceRdd.filter(lambda line: line != first_line).distinct().map(lambda tup: tup.split(','))
    
    gen_pair = uid_bid.map(lambda line: (line[1], [line[0]])).reduceByKey(add).flatMap(lambda list_pair:make_pair(list_pair[1]))
    
    freq_pair = gen_pair.map(lambda line: (line, 1)).reduceByKey(add).filter(lambda a: a[1] >= filter_threshold).keys()
    
    edges = freq_pair.flatMap(lambda line: ((line[0], line[1]), (line[1], line[0])))
    
    #create DataFrame needs a tuple as input
    vertices = freq_pair.flatMap(lambda line: line).distinct().map(lambda line: (line, ))
    
    sql_ctx = SQLContext(sc)

    v_df = sql_ctx.createDataFrame(vertices, ["id"])
    e_df = sql_ctx.createDataFrame(edges, ["src", "dst"])
    
    graph_f = GraphFrame(v_df, e_df)
    
    label_prop = graph_f.labelPropagation(maxIter=5)
    
    result_community = label_prop.rdd.coalesce(1).map(lambda id_label: (id_label[1], id_label[0])).groupByKey()\
                       .map(lambda communities: sorted(list(communities[1]))).sortBy(lambda comms: (len(comms), comms))
    
    dumpResult(output_file, result_community)
    
    end = time.time()
    print("Duration:", end-start)