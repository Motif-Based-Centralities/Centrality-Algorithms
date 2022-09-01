from parallel import all_subgraphs_centrality_parallel
from non_parallel import all_subgraphs_centrality
import getopt
import sys
import os
import networkx as nx
import numpy as np


def get_graph_from_np(a):
    a.tolist()
    nodes = set()
    edges = set()
    for e in a:
        nodes.add(e[0])
        nodes.add(e[1])
        if e[0] != e[1] and (e[1], e[0]) not in edges:
            edges.add(tuple(e))
    g = nx.Graph()
    g.add_nodes_from(nodes)
    g.add_edges_from(edges)
    return g


def run_mode(ipath, opath, cores, parallel, trees):
    print(ipath, opath, cores, parallel, trees)
    for path in os.listdir(ipath):
        print(path)
        if '.edges' in path or '.mtx' in path:
            complete_path = f'{ipath}/{path}'
            if path.split('.')[-1] == "edges":
                try:
                    a = np.loadtxt(complete_path, dtype=int, skiprows=0,
                                   usecols=(0, 1), comments="%")
                except:
                    a = np.loadtxt(complete_path, dtype=int, skiprows=0, delimiter=',',
                                   usecols=(0, 1), comments="%")
            else:
                a = np.loadtxt(complete_path, dtype=int, skiprows=2,
                               usecols=(0, 1), comments="%")
            g_c = get_graph_from_np(a)
            biggest = sorted(list(nx.connected_components(g_c)), key=len)[-1]
            g = nx.Graph()
            g.add_nodes_from(biggest)
            edges = filter(
                lambda e: e[0] in biggest and e[1] in biggest, g_c.edges)
            g.add_edges_from(edges)

            nodelist = {n: i for i, n in enumerate(g.nodes())}
            g = nx.relabel_nodes(g, nodelist)
            trees_label = ''
            if trees:
                trees_label = 'trees'
            if parallel:
                values = all_subgraphs_centrality_parallel(
                    g, path, nodelist, count_trees=trees, cores=cores)
                name = path.split('.')[0]
                with open(f'{opath}/{name}{trees_label}.txt', 'w') as outfile:
                    for e in values.items():
                        outfile.write(f'{e[0]},{e[1]}\n')
            else:
                values = all_subgraphs_centrality(g)
                name = path.split('.')[0]
                with open(f'{opath}/{name}{trees_label}.txt', 'w') as outfile:
                    for e in values.items():
                        outfile.write(f'{e[0]},{e[1]}\n')


def execute(argv):
    inputpath = None
    outputpath = None
    cores = None
    parallel = False
    trees = False
    try:
        opts, _ = getopt.getopt(
            argv, "hi:o:c:pt", ["ipath=", "opath=", "cores=", "parallel", "trees"])
    except getopt.GetoptError:
        print("main.py -i <inputpath> -o <outputfile> -c <cores> -p -t ")
        sys.exit()
    for opt, arg in opts:
        if opt in ("-i", "--ipath"):
            inputpath = arg
        elif opt in ("-o", "--opath"):
            outputpath = arg
        elif opt in ("-c", "--cores"):
            cores = int(arg)
        elif opt in ("-p", "--parallel"):
            parallel = True
        elif opt in ("-t", "--trees"):
            trees = True
    if not (inputpath and outputpath and cores):
        print('FORMAT ERROR')
        print("main.py -i <inputpath> -o <outputfile> -c <cores> -p -t")
        sys.exit()

    run_mode(inputpath, outputpath, cores, parallel, trees)


if __name__ == '__main__':
    execute(sys.argv[1:])
