from networkx.algorithms.approximation.treewidth import treewidth_min_degree
from networkx.generators.trees import random_tree
import networkx as nx


def get_greedy_rich_tree_decomp(G, name, nodelist, given_td=False):
    if given_td:
        edge_map = dict()
        td = nx.Graph()
        with open('results/' + name + '-td.txt', 'r') as file:
            _, _, nbags, twp, _ = file.readline().strip().split(' ')
            tw = int(twp) - 1
            print('treewidth', int(twp) - 1)
            for i in range(int(nbags)):
                # bolsa i
                res = file.readline().strip().split(' ')
                nodes = res[2:]
                edge_map[i + 1] = frozenset({nodelist[int(j)]
                                            for j in nodes})
            for l in file.readlines():
                a, b = l.strip().split(' ')
                td.add_edge(edge_map[int(a)], edge_map[int(b)])
    else:
        # print(len(td.nodes()), len(td.edges()))
        # for e in td.edges():
        #     print(e)
        tw, td = treewidth_min_degree(G)
        # print('\ttreewidth', tw)
    # print(len(td.nodes()), len(td.edges()))
    # for e in td.edges():
    #     print(e)
    # raise Exception()
    # if tw > 7:
    #     raise Exception('Too big treewidth')
    edges = [('.'.join(map(str, sorted([i for i in e[0]]))), '.'.join(map(str, sorted([i
                                                                                       for i in e[1]])))) for e in td.edges]
    if len(edges):
        rtd = nx.Graph(edges)
    else:
        rtd = nx.Graph()
        rtd.add_node('.'.join([str(n) for n in G.nodes()]))
    for n in rtd.nodes():
        components = [set([u]) for u in n.split('.')]
        rtd.nodes()[n]['components'] = components
        rtd.nodes()[n]['distinguished'] = set(n.split('.'))
        rtd.nodes()[n]['subgraph'] = nx.Graph()
        rtd.nodes()[n]['subgraph'].add_nodes_from(
            rtd.nodes()[n]['distinguished'])
    node_list = [n for n in rtd.nodes()]
    for edge in G.edges():
        e = sorted(list(edge))
        node_list = sorted(node_list, key=lambda n: len(
            rtd.nodes()[n]['components']), reverse=True)
        for n in node_list:
            if str(e[0]) in rtd.nodes()[n]['distinguished'] and str(e[1]) in rtd.nodes()[n]['distinguished']:
                comp1 = next(filter(lambda comp: str(
                    e[0]) in comp, rtd.nodes()[n]['components']), None)
                comp2 = next(filter(lambda comp: str(
                    e[1]) in comp, rtd.nodes()[n]['components']), None)
                if comp1 and comp2 and comp1 != comp2:
                    comp1 = rtd.nodes()[n]['components'].index(comp1)
                    comp2 = rtd.nodes()[n]['components'].index(comp2)
                    rtd.nodes()[n]['components'][comp1] = rtd.nodes()[n]['components'][comp1].union(
                        rtd.nodes()[n]['components'][comp2])
                    rtd.nodes()[n]['components'].pop(comp2)
                rtd.nodes()[n]['subgraph'].add_edges_from(
                    [(str(e[0]), str(e[1]))])
                break
            # if n == node_list[-1]:
            #     print('No entró', e)
    for n in rtd.nodes():
        # each component starts as an empty graph to which we add nodes and edges
        rtd.nodes()[n]['graph components'] = [nx.Graph()
                                              for _ in rtd.nodes()[n]['components']]
        for c, comp in enumerate(rtd.nodes()[n]['components']):
            rtd.nodes()[n]['graph components'][c].add_nodes_from(comp)
        for edge in rtd.nodes()[n]['subgraph'].edges():
            e = list(sorted(map(str, (int(i) for i in edge))))
            for c, comp in enumerate(rtd.nodes()[n]['components']):
                if e[0] in comp:
                    rtd.nodes()[n]['graph components'][c].add_edge(*e)
        if len(rtd.nodes()[n]['subgraph'].edges()):
            edges = sorted([list(sorted(int(i) for i in pair))
                            for pair in rtd.nodes()[n]['subgraph'].edges()])
            edges = [[str(i) for i in e] for e in edges]
            edges_string = '-'.join(('.'.join(pair)
                                     for pair in edges))
            rtd = nx.relabel_nodes(rtd, {n: n + '-' + edges_string})
    # for e in rtd.edges:
    #     print(e[0], '\n\t', e[1])
    # raise Exception()
    return rtd


if __name__ == '__main__':
    # g = nx.complete_graph(9)
    # g = random_tree(25, 1)
    g = random_tree(11, 0)
    # g = nx.Graph()
    # g.add_edges_from([(0, 1), (1, 2), (2, 0), (3, 1), (4, 3), (4, 1)])
    # g.add_edges_from([(0, 1), (1, 2), (2, 0), (1, 3), (3, 0), (1, 4), (4, 0)])
    # g = nx.les_miserables_graph()  # TW=9
    # g = nx.karate_club_graph()  # TW=5
    # g = nx.florentine_families_graph() # TW=3

    # t = get_greedy_rich_tree_decomp(g)
    # for e in t.edges():
    #     print(e[0], '->', e[1])
    # t = get_binary_greedy_rich_tree_decomp(g)
