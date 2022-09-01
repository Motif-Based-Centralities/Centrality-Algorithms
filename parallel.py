import networkx as nx
import numpy as np
from re import split
from itertools import combinations, product
from collections import defaultdict
from math import pow
from time import time
from utils.decomposition import get_greedy_rich_tree_decomp
from utils.partition_tools import add_node_to_partition, format_d, get_partition, partition_repr, get_supremum, get_subset_positions, get_not_null_partitions, remove_node_from_partition
from utils.contraction_counting import contraction_subgraph_count
import ray
import os


def get_covered_nodes(formated_p):
    return '.'.join(sorted(split('[-.]', formated_p), key=lambda t: int(t)))


# PARALLEL
@ray.remote
def get_bag_base_values(u, subgraph, components, distinguished, count_trees=False):
    not_null_parts = dict()
    not_null_parts[u] = dict()
    not_null_parts[u][u] = dict()
    values = dict()
    separated_component_values = list()
    # analysis for each component in this bag's subgraph
    for comp in components:
        separated_component_values.append(dict())
        universe = sorted(comp.nodes(), key=lambda u: int(u))
        component_values = dict()
        for integer_repr in range(1, int(pow(2, len(universe)))):
            node_set = [universe[i]
                        for i in get_subset_positions(integer_repr)]
            node_set.reverse()
            if len(node_set) == 1:
                component_values[integer_repr] = 1
                continue
            if len(node_set) == 2:
                if node_set[0] in subgraph[node_set[1]]:
                    component_values[integer_repr] = 1
                    continue
            # check if there is a single component
            matrix = [[0 for _ in node_set] for _ in node_set]
            for i in range(0, len(node_set) - 1):
                for j in range(i + 1, len(node_set)):
                    if node_set[j] in subgraph[node_set[i]]:
                        matrix[i][j] = 1
                        matrix[j][i] = 1
            m = nx.convert_matrix.from_numpy_matrix(np.array(matrix))
            if not nx.is_connected(m):
                continue
            val = contraction_subgraph_count(matrix, set([i for i in range(len(node_set))]), [
                [i for i in range(len(node_set))]], count_trees=count_trees)
            component_values[integer_repr] = val
        # Build valid partitions for this component
        # e.g. if A and B are subsets of nodes with A inter B = 0, then [A,B] is a valid partition
        for p in get_not_null_partitions(list(component_values.keys())):
            part = [[universe[k]
                     for k in get_subset_positions(i)] for i in p]
            accum = 1
            for i in p:
                accum *= component_values[i]
            separated_component_values[-1][partition_repr(part)] = accum
    dist = format_d(distinguished)
    if len(components) == 1:
        for p, value in separated_component_values[-1].items():
            key = f'{u}:{dist}:{p}'
            cov = get_covered_nodes(p)
            if dist not in not_null_parts[u][u]:
                not_null_parts[u][u][dist] = dict()
            if cov not in not_null_parts[u][u][dist]:
                not_null_parts[u][u][dist][cov] = set()
            not_null_parts[u][u][dist][cov].add(p)
            values[key] = value
    else:
        for k in range(1, len(components)+1):
            for selected_indexes in combinations(
                    [i for i in range(len(components))], k):
                for prod in product(*[separated_component_values[c] for c in selected_indexes]):
                    p = []
                    value = 1
                    for i, s in enumerate(prod):
                        p.extend(get_partition(s))
                        value *= separated_component_values[selected_indexes[i]][s]
                    p = partition_repr(p)
                    key = f'{u}:{dist}:{p}'
                    cov = get_covered_nodes(p)
                    if dist not in not_null_parts[u][u]:
                        not_null_parts[u][u][dist] = dict()
                    if cov not in not_null_parts[u][u][dist]:
                        not_null_parts[u][u][dist][cov] = set(
                        )
                    not_null_parts[u][u][dist][cov].add(p)
                    values[key] = value
    return values, not_null_parts


def get_base_values(rtd, count_trees=False):
    values = dict()
    not_null_parts = dict()

    tuples = [(u, rtd.nodes()[u]['subgraph'], rtd.nodes()[
        u]['graph components'], rtd.nodes()[u]['distinguished']) for u in rtd.nodes()]

    results = [get_bag_base_values.remote(u, subgraph, components, distinguished, count_trees=count_trees)
               for u, subgraph, components, distinguished in tuples]
    for vals, parts in ray.get(results):
        values.update(vals)
        not_null_parts.update(parts)
    return values, not_null_parts


def combine_only_base_values(u, curr_d, combined_values, values, not_null_parts):
    if curr_d in not_null_parts[u][u]:
        for c in not_null_parts[u][u][curr_d]:
            for p in not_null_parts[u][u][curr_d][c]:
                combined_values[f'{curr_d}:{p}'] = values[f'{u}:{curr_d}:{p}']


def combine_children_values(
        w, u, curr_d, combined_values, values, edge_values, not_null_parts, count_trees=False):
    new_values = defaultdict(int)
    if curr_d in not_null_parts[u][u] and curr_d in not_null_parts[w][u]:
        new_not_null_parts = set()
        for c in not_null_parts[u][u][curr_d]:
            if c not in not_null_parts[w][u][curr_d]:
                continue
            for p1 in not_null_parts[u][u][curr_d][c]:
                for p2 in not_null_parts[w][u][curr_d][c]:
                    sup = get_supremum(
                        get_partition(p1), get_partition(p2), count_trees=count_trees)
                    if sup:
                        # print(p1, p2, sup)
                        supremum_p = partition_repr(sup)
                        # if sup == [[0, 1], [2]]:
                        #     print(p1, p2)
                        #     print(
                        #         f"H'[{supremum_p}] <- H'[{supremum_p}] + H1[{p1}]*H2[{p2}]")
                        #     print(
                        #         f"H'[{supremum_p}] <- {new_values[f'{u}:{curr_d}:{supremum_p}']} + {values[f'{u}:{curr_d}:{p1}']}*{edge_values[w][u][f'{curr_d}:{p2}']}")
                        new_values[f'{u}:{curr_d}:{supremum_p}'] += values[f'{u}:{curr_d}:{p1}'] * \
                            edge_values[w][u][f'{curr_d}:{p2}']
                        # if sup == [[0, 1], [2]]:
                        #     print(new_values[f'{u}:{curr_d}:{supremum_p}'])
                        new_not_null_parts.add(supremum_p)
        for k, v in new_values.items():
            p = k.split(':')[-1]
            combined_values[f'{curr_d}:{p}'] = v
            values[k] = v
        for p in new_not_null_parts:
            cov = get_covered_nodes(p)
            if cov not in not_null_parts[u][u][curr_d]:
                not_null_parts[u][u][curr_d][cov] = set()
            not_null_parts[u][u][curr_d][cov].add(p)


def update_edge_values(u, f, combined_values, edge_values):
    if u not in edge_values:
        edge_values[u] = dict()
    if f not in edge_values[u]:
        edge_values[u][f] = dict()
    for k, v in combined_values.items():
        edge_values[u][f][k] = v


def update_not_null_parts(u, f, edge_values, not_null_parts):
    if u not in not_null_parts:
        not_null_parts[u] = dict()
    if f not in not_null_parts[u]:
        not_null_parts[u][f] = dict()
    for k in edge_values[u][f]:
        s_d, s_p = k.split(':')
        cov = get_covered_nodes(s_p)
        if s_d not in not_null_parts[u][f]:
            not_null_parts[u][f][s_d] = dict()
        if cov not in not_null_parts[u][f][s_d]:
            not_null_parts[u][f][s_d][cov] = set()
        not_null_parts[u][f][s_d][cov].add(s_p)


def combine_children(u, f, rtd, values, edge_values, not_null_parts, count_trees=False):
    dist_u = rtd.nodes()[u]["distinguished"]
    children = [n for n in rtd.neighbors(u) if n != f]
    curr_d = format_d(dist_u)
    combined_values = defaultdict(int)
    if not len(children):
        # the current node is a leaf and we only need its base values
        # to compute the forget-add-result that will be saved in the edge
        combine_only_base_values(
            u, curr_d, combined_values, values, not_null_parts)
    else:
        for w in children:
            combine_children_values(
                w, u, curr_d, combined_values, values, edge_values, not_null_parts, count_trees=count_trees)
    update_edge_values(u, f, combined_values, edge_values)
    update_not_null_parts(u, f, edge_values, not_null_parts)


def forget_nodes(u, f, rtd, edge_values, not_null_parts):
    dist_u = rtd.nodes()[u]["distinguished"]
    dist_f = rtd.nodes()[f]["distinguished"]
    nodes_to_forget = dist_u - dist_f
    original_ref_set = set(dist_u)
    for n in nodes_to_forget:
        new_ref_set = original_ref_set - set([n])
        new_values = defaultdict(int)
        original_d = format_d(original_ref_set)
        new_d = format_d(new_ref_set)
        if original_d in not_null_parts[u][f]:
            for c in not_null_parts[u][f][original_d]:
                for p in not_null_parts[u][f][original_d][c]:
                    p_wo_n = remove_node_from_partition(p, n)
                    if not p_wo_n:
                        continue
                    # if n == '3':
                    #     print(
                    #         f"H'[{p_wo_n}] <- {new_values[f'{new_d}:{p_wo_n}']} + {edge_values[u][f][f'{original_d}:{p}']}")
                    cov = get_covered_nodes(p_wo_n)
                    new_values[f'{new_d}:{p_wo_n}'] += edge_values[u][f][f'{original_d}:{p}']
                    # if n == '3':
                    #     print(f"H'[{p_wo_n}] =",
                    #           new_values[f'{new_d}:{p_wo_n}'])
                    # print('forgetting and saving in', p_wo_n, 'from', p)
                    if new_d not in not_null_parts[u][f]:
                        not_null_parts[u][f][new_d] = dict()
                    if cov not in not_null_parts[u][f][new_d]:
                        not_null_parts[u][f][new_d][cov] = set()
                    not_null_parts[u][f][new_d][cov].add(p_wo_n)
        original_ref_set.discard(n)
        update_edge_values(u, f, new_values, edge_values)
    update_not_null_parts(u, f, edge_values, not_null_parts)


def add_nodes(u, f, rtd, edge_values, not_null_parts):
    dist_u = rtd.nodes()[u]["distinguished"]
    dist_f = rtd.nodes()[f]["distinguished"]
    nodes_to_forget = dist_u - dist_f
    nodes_to_add = dist_f - dist_u
    # print('forget', dist_u, dist_f, nodes_to_forget, nodes_to_add)
    original_ref_set = set(dist_u) - nodes_to_forget
    for n in nodes_to_add:
        new_ref_set = original_ref_set.union(set([n]))
        new_d = format_d(new_ref_set)
        original_d = format_d(original_ref_set)
        new_values = {f'{new_d}:{partition_repr([[n]])}': 1}
        cov = str(n)
        if new_d not in not_null_parts[u][f]:
            not_null_parts[u][f][new_d] = dict()
        if cov not in not_null_parts[u][f][new_d]:
            not_null_parts[u][f][new_d][cov] = set()
        not_null_parts[u][f][new_d][cov].add(partition_repr([[n]]))
        if original_d in not_null_parts[u][f]:
            for c in not_null_parts[u][f][original_d]:
                for p in not_null_parts[u][f][original_d][c]:
                    p_w_n = add_node_to_partition(p, n)
                    cov = get_covered_nodes(p_w_n)
                    # if n == '0' and '1' in dist_u:
                    #     print(
                    #         f"H'[{p_w_n}] <- {edge_values[u][f][f'{original_d}:{p}']} ({p})")
                    #     print(
                    #         f"H'[{p}] <- {edge_values[u][f][f'{original_d}:{p}']}")
                    new_values[f'{new_d}:{p_w_n}'] = edge_values[u][f][f'{original_d}:{p}']
                    new_values[f'{new_d}:{p}'] = edge_values[u][f][f'{original_d}:{p}']
                    if cov not in not_null_parts[u][f][new_d]:
                        not_null_parts[u][f][new_d][cov] = set()
                    if c not in not_null_parts[u][f][new_d]:
                        not_null_parts[u][f][new_d][c] = set()
                    not_null_parts[u][f][new_d][cov].add(p_w_n)
                    not_null_parts[u][f][new_d][c].add(p)
        original_ref_set.add(n)
        update_edge_values(u, f, new_values, edge_values)
    update_not_null_parts(u, f, edge_values, not_null_parts)


@ray.remote
def compute_first_traversal_bag(u, f, values, rtd, not_null_parts, edge_values, count_trees=False):
    combine_children(u, f, rtd, values, edge_values,
                     not_null_parts, count_trees=count_trees)
    forget_nodes(u, f, rtd, edge_values, not_null_parts)
    add_nodes(u, f, rtd, edge_values, not_null_parts)
    return u, edge_values, not_null_parts


def copy_null_parts(ref):
    ref_copy = {}
    for k in ref:
        ref_copy[k] = {}
        for m in ref[k]:
            ref_copy[k][m] = {}
            for n in ref[k][m]:
                ref_copy[k][m][n] = {}
                for o in ref[k][m][n]:
                    ref_copy[k][m][n][o] = set(ref[k][m][n][o])
    return ref_copy


def copy_edge_values(ref):
    ref_copy = {}
    for k in ref:
        ref_copy[k] = {}
        for m in ref[k]:
            ref_copy[k][m] = ref[k][m].copy()
    return ref_copy


def copy_edge_values_first(u, ref):
    ref_copy = {}
    for k in ref:
        if k == u:
            ref_copy[k] = {}
            for m in ref[k]:
                ref_copy[k][m] = ref[k][m].copy()
        if u in ref[k]:
            ref_copy[k] = {u: ref[k][u].copy()}
    return ref_copy


def copy_edge_values_second(u, ref):
    ref_copy = {}
    # print(ref.keys())
    u_neigh = [e for e in ref if u in ref[e]]
    to_check = list(u_neigh)
    for n in u_neigh:
        to_check.extend([e for e in ref if n in ref[e]])
    # print(to_check)
    for k in ref:
        if k in to_check:
            ref_copy[k] = {}
            for m in ref[k]:
                ref_copy[k][m] = ref[k][m].copy()
        # if u in ref[k]:
        #     ref_copy[k] = {u: ref[k][u].copy()}
    # for k in ref:
    #     if k == u:
    #         ref_copy[k] = {}
    #         for m in ref[k]:
    #             ref_copy[k][m] = ref[k][m].copy()
    #     if u in ref[k]:
    #         ref_copy[k] = {u: ref[k][u].copy()}
    #
    # for k in ref:
    #     ref_copy[k] = {}
    #     for m in ref[k]:
    #         ref_copy[k][m] = ref[k][m].copy()
    return ref_copy


def update_parts(new_vals, ref):
    for k in new_vals:
        if k not in ref:
            ref[k] = {}
        for m in new_vals[k]:
            if m not in ref[k]:
                ref[k][m] = {}
            for n in new_vals[k][m]:
                if n not in ref[k][m]:
                    ref[k][m][n] = {}
                for o in new_vals[k][m][n]:
                    if o not in ref[k][m][n]:
                        ref[k][m][n][o] = set()
                    ref[k][m][n][o] = set.union(
                        ref[k][m][n][o], new_vals[k][m][n][o])


def update_edges(new_vals, ref):
    for k in new_vals:
        if k not in ref:
            ref[k] = {}
        for m in new_vals[k]:
            if m not in ref[k]:
                ref[k][m] = {}
            ref[k][m].update(new_vals[k][m])


def get_u_values(u, values):
    res = {}
    for k in values:
        if k.startswith(u):
            res[k] = values[k]
    return res


def get_u_parts(u, parts):
    res = {}
    for k in parts:
        if k == u:
            # agregamos todo lo de este objeto, pues son
            # particiones que toman como punto de partida a u
            res[k] = {}
            for m in parts[k]:
                res[k][m] = {}
                for n in parts[k][m]:
                    res[k][m][n] = {}
                    for o in parts[k][m][n]:
                        res[k][m][n][o] = set(parts[k][m][n][o])
        if u in parts[k]:
            # k es hijo de u y tenemos que copiar solo esa rama
            res[k] = {u: {}}
            for n in parts[k][u]:
                res[k][u][n] = {}
                for o in parts[k][u][n]:
                    res[k][u][n][o] = set(parts[k][u][n][o])
    return res


def get_u_parts_final(u, parts):
    res = {}
    for k in parts:
        if k == u:
            # agregamos todo lo de este objeto, pues son
            # particiones que toman como punto de partida a u
            res[k] = {}
            for m in parts[k]:
                res[k][m] = {}
                for n in parts[k][m]:
                    res[k][m][n] = {}
                    for o in parts[k][m][n]:
                        res[k][m][n][o] = set(parts[k][m][n][o])
        if u in parts[k]:
            # k es hijo de u y tenemos que copiar solo esa rama
            res[k] = {u: {}}
            for n in parts[k][u]:
                res[k][u][n] = {}
                for o in parts[k][u][n]:
                    res[k][u][n][o] = set(parts[k][u][n][o])
    return res


def first_tree_traversal(rtd, values, not_null_parts, v_assignment, count_trees=False):
    _n = time()
    edge_values = dict()
    neighbors = {u: set(rtd.neighbors(u)) for u in rtd}
    in_progress = {u for u in neighbors if len(neighbors[u]) == 1}
    i = 1
    # ids = [compute_first_traversal_bag.remote(
    #     u, list(neighbors[u])[0], get_u_values(u, values), rtd, copy_null_parts(not_null_parts), dict(edge_values)) for u in neighbors if len(neighbors[u]) == 1]
    ids = [compute_first_traversal_bag.remote(
        u, list(neighbors[u])[0], get_u_values(u, values), rtd, get_u_parts(u, not_null_parts), dict(), count_trees=count_trees) for u in neighbors if len(neighbors[u]) == 1]
    for _ in range(len(rtd.nodes())):
        ready, not_ready = ray.wait(ids)
        ready_vals = ray.get(ready)
        for u, vals, parts in ready_vals:
            # print('\tfirst', i, u)
            i += 1
            in_progress.remove(u)
            update_edges(vals, edge_values)
            update_parts(parts, not_null_parts)
            # print('edge values', u, json.dumps(
            #     edge_values, indent=2, sort_keys=True))
            n_u = neighbors[u]
            for v in n_u:
                neighbors[v].remove(u)
                if len(neighbors[v]) == 0:
                    root = v
                if len(neighbors[v]) == 1 and all(w not in in_progress for w in neighbors[v]):
                    in_progress.add(v)
                    not_ready.append(compute_first_traversal_bag.remote(
                        v, list(neighbors[v])[0], get_u_values(v, values), rtd, get_u_parts(v, not_null_parts), copy_edge_values_first(v, edge_values), count_trees=count_trees))
            ids = not_ready
            if not ids:
                break
    centralities = dict()
    # print('inner first', time() - _n)
    if len(v_assignment[root]):
        # id = compute_bag_centrality.remote(
        #     root, rtd, values, edge_values, not_null_parts, v_assignment[root])
        id = compute_bag_centrality.remote(
            root, rtd, get_u_values(root, values), copy_edge_values_first(root, edge_values), get_u_parts(root, not_null_parts), v_assignment[root])
        res = ray.get(id)
        centralities.update(res[1])
        # print('\t\tbolsa 1', res[1])
    return edge_values, root, centralities


def combine_neighbors(u, vs, rtd, values, edge_values, not_null_parts, clean_parts, combined_values, count_trees=False):
    children = [n for n in rtd.neighbors(u)]
    dist_u = rtd.nodes()[u]["distinguished"]
    curr_d = format_d(dist_u)
    if not len(children):
        if curr_d in not_null_parts[u][u]:
            for c in not_null_parts[u][u][curr_d]:
                for p in not_null_parts[u][u][curr_d][c]:
                    pm = [a.split('.') for a in p.split('-')]
                    if len(pm) == 1:
                        ipm = [int(i) for i in pm[0]]
                        if any((int(v) in ipm for v in vs)):
                            combined_values[f'{curr_d}:{p}'] = values[f'{u}:{curr_d}:{p}']
                            if curr_d not in clean_parts:
                                clean_parts[curr_d] = dict()
                            cov = get_covered_nodes(p)
                            if cov not in clean_parts[curr_d]:
                                clean_parts[curr_d][cov] = set()
                            clean_parts[curr_d][cov].add(p)
    else:
        vals = dict(values)
        clean_to_add = set()
        for i, w in enumerate(children):
            not_nulls_to_add = set()
            aux = defaultdict(int)
            # print('inicio   u=', u, 'w=', w)
            # print('\tllaves con u=', u, not_null_parts[u].keys(
            # ), ' w=', w, not_null_parts[w].keys())
            case_a = curr_d in not_null_parts[u][u]
            case_b = curr_d in not_null_parts[w][u]
            # print('no murio u=', u, 'w=', w)
            if case_a and case_b:
                for c in not_null_parts[u][u][curr_d]:
                    if c not in not_null_parts[w][u][curr_d]:
                        continue
                    for p1 in not_null_parts[u][u][curr_d][c]:
                        for p2 in not_null_parts[w][u][curr_d][c]:
                            sup = get_supremum(
                                get_partition(p1), get_partition(p2), count_trees=count_trees)
                            if sup:
                                if i == len(children)-1:
                                    if len(sup) != 1:
                                        continue
                                    if all((int(v) not in sup[0] for v in vs)):
                                        continue
                                supremum_p = partition_repr(sup)
                                # print('combining', p1, p2, supremum_p)
                                aux[f'{u}:{curr_d}:{supremum_p}'] += vals[f'{u}:{curr_d}:{p1}'] * \
                                    edge_values[w][u][f'{curr_d}:{p2}']
                                not_nulls_to_add.add((curr_d, supremum_p))
                                if i == len(children) - 1:
                                    clean_to_add.add((curr_d, supremum_p))
            for k, v in aux.items():
                vals[k] = v
            for d, p in not_nulls_to_add:
                cov = get_covered_nodes(p)
                if cov not in not_null_parts[u][u][curr_d]:
                    not_null_parts[u][u][curr_d][cov] = set()
                not_null_parts[u][u][curr_d][cov].add(p)
        for d, p in clean_to_add:
            if d not in clean_parts:
                clean_parts[d] = dict()
            cov = get_covered_nodes(p)
            if cov not in clean_parts[d]:
                clean_parts[d][cov] = set()
            clean_parts[d][cov].add(p)
        for k, v in vals.items():
            _, curr_d, p = k.split(':')
            combined_values[f'{curr_d}:{p}'] = v


@ray.remote
def compute_bag_centrality(u, rtd, values, edge_values, not_null_parts, objective_vs):
    clean_parts = dict()
    centralities = dict()
    combined_values = defaultdict(int)
    combine_neighbors(u, objective_vs, rtd, values, edge_values,
                      not_null_parts, clean_parts, combined_values)
    for v in objective_vs:
        partial_sum = 0
        ref_set = rtd.nodes()[u]['distinguished']
        curr_d = format_d(ref_set)
        if curr_d in clean_parts:
            for c in clean_parts[curr_d]:
                for p in clean_parts[curr_d][c]:
                    pm = [a.split('.') for a in p.split('-')]
                    if len(pm) == 1 and v in pm[0]:
                        partial_sum += combined_values[f'{curr_d}:{p}']
        centralities[v] = partial_sum
    return 'BAG', centralities


@ray.remote
def compute_second_traversal_bag(u, f, edge_values, vals, rtd, count_trees=False):
    nn_parts = dict()
    for k in vals:
        n, d, p = k.split(':')
        cov = get_covered_nodes(p)
        if n not in nn_parts:
            nn_parts[n] = dict()
        if n not in nn_parts[n]:
            nn_parts[n][n] = dict()
        if d not in nn_parts[n][n]:
            nn_parts[n][n][d] = dict()
        if cov not in nn_parts[n][n][d]:
            nn_parts[n][n][d][cov] = set()
        nn_parts[n][n][d][cov].add(p)
    for k, v in edge_values.items():
        for m, n in v.items():
            for x in n:
                s_d, s_p = x.split(':')
                cov = get_covered_nodes(s_p)
                if k not in nn_parts:
                    nn_parts[k] = dict()
                if m not in nn_parts[k]:
                    nn_parts[k][m] = dict()
                if s_d not in nn_parts[k][m]:
                    nn_parts[k][m][s_d] = dict()
                if cov not in nn_parts[k][m][s_d]:
                    nn_parts[k][m][s_d][cov] = set()
                nn_parts[k][m][s_d][cov].add(s_p)
    # print('nn_parts', json.dumps(nn_parts,
    #       indent=2, sort_keys=True, cls=SetEncoder))

    combine_children(u, f, rtd, vals, edge_values,
                     nn_parts, count_trees=count_trees)
    forget_nodes(u, f, rtd, edge_values, nn_parts)
    add_nodes(u, f, rtd, edge_values, nn_parts)
    return 'EDGE', u, f, edge_values, nn_parts


def second_tree_traversal(rtd, edge_values, values, root, centralities, v_assignment, count_trees=False):
    nodes = set()
    for bag in v_assignment:
        nodes.update(v_assignment[bag])
    neighbors = {
        u: set(list(filter(lambda t: t != root, rtd.neighbors(u)))) for u in rtd}

    # print('edge values OUTSIDE', json.dumps(edge_values,
    #       indent=2, sort_keys=True, cls=SetEncoder))
    # # print('values OUTSIDE', json.dumps(values, indent=2, sort_keys=True))
    # print('edge values IN', json.dumps(copy_edge_values_first(root, edge_values),
    #       indent=2, sort_keys=True, cls=SetEncoder))
    # print('values IN', json.dumps(get_u_values(
    #     root, values), indent=2, sort_keys=True))
    # print('root', root, [f for f in neighbors[root]])
    ids = [compute_second_traversal_bag.remote(
        root, f, copy_edge_values_second(root, edge_values), values, rtd, count_trees=count_trees) for f in neighbors[root]]
    # ids = [compute_second_traversal_bag.remote(
    #     root, f, copy_edge_values(edge_values), dict(values), rtd) for f in neighbors[root]]
    i = 2
    j = 1
    for _ in range(2*len(rtd.nodes())):
        ready, not_ready = ray.wait(ids)
        for r_id in ready:
            res = ray.get(r_id)
            if res[0] == 'BAG':
                centralities.update(res[1])
                # print('\t\tbolsa', i, res[1])
                i += 1
            else:
                u, f, vals, not_null_parts = res[1:]
                # print('RES', json.dumps(vals, indent=2, sort_keys=True))

                # print('\tsecond', j, u, f)
                update_edges(vals, edge_values)
                if len(v_assignment[f]):

                    # print('edge values OUTSIDE', json.dumps(edge_values,
                    #       indent=2, sort_keys=True, cls=SetEncoder))
                    # print('not null parts OUTSIDE', json.dumps(not_null_parts,
                    #       indent=2, sort_keys=True, cls=SetEncoder))
                    # # # print('edge values IN', json.dumps(copy_edge_values_first(root, edge_values),
                    # # #       indent=2, sort_keys=True, cls=SetEncoder))
                    # print('not null parts IN f=', f, json.dumps(get_u_parts_final(f, not_null_parts),
                    #       indent=2, sort_keys=True, cls=SetEncoder))

                    not_ready.append(compute_bag_centrality.remote(f, rtd, values, copy_edge_values_first(
                        f, edge_values), get_u_parts(f, not_null_parts), v_assignment[f]))
                    # not_ready.append(compute_bag_centrality.remote(f, rtd, dict(
                    #     values), copy_edge_values(edge_values), not_null_parts, v_assignment[f]))
                n_f = neighbors[f]
                for v in n_f:
                    neighbors[v].remove(f)
                    not_ready.append(compute_second_traversal_bag.remote(
                        f, v, copy_edge_values_second(f, edge_values), values, rtd, count_trees=count_trees))
                    # not_ready.append(compute_second_traversal_bag.remote(
                    #     f, v, copy_edge_values(edge_values), dict(values), rtd))
                j += 1
        ids = not_ready
        if len(centralities) == len(nodes):
            break


def get_vertex_assignment(rtd):
    bags = {}
    edges = {}
    nodes = set()
    assignment = {}
    for u in rtd.nodes():
        bags[u] = set(u.split('-')[0].split('.'))
        if len(u.split('-')) > 1:
            edges[u] = u.split('-')[1:]
        else:
            edges[u] = []
        nodes.update(bags[u])
        assignment[u] = set()
    for n in nodes:
        covering_bags = sorted(
            [u for u in bags if n in bags[u]], key=lambda t: (len(bags[t]), len(edges[t])))
        assignment[covering_bags[0]].add(n)
    return assignment


def all_subgraphs_centrality_parallel(graph, name, nodelist, count_trees=False, cores=4):
    ray.init(num_cpus=cores, ignore_reinit_error=True)
    # print('#nodes', len(graph.nodes()))

    rtd = get_greedy_rich_tree_decomp(graph, name, nodelist, given_td=None)
    # print(rtd.edges)
    print('TD #nodes', len(rtd.nodes()), 'TD #edges', len(rtd.edges()))
    # print(rtd.edges)

    # print('TreeDecomp:', r)

    # se paraleliza por bolsa el cálculo de valores iniciales
    # y se combinan para obtener values y not_null_parts
    values, not_null_parts = get_base_values(rtd, count_trees=count_trees)

    # First traversal
    first_values = dict(values)
    v_assignment = get_vertex_assignment(rtd)
    edge_values, root, centralities = first_tree_traversal(
        rtd, first_values, not_null_parts, v_assignment, count_trees=count_trees)

    # Second traversal
    second_values = dict(values)
    second_tree_traversal(
        rtd, edge_values, second_values, root, centralities, v_assignment, count_trees=count_trees)
    return centralities


if __name__ == '__main__':
    # g = nx.complete_graph(7)
    pass
