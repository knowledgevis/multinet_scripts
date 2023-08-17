import networkx as nx
import matplotlib 



# develop accessor function to return node index from _key. Be tolerant of
# table names preceding the unique node name
def nodeKeyToNumber(g,namestring):
    if '/' in namestring:
        # chop off the table name
        namestring = namestring.split('/')[1]
    names = nx.get_node_attributes(g,'_key')
    for name in names:
        # chop of the table name
        nodename = names[name]
        if '/' in nodename:
            nodename = nodename.split('/')[1]
        if str(nodename) == str(namestring):
            return name

def buildGraph_netX(node_list,edge_list):
    # create empty directed graph.  All ArangoDB graphs are directed by default
    g = nx.DiGraph()
    
    # insert nodes
    node_index = 1
    for node in node_list:
        for attrib in node:
            g.add_node(node_index)
            if attrib not in ['gt_object','used_by_edge','index']:
                g.nodes[node_index][attrib] = node[attrib]
        node_index+=1
        
    # insert edges
    for edge in edge_list:
        sourceNode = nodeKeyToNumber(g,edge['_from'])
        destNode = nodeKeyToNumber(g,edge['_to'])
        g.add_edge(sourceNode,destNode)
        for attrib in edge:
            if attrib not in ['_from','_to']:
                g[sourceNode][destNode][attrib] = edge[attrib]
    # return the nx graph object
    return g

# return a subset of a graph by filtering by node in or out degree
def subsetGraph_netX(g,algorithm,threshold):
    def in_node_filter(index):
        if algorithm == 'in-degree':
            return (g.in_degree(index+1)>threshold)
    def out_node_filter(index):
            return (g.out_degree(index+1)>threshold)
    if algorithm == 'in-degree':
        view = nx.subgraph_view(g, filter_node=in_node_filter)
    else:
        view = nx.subgraph_view(g, filter_node=out_node_filter)
    #print('reduced graph has ',view.number_of_nodes(),'nodes')
    #print('reduced graph has ',nx.number_of_edges(view),'edges')
    return view

nodes_df = pd.read_csv('/home/clisle/Dropbox/ipython-notebook/multinet/gpe_kg_subset_240_nodes.csv')
edges_df = pd.read_csv('/home/clisle/Dropbox/ipython-notebook/multinet/gpe_kg_subset_829_edges.csv')
edge_list = edges_df.to_dict('records')
node_list = nodes_df.to_dict('records')
g = buildGraph_netX(node_list,edge_list)
smallg = subsetGraph_netX(g,'in-degree',0)

print('reduced graph has ',smallg.number_of_nodes(),'nodes')
print('reduced graph has ',nx.number_of_edges(smallg),'edges')
