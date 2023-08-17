from arango import ArangoClient


miserablesDatabase = "w-541c3500f9944ce395537e09a61b8b97"
originalGraphName = 'miserables'
reducedGraphName = 'reduced_'+originalGraphName
originalNodeCollectionName = 'characters'
originalEdgeCollectionName = 'relationships'
reducedNodeCollectionName = 'reducedNodes'
reducedEdgeCollectionName = 'reducedEdges'

'''
eurovisDatabase = 'w-f3bcbd142a0b405687cd82a068f26c39'
originalGraphName = 'eurovis'
reducedGraphName = 'reduced_'+originalGraphName
originalNodeCollectionName = 'people'
originalEdgeCollectionName = 'connections'
reducedNodeCollectionName = 'reducedNodes'
reducedEdgeCollectionName = 'reducedEdges'
'''


def CreateSimplifiedGraph(databaseName,originalGraphName,originalEdgeCollectionName,originalNodeCollectionName, reducedGraphName,reducedEdgeCollectionName,reducedNodeCollectionName,nameField="name"):
 
    # Initialize the client for ArangoDB.
    client = ArangoClient(hosts="http://localhost:8529")
    # Connect to "miserables" database as root user.
    db = client.db(databaseName, username="root", password="letmein")

    # create the output collections if they don't exist.  We will make a new regular
    # collection for the nodes and a new edge collection for the new edges.  The new
    # edges will connect between nodes of the new output node collection

    if db.has_graph(reducedGraphName):
        newGraph = db.graph(reducedGraphName)
    else:
        newGraph = db.create_graph(reducedGraphName)

    if not db.has_collection(reducedNodeCollectionName):
        newNodeCol = db.create_collection(reducedNodeCollectionName)
    if not newGraph.has_edge_definition(reducedEdgeCollectionName):
        newEdgeCol = newGraph.create_edge_definition(
            edge_collection= reducedEdgeCollectionName,
            from_vertex_collections=[reducedNodeCollectionName],
            to_vertex_collections=[reducedNodeCollectionName]
        )

    # Execute an AQL query and iterate through the result cursor to find how many nodes are in the graph
    cursor = db.aql.execute("FOR doc IN "+ originalNodeCollectionName + " RETURN doc")
    node_names = [document[nameField] for document in cursor]
    print('found ',len(node_names), originalNodeCollectionName+' in the '+ originalGraphName +' graph')

    # now we do the actual query we are looking for.  This returns a list of the nodes 
    # which touch a node that has a different community type than it is.  This is the subset
    # of all the nodes in the graph, which we want to have in our simplified graph

    query_str = 'RETURN UNIQUE(\
                    FOR n in '+originalNodeCollectionName+' \
                        FOR k \
                            in 1..1 \
                            any n \
                            graph ' + originalGraphName + ' \
                            filter n._community_SLPA != k._community_SLPA \
                            return k) \
        '
    bind_vars = {}
    cursor = db.aql.execute(query=query_str, bind_vars=bind_vars)
    boundary_node_return = [doc for doc in cursor]
    boundary_node_list = boundary_node_return[0]
    #print(' *** node list returned')
    #print(node_name_list[0])
    print('sample node:',boundary_node_list[0])
    print('found ',len(boundary_node_list),'boundary nodes')


    # make an index of the edges by ID and by name for use later
    nodeIds = []
    nodeDict = {}
    nodeDictByName = {}

    for node in boundary_node_list:
        nodeIds.append(node['_id'])
        nodeDict[node['_id']] = node
        nodeDictByName[node[nameField]] = node

    # now that we have the nodes, lets loop through the edges and find the 
    # edges that are connecting these nodes to another boundary node which has a different
    # community.  All edges between same community nodes will be filtered out. This technique
    # will reduce the graph but will leave some nodes floating without an edge. 

    bind_vars = {"nodeIds": nodeIds,"nodeDict": nodeDict}
    query_str = 'FOR e in '+ originalEdgeCollectionName + ' \
                        Filter (e._from IN @nodeIds) \
                            Filter (e._to IN @nodeIds) \
                            Filter @nodeDict[e._from]._community_SLPA != @nodeDict[e._to]._community_SLPA \
                            RETURN e\
                    '

    cursor = db.aql.execute(query=query_str, bind_vars=bind_vars)
    interior_edges = [doc for doc in cursor]
    print('count of interior edges:',len(interior_edges))
    print('sample interior edge',interior_edges[0])

    # now that we have the edges that are connecting disparate community nodes, let us
    # just keep only the nodes that have an incident edge.  A node might be in this list
    # multiple times

    nodesUsed = []
    for n in nodeIds:
        for e in interior_edges:
            #print('edge:',e)
            if e['_from'] == n or e['_to'] == n:
                nodesUsed.append(n)

    # name the node list unique by storing in a set to remove duplicates
    print('length of nodesUsed (could contain dups):',len(nodesUsed))
    nodesUsedSet = set(nodesUsed)
    print('length of nodesUsedSet',len(nodesUsedSet))

    #  when this loop is finished, the nodeDictNoId dictionary will be indexed
    # by the _key field of the node, not the Id field.  This way, we can rebuild
    # the IDs in a consistent way using the new name of the output node collection.

    nodeDictByKey = {}
    for nodeKey in nodesUsedSet:
        nodeName = nodeDict[nodeKey]['_key']
        nodeDictByKey[nodeName] = {}
        for attrib in nodeDict[nodeKey]:
            # skip the id so these records can be inserted into the new collection
            if attrib != '_id':
                nodeDictByKey[nodeName][attrib] = nodeDict[nodeKey][attrib]


    # loop through the node dictionary and insert these nodes into a new output collection
    # This is done in two steps because we are preserving the _key values from the original collection.
    # The node is Inserted with a specific key in the first query.  Then the second query updates the other
    # node attribute values, whatever attribute is present.  (no uniform schema is proceeded.
    
    uniqueNodes = [n for n in nodeDictByKey.keys()]

    bind_vars = {"nodesUsed": uniqueNodes,"nodeDict": nodeDictByKey}
    query_str = 'FOR n in @nodesUsed \
                    INSERT { \
                        _key: @nodeDict[n]._key, \
                        } \
                    INTO '+reducedNodeCollectionName
    print('entering nodes into new collection')
    cursor = db.aql.execute(query=query_str,bind_vars=bind_vars)

    # now add any other attributes, regardless of the attribute name, to the nodes
    bind_vars = {"nodesUsed": uniqueNodes,"nodeDict": nodeDictByKey}
    query_str = 'FOR n in @nodesUsed \
                    UPDATE @nodeDict[n] \
                    INTO '+reducedNodeCollectionName
    print('entering nodes into new collection')
    cursor = db.aql.execute(query=query_str,bind_vars=bind_vars)

    # create an output edge collection of the reduced edges only. However, there is 
    # a problem: the reducedNodes have different IDs than the original nodes 
    # but the edges reference the original nodes.  Our NodeDictByKey about has the 
    # same _key values as the original nodes, so lets just change the _from and _to 
    # references from the original collection name to the new collection name

    fixed_interior_edges = []
    for edge in interior_edges:
        fixed_edge = {}
        for attrib in edge:
            if attrib =='_from':
                # find the key from the old node and add the new collection name
                oldNodeKey = edge[attrib].split('/')[1]
                fixed_edge[attrib] = reducedNodeCollectionName+'/'+oldNodeKey
            elif attrib =='_to':
                # find the key from the old node and add the new collection name
                oldNodeKey = edge[attrib].split('/')[1]
                fixed_edge[attrib] = reducedNodeCollectionName+'/'+oldNodeKey
            # ignore the Id and key of the edge since these will be assigned during storage
            elif attrib not in ['_id','_key']:
                fixed_edge[attrib] = edge[attrib]
        fixed_interior_edges.append(fixed_edge)

    # now do the query to add these edges to the new edge collection
    bind_vars = {"edgesUsed": fixed_interior_edges}
    query_str = 'FOR e in @edgesUsed \
                    INSERT e \
                    INTO '+ reducedEdgeCollectionName
    print('writing edges to new collection')
    cursor = db.aql.execute(query=query_str, bind_vars=bind_vars)
    

CreateSimplifiedGraph(miserablesDatabase,originalGraphName,originalEdgeCollectionName,
                      originalNodeCollectionName, reducedGraphName, reducedEdgeCollectionName,reducedNodeCollectionName
                      )

#CreateSimplifiedGraph(eurovisDatabase,originalGraphName,originalEdgeCollectionName,
#                      originalNodeCollectionName, reducedGraphName, reducedEdgeCollectionName,reducedNodeCollectionName,
#                      nameField='screen_name')