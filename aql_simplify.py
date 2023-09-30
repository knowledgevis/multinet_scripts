from arango import ArangoClient
from arango.http import DefaultHTTPClient
import time
import arrow
import pandas as pd


'''
databaseName = kg_db = 'w-3c7dda4d9d2b4f5087c40ca6292aacef'
originalGraphName = 'kg_n737_e5000'
#this_method = 'LabelProp'
this_method = 'SpeakerListener'
#this_method = 'Attribute'
selected_attribute = '_degree'
threshold = 1
if this_method == 'Attribute':
    reducedGraphName = 'reduced_'+originalGraphName+'_'+selected_attribute+'_'+str(threshold)
else:
    reducedGraphName = 'reduced_'+originalGraphName+'_'+this_method
originalNodeCollectionName = 'kg_n737_e5000__kg_nodes_737_cleaned'
originalEdgeCollectionName = 'kg_n737_e5000__kg_edges_5000'
reducedNodeCollectionName = originalNodeCollectionName+'_reduced_'+this_method
reducedEdgeCollectionName = originalEdgeCollectionName + '_reduced_'+this_method


databaseName = kg_db = 'w-70e8ac78da6041349894f13349faa7a1'
originalGraphName = 'merged_full'
#this_method = 'LabelProp'
#this_method = 'SpeakerListener'
this_method = 'Attribute'
selected_attribute = 'category'
threshold = None
reducedGraphName = 'reduced_'+originalGraphName+'_'+selected_attribute
originalNodeCollectionName = 'merged_kg_nodes'
originalEdgeCollectionName = 'merged_kg_edges'
reducedNodeCollectionName = originalNodeCollectionName+'_reduced_'+this_method
reducedEdgeCollectionName = originalEdgeCollectionName + '_reduced_'+this_method



databaseName = kg_db = "w-541c3500f9944ce395537e09a61b8b97"
originalGraphName = 'miserables'
this_method = 'SpeakerListener'
#this_method = 'LabelProp'
this_method = 'Attribute'
selected_attribute = '_degree'
threshold = 10
reducedGraphName = 'reduced_'+originalGraphName+'_'+this_method
originalNodeCollectionName = 'characters'
originalEdgeCollectionName = 'relationships'
reducedNodeCollectionName = 'reducedNodes_'+this_method
reducedEdgeCollectionName = 'reducedEdges_'+this_method
databaseName = kg_db = "w-541c3500f9944ce395537e09a61b8b97"
originalGraphName = 'miserables'
this_method = 'SpeakerListener'
#this_method = 'LabelProp'
#this_method = 'Attribute'
selected_attribute = '_degree'
reducedGraphName = 'reduced_'+originalGraphName+'_'+this_method
originalNodeCollectionName = 'characters'
originalEdgeCollectionName = 'relationships'
reducedNodeCollectionName = 'reducedNodes_'+this_method
reducedEdgeCollectionName = 'reducedEdges_'+this_method

databaseName = kg_db = 'w-70e8ac78da6041349894f13349faa7a1'
originalGraphName = 'tiny'
#this_method = 'LabelProp'
this_method = 'Attribute'
selected_attribute = 'category'
reducedGraphName = 'reduced_'+originalGraphName+'_'+this_method
originalNodeCollectionName = 'kg_25_nodes'
originalEdgeCollectionName = 'kg_20_edges'
reducedNodeCollectionName = originalNodeCollectionName+'_reduced_'+this_method
reducedEdgeCollectionName = originalEdgeCollectionName + '_reduced_'+this_method


databaseName = kg_db = 'w-70e8ac78da6041349894f13349faa7a1'
originalGraphName = 'merged_full'
this_method = 'LabelProp'
#method = 'Attribute'
selected_attribute = 'category'
reducedGraphName = 'reduced_'+originalGraphName+'_'+this_method
originalNodeCollectionName = 'merged_kg_nodes'
originalEdgeCollectionName = 'merged_kg_edges'
reducedNodeCollectionName = originalNodeCollectionName+'_reduced_'+this_method
reducedEdgeCollectionName = originalEdgeCollectionName + '_reduced_'+this_method


databaseName = eurovisDatabase = 'w-f3bcbd142a0b405687cd82a068f26c39'
originalGraphName = 'eurovis'
reducedGraphName = 'reduced_'+originalGraphName
originalNodeCollectionName = 'people'
originalEdgeCollectionName = 'connections'
reducedNodeCollectionName = 'reducedNodes'
reducedEdgeCollectionName = 'reducedEdges'


'''

def AddGraphAnalyticAttributeToNodes(method,attributeName,arango_db,graphName,nodeCollection=None):
    if method.lower() =='labelprop':
        print("running label propogation on:",graphName)
        label_prop_job_id = arango_db.pregel.create_job(
            graph=graphName,
            algorithm='labelpropagation',
            store=True,
            async_mode=False,
            result_field=attributeName
            )
        while ((arango_db.pregel.job(label_prop_job_id)['state'] == 'running') or
            (arango_db.pregel.job(label_prop_job_id)['state'] == 'storing')):
            time.sleep(0.25)
            print('waiting for community label propogation job to finish')

    elif method.lower() == 'speakerlistener':
        print("running speaker-Listener label propogation on:",graphName)
        slpa_job_id = arango_db.pregel.create_job(
            graph=graphName,
            algorithm='slpa',
            store=True,
            async_mode=False,
            result_field=attributeName
        )
        while ((arango_db.pregel.job(slpa_job_id)['state'] == 'running') or 
                (arango_db.pregel.job(slpa_job_id)['state'] == 'storing')):
            time.sleep(0.25)
            print('waiting for SLPA community job to finish')

    elif method.lower() == 'pagerank':
        print("running speaker-Listener label propogation on:",graphName)
        prank_job_id = arango_db.pregel.create_job(
            graph=graphName,
            algorithm='pagerank',
            store=True,
            async_mode=False,
            result_field=attributeName
        )
        while ((arango_db.pregel.job(prank_job_id)['state'] == 'running') or 
                (arango_db.pregel.job(prank_job_id)['state'] == 'storing')):
            time.sleep(0.25)
            print('waiting for pagerank job to finish')

    elif method.lower() == 'betweenness':
        print("running speaker-Listener label propogation on:",graphName)
        btweeen_job_id = arango_db.pregel.create_job(
            graph=graphName,
            algorithm='linerank',
            store=True,
            async_mode=False,
            result_field=attributeName
        )
        while ((arango_db.pregel.job(btweeen_job_id)['state'] == 'running') or 
                (arango_db.pregel.job(btweeen_job_id)['state'] == 'storing')):
            time.sleep(0.25)
            print('waiting for betweenness job to finish')

    elif method.lower() == 'degree':
        print('calculating degree for nodes in graph',graphName)
        query_str = """FOR doc in @@COLL
                UPDATE {"_key": doc._key,
                @outputAttribute : LENGTH(for edge 
                    in 1 ANY doc._id 
                    graph @graphName
                    return edge._id
                    )
                } in @@COLL
            RETURN doc._id """
        # set the node collection and graph name dynamically
        bind_vars = {'@COLL': nodeCollection, 'graphName': graphName,'outputAttribute':attributeName}
        cursor = arango_db.aql.execute(query=query_str, bind_vars=bind_vars)
        print('node degree calculations are complete')


# run background jobs to add graph metrics to all nodes in a graph. 
def AddAllGraphMetrics(arangodb,graphName,nodeColl=None):
    AddGraphAnalyticAttributeToNodes('betweenness','_betweenness',arangodb,graphName)
    AddGraphAnalyticAttributeToNodes('labelprop','_community_LP',arangodb,graphName)
    AddGraphAnalyticAttributeToNodes('SpeakerListener','_community_SLPA',arangodb,graphName)
    AddGraphAnalyticAttributeToNodes('pageRank','_pagerank',arangodb,graphName)
    AddGraphAnalyticAttributeToNodes('degree','_degree',arangodb,graphName,nodeColl)

# create a derived network that represents the existing network without any floating edges or unconnected nodes
def network_remove_floating_nodes_and_edges(databaseName,originalGraphName,originalEdgeCollectionName,originalNodeCollectionName, 
                                            reducedGraphName,reducedEdgeCollectionName,reducedNodeCollectionName):
      # Initialize the client for ArangoDB.
    client = ArangoClient(
        hosts="http://localhost:8529")
    # Connect to "miserables" database as root user.
    db = client.db(databaseName, username="root", password="letmein")

      # create the output collections if they don't exist.  We will make a new regular
    # collection for the nodes and a new edge collection for the new edges.  The new
    # edges will connect between nodes of the new output node collection

    if db.has_collection(reducedNodeCollectionName):
        db.delete_collection(reducedNodeCollectionName)
    newNodeCol = db.create_collection(reducedNodeCollectionName)

    if db.has_graph(reducedGraphName):
        existingGraph = db.graph(reducedGraphName)
        if existingGraph.has_edge_definition(reducedEdgeCollectionName):
            existingGraph.delete_edge_definition(reducedEdgeCollectionName,purge=True)
        db.delete_graph(reducedGraphName)
    newGraph = db.create_graph(reducedGraphName)

    if not newGraph.has_edge_definition(reducedEdgeCollectionName):
        newEdgeCol = newGraph.create_edge_definition(
            edge_collection= reducedEdgeCollectionName,
            from_vertex_collections=[reducedNodeCollectionName],
            to_vertex_collections=[reducedNodeCollectionName]
        )

    print('gathering all the edges')
    all_edges = []
    bind_vars = {"@EdgeCollName": originalEdgeCollectionName}
    query_str = 'FOR e in @@EdgeCollName RETURN e'
    cursor = db.aql.execute(query=query_str, bind_vars=bind_vars,batch_size=10000)
    all_edges = [doc for doc in cursor]
    if cursor.has_more():
       while cursor.has_more():
        while not cursor.empty():
            edges = cursor.next()
            for edge in cursor:
                all_edges.append(edge)
        print('edge count is:',len(all_edges))
        cursor.fetch()
    # get the last partial batch
    while not cursor.empty():
        edges = cursor.next()
        for edge in cursor:
            all_edges.append(edge)
    print('total edge count is:',len(all_edges))

    # find all the nodes that are referenced from these edges. This may be a superset 
    # of the nodes in the network.  We will intersect this nodesUsedSet with the nodes in the node collection later.
    nodesUsedSet = set()
    for e in all_edges:
        nodesUsedSet.add(e['_from'])
        nodesUsedSet.add(e['_to'])
    print('length of nodesUsedSet',len(nodesUsedSet))

    # retreive node keys for all nodes in the node collection
    query_str = 'FOR doc in @@nodeCollection RETURN doc._key'              
    bind_vars = {'@nodeCollection': originalNodeCollectionName}
    cursor = db.aql.execute(query=query_str, bind_vars=bind_vars)
    collection_node_set = set()
    for doc in cursor:
        collection_node_set.add(doc)
    print('found ',len(collection_node_set),'nodes declared in the node collection')

    # find the truly used nodes in the graph by making sure a node is used in an end and
    # also in the node collection.  We do this by making sure the node was also used by an
    # edge.  This way we end up with a strict subset of the included nodes, which were used
    # by edges. 
    truly_used_nodeset = set()
    for node in collection_node_set:
        if node in nodesUsedSet:
            truly_used_nodeset.add(node)

    # now that we have the truly used nodes, lets thin the edges by including only edges between
    # truly used nodes
    print('calculating interior edges from full edge set')
    nodeIdsSet = set(nodeIds)
    truly_used_edges = []
    for e in all_edges:
        if (e['_from'] in truly_used_nodeset) and (e['_to'] in truly_used_nodeset):
            truly_used_edges.append(e)
    print('count of interior edges:',len(truly_used_edges))
    if len(truly_used_edges)>0:
        print('sample interior edge',truly_used_edges[0])




    

def CreateSimplifiedGraph(databaseName,originalGraphName,originalEdgeCollectionName,originalNodeCollectionName, reducedGraphName,reducedEdgeCollectionName,reducedNodeCollectionName,
                          nameField="name",method='SpeakerListener',selected_attribute = None,batchsize=1000,
                          selectByThreshold=False,threshold=0.5,createOutputCollections=False):

    # Initialize the client for ArangoDB.
    client = ArangoClient(
        hosts="http://localhost:8529")
    # Connect to "miserables" database as root user.
    db = client.db(databaseName, username="root", password="letmein")

    starttime = arrow.now()

    '''  (Assume metrics are already in place and simplify this code)
    # depending on he method used to reduce the graph, we need to check if the nodes
    # already have the proper attributes.  We pull one node and look at its attributes.
    cursor = db.aql.execute("FOR doc IN "+ originalNodeCollectionName + " LIMIT 1 RETURN doc")
    sample_node = [document for document in cursor]
    print('extracted ',len(sample_node),' sample from', originalNodeCollectionName+' in the '+ originalGraphName +' graph')

    print('selected method')
    # now add attributes if they are needed and are not already present in the source graph
    if method.lower() == "speakerlistener" and '_community_SLPA' not in sample_node[0]:
        AddGraphAnalyticAttributeToNodes('SpeakerListener','_community_SLPA',db,originalGraphName)
    elif method.lower() == "labelprop" and '_community_LP' not in sample_node[0]:
        AddGraphAnalyticAttributeToNodes('LabelProp','_community_LP',db,originalGraphName)
    '''

    # create the output collections if they don't exist.  We will make a new regular
    # collection for the nodes and a new edge collection for the new edges.  The new
    # edges will connect between nodes of the new output node collection

    if db.has_collection(reducedNodeCollectionName):
        db.delete_collection(reducedNodeCollectionName)
    newNodeCol = db.create_collection(reducedNodeCollectionName)

    if db.has_graph(reducedGraphName):
        existingGraph = db.graph(reducedGraphName)
        if existingGraph.has_edge_definition(reducedEdgeCollectionName):
            existingGraph.delete_edge_definition(reducedEdgeCollectionName,purge=True)
        db.delete_graph(reducedGraphName)
    newGraph = db.create_graph(reducedGraphName)

    if not newGraph.has_edge_definition(reducedEdgeCollectionName):
        newEdgeCol = newGraph.create_edge_definition(
            edge_collection= reducedEdgeCollectionName,
            from_vertex_collections=[reducedNodeCollectionName],
            to_vertex_collections=[reducedNodeCollectionName]
        )


    # Execute an AQL query and iterate through the result cursor to find how many nodes are in the graph
    cursor = db.aql.execute("FOR doc IN "+ originalNodeCollectionName + " RETURN doc."+nameField)
    node_names = [document for document in cursor]
    print('found ',len(node_names), originalNodeCollectionName+' in the '+ originalGraphName +' graph')

    # now we do the actual query we are looking for.  This returns a list of the nodes 
    # which touch a node that has a different community type than it is.  This is the subset
    # of all the nodes in the graph, which we want to have in our simplified graph

    if method.lower() == 'speakerlistener':
        method_attribute = '_community_SLPA'
    elif method.lower() == "labelprop":
        method_attribute = '_community_LP'
    elif (method.lower() == 'attribute') and selected_attribute != None:
        method_attribute = selected_attribute
    else:
        print('incorrect settings discovered. Please review the method and appropriate parameters')

    '''
    # this query takes too long for large graphs, so break it up to process nodes in batches
    query_str = 'RETURN UNIQUE(\
                    FOR n in '+originalNodeCollectionName+' \
                        FOR k \
                            in 1..1 \
                            any n \
                            graph ' + originalGraphName + ' \
                            filter n.@selectionAttribute != k.@selectionAttribute \
                            return k) \
        '
    '''

    if threshold and method.lower()=='attribute':
        # the method of simplification is to include nodes with values over a threshold, this is a smpler 
        # query so the nodes of the graph can be traversed directly, even for large graphs
        query_str = '''
                FOR doc in @@nodeCollection
                        FILTER doc.@selectionAttribute > @threshold
                        RETURN doc
                '''
        bind_vars = {'@nodeCollection': originalNodeCollectionName,
                        'selectionAttribute':method_attribute, 'threshold':threshold}
        cursor = db.aql.execute(query=query_str, bind_vars=bind_vars)
        boundary_node_list = [doc for doc in cursor]
    else:
        # we will break up the nodes into batches, using the batchsize argument
        numberOfBatches = (len(node_names)//batchsize)
        print('number of full batches:',numberOfBatches)
        # start with an empty set for the nodes.  We will add to it incrementally with each batch
        nodeKeySet = {}
        for b in range(numberOfBatches):
            #print('batch',b)
            batch = node_names[b*batchsize:(b+1)*batchsize]
            #print('batch contents:',batch)

            # perform a query where we feed in a batch of node Ids.  We create a small batch because traversing
            # every node in the graph at once takes too long for large graphs.  In our loop, we look up the node
            # by name and assign it as the startNode.  Then we traverse one-hop from the startNode and output any
            # nodes we encounter that have a different selectionAttribute than our current node (because they are
            # boundary nodes that are connected. )
            query_str = '''
                    RETURN UNIQUE(For nodeKey in @node_keys
                        LET Start = (FOR doc in @@nodeCollection
                            FILTER doc.@nameField == nodeKey
                            RETURN doc)
                        FOR k 
                            in 1..1 
                            any Start[0]._id 
                            graph @originalGraphName
                            filter Start[0].@selectionAttribute != k.@selectionAttribute 
                            return k)
                '''
            bind_vars = {'node_keys': batch, '@nodeCollection': originalNodeCollectionName,
                        'selectionAttribute':method_attribute,'nameField':nameField, 'originalGraphName':originalGraphName}
            print('finding boundary nodes, using attribute:',method_attribute)
            cursor = db.aql.execute(query=query_str, bind_vars=bind_vars)
            print('complete')
            boundary_node_return = [doc for doc in cursor]
            #print('length of boundary_node_return',len(boundary_node_return[0]))
            #print('boundary_node_return',boundary_node_return)
            # there could be duplicates from a previous batch, so assign to a dictionary to remove duplicates
            for node in boundary_node_return[0]:
                nodeKeySet[node['_key']] = node 
            #print('length of nodeKeySet',len(nodeKeySet))
            #print('nodeKeySet:',nodeKeySet)

        # there is a last partial batch of nodes left, run this smaller final query on lefover nodes
        remainingNodeCount = len(node_names) - numberOfBatches*batchsize 
        print('now processing the remaining',remainingNodeCount,'nodes')
        batch = node_names[numberOfBatches*batchsize:len((node_names))]
        #print('batch contents:',batch)
        # perform a query where we feed in a batch of node Ids.  We create a small batch because traversing
        # every node in the graph at once takes too long for large graphs.  In our loop, we look up the node
        # by name and assign it as the startNode.  Then we traverse one-hop from the startNode and output any
        # nodes we encounter that have a different selectionAttribute than our current node (because they are
        # boundary nodes that are connected. )
        query_str = '''
                RETURN UNIQUE(For nodeKey in @node_keys
                    LET Start = (FOR doc in @@nodeCollection
                        FILTER doc.@nameField == nodeKey
                        RETURN doc)
                    FOR k 
                        in 1..1 
                        any Start[0]._id 
                        graph @originalGraphName
                        filter Start[0].@selectionAttribute != k.@selectionAttribute 
                        return k)
            '''
        bind_vars = {'node_keys': batch, '@nodeCollection': originalNodeCollectionName,
                        'selectionAttribute':method_attribute,'nameField':nameField, 'originalGraphName':originalGraphName}
        cursor = db.aql.execute(query=query_str, bind_vars=bind_vars)
        boundary_node_return = [doc for doc in cursor]
        #print('length of boundary_node_return',len(boundary_node_return[0]))
        #print('boundary_node_return',boundary_node_return)
        # eliminate duplicates that were in previous batches
        for node in boundary_node_return[0]:
            nodeKeySet[node['_key']] = node 
        print('final length of nodeKeySet',len(nodeKeySet.keys()))
        #print('nodeKeySet:',nodeKeySet)


        # make a python list containing only the boundary nodes for use below
        boundary_node_list = []
        for key in nodeKeySet.keys():
            boundary_node_list.append(nodeKeySet[key])

    # ----------- end of batch breakup method
    if len(boundary_node_list)>0:
        print('sample node:',boundary_node_list[0])
    print('found ',len(boundary_node_list),'boundary nodes')


    # make an index of the edges by ID and by name for use later
    nodeIds = []
    nodeDict = {}
    nodeDictByName = {}

    # ** note, the nodeDict is essentially a duplicate of the nodeKeySet above. These 
    # could be merged later to improve the algorithm's efficiency?

    for node in boundary_node_list:
        nodeIds.append(node['_id'])
        nodeDict[node['_id']] = node
        nodeDictByName[node[nameField]] = node

    # now that we have the nodes, lets loop through the edges and find the 
    # edges that are connecting these nodes to another boundary node which has a different
    # community.  All edges between same community nodes will be filtered out. This technique
    # will reduce the graph but will leave some nodes floating without an edge. 

    # similarly to the node selection step above, we will split the edges into batches to prevent (or reduce)
    # timeouts from happening.  First, we will do a simple query to get the edges in a python list

    print('gathering all the edges')
    all_edges = []
    bind_vars = {"@EdgeCollName": originalEdgeCollectionName}
    query_str = 'FOR e in @@EdgeCollName RETURN e'
    cursor = db.aql.execute(query=query_str, bind_vars=bind_vars,batch_size=10000)
    all_edges = [doc for doc in cursor]
    if cursor.has_more():
       while cursor.has_more():
        while not cursor.empty():
            edges = cursor.next()
            for edge in cursor:
                all_edges.append(edge)
        print('edge count is:',len(all_edges))
        cursor.fetch()
    # get the last partial batch
    while not cursor.empty():
        edges = cursor.next()
        for edge in cursor:
            all_edges.append(edge)
    print('total edge count is:',len(all_edges))

    # below is a query that works in arango for small enough databases.  But the arango python driver
    # times out for queries over 60 seconds, even though the database continues working on the query. 

    '''
    bind_vars = {"nodeIds": nodeIds,"nodeDict": nodeDict,'selectionAttribute': method_attribute}
    query_str = 'FOR e in '+ originalEdgeCollectionName + ' \
                        Filter (e._from IN @nodeIds) \
                            Filter (e._to IN @nodeIds) \
                            Filter @nodeDict[e._from].@selectionAttribute != @nodeDict[e._to].@selectionAttribute \
                            RETURN e\
                    '
    cursor = db.aql.execute(query=query_str, bind_vars=bind_vars)
    interior_edges = [doc for doc in cursor]
    '''

    # since we have all the nodes in a dictonary and all the edges in a list, we will just do the query  in local Python
    # memory space instead to avoid the complexity of AQL, at least initially... convert nodes to a set since testing
    # membership is faster than in a list. We are looking for edges with both ends in the boundary node set and the
    # edge should connect nodes with a different selection attribute (e.g. different community)

    print('calculating interior edges from full edge set')
    nodeIdsSet = set(nodeIds)
    interior_edges = []
    for e in all_edges:
        if (e['_from'] in nodeIdsSet) and (e['_to'] in nodeIdsSet) and (nodeDict[e['_from']][method_attribute] != nodeDict[e['_to']][method_attribute]):
            interior_edges.append(e)
    print('count of interior edges:',len(interior_edges))
    if len(interior_edges)>0:
        print('sample interior edge',interior_edges[0])

    # now that we have the edges that are connecting disparate community nodes, let us
    # just keep only the nodes that have an incident edge.  Some nodes might have become unused
    # since we just thinned the edges. A node might be in this list multiple times

    print('removing floating nodes now that edges have been thinned')

    '''
    *** redo the loop below looping once through edges and adding nodes to a nodesUsedSet to be O(e) instead of O(n*e)
    
    nodesUsed = []
    for n in nodeIds:
        for e in interior_edges:
            #print('edge:',e)
            if e['_from'] == n or e['_to'] == n:
                nodesUsed.append(n)
    '''
    nodesUsedSet = set()
    for e in interior_edges:
        nodesUsedSet.add(e['_from'])
        nodesUsedSet.add(e['_to'])
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

    # record when the algorithm is done (without Arango output collection creation)
    reductionTime = arrow.now()

    # loop through the node dictionary and insert these nodes into a new output collection
    # This is done in two steps because we are preserving the _key values from the original collection.
    # The node is Inserted with a specific key in the first query.  Then the second query updates the other
    # node attribute values, whatever attribute is present.  
    
    uniqueNodes = [n for n in nodeDictByKey.keys()]

    bind_vars = {"nodesUsed": uniqueNodes,"nodeDict": nodeDictByKey,'@outputNodeColl':reducedNodeCollectionName}
    query_str = '''FOR n in @nodesUsed 
                    INSERT { 
                        _key: @nodeDict[n]._key, 
                        } 
                    INTO @@outputNodeColl
                    '''
    print('entering nodes into new collection')
    if createOutputCollections:
        cursor = db.aql.execute(query=query_str,bind_vars=bind_vars)

    # now add any other attributes, regardless of the attribute name, to the nodes
    bind_vars = {"nodesUsed": uniqueNodes,"nodeDict": nodeDictByKey,'@outputNodeColl':reducedNodeCollectionName}
    query_str = '''FOR n in @nodesUsed 
                    UPDATE @nodeDict[n] 
                    INTO @@outputNodeColl
                    '''
    print('entering nodes into new collection')
    if createOutputCollections:
        cursor = db.aql.execute(query=query_str,bind_vars=bind_vars)

    # create an output edge collection of the reduced edges only. However, there is 
    # a problem: the reducedNodes have different IDs than the original nodes 
    # but the edges reference the original nodes.  Our NodeDictByKey about has the 
    # same _key values as the original nodes, so lets just change the _from and _to 
    # references from the original collection name to the new collection name
    
    print('renaming _to and _from in edges for new reduced collection')
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
    bind_vars = {"edgesUsed": fixed_interior_edges,'@outputEdgeColl': reducedEdgeCollectionName}
    query_str = '''FOR e in @edgesUsed 
                    INSERT e 
                    INTO @@outputEdgeColl
                    '''
    print('writing edges to new collection')
    if createOutputCollections:
        cursor = db.aql.execute(query=query_str, bind_vars=bind_vars)

    print('algorithm is complete')
    stoptime = arrow.now()

    algorithmTime = reductionTime-starttime
    totalTime = stoptime-starttime
    print('algorithm time:',algorithmTime)
    print('total time:',totalTime)

    return len(uniqueNodes), len(interior_edges)


'''
databaseName = kg_db = 'w-09e8f51bf51f45008b70f903f6920d5c'
originalGraphName = 'eurovis-2019'
#this_method = 'LabelProp'
#this_method = 'SpeakerListener'
this_method = 'Attribute'
selected_attribute = '_betweenness'
threshold = 0.015
if this_method == 'Attribute':
    reducedGraphName = 'reduced_'+originalGraphName+'_'+selected_attribute+'_'+str(threshold)
else:
    reducedGraphName = 'reduced_'+originalGraphName+'_'+this_method
originalNodeCollectionName = 'people'
originalEdgeCollectionName = 'connections'
if this_method == 'Attribute':
    reducedNodeCollectionName = originalNodeCollectionName+'_reduced_'+this_method
    reducedEdgeCollectionName = originalEdgeCollectionName + '_reduced_'+this_method
else:
    reducedNodeCollectionName = originalNodeCollectionName+'_reduced_'+this_method
    reducedEdgeCollectionName = originalEdgeCollectionName + '_reduced_'+this_method   
'''

'''
databaseName = kg_db = 'graph_test'
originalGraphName = 'inet_july06'
this_method = 'LabelProp'
this_method = 'SpeakerListener'
this_method = 'Attribute'
selected_attribute = '_betweenness'
threshold = 0.0001
if this_method == 'Attribute':
    reducedGraphName = 'reduced_'+originalGraphName+'_'+selected_attribute+'_'+str(threshold)
else:
    reducedGraphName = 'reduced_'+originalGraphName+'_'+this_method
originalNodeCollectionName = 'inet_july06_verts'
originalEdgeCollectionName = 'inet_july06_edges'
if this_method == 'Attribute':
    reducedNodeCollectionName = originalNodeCollectionName+'_reduced_'+this_method
    reducedEdgeCollectionName = originalEdgeCollectionName + '_reduced_'+this_method
else:
    reducedNodeCollectionName = originalNodeCollectionName+'_reduced_'+this_method
    reducedEdgeCollectionName = originalEdgeCollectionName + '_reduced_'+this_method   
'''

databaseName = kg_db = 'graph_test'
originalGraphName = 'hyves'
this_method = 'LabelProp'
this_method = 'SpeakerListener'
this_method = 'Attribute'
selected_attribute = '_betweenness'
threshold = 0.0001
if this_method == 'Attribute':
    reducedGraphName = 'reduced_'+originalGraphName+'_'+selected_attribute+'_'+str(threshold)
else:
    reducedGraphName = 'reduced_'+originalGraphName+'_'+this_method
originalNodeCollectionName = 'hyves_verts'
originalEdgeCollectionName = 'hyves_edges'
if this_method == 'Attribute':
    reducedNodeCollectionName = originalNodeCollectionName+'_reduced_'+this_method
    reducedEdgeCollectionName = originalEdgeCollectionName + '_reduced_'+this_method
else:
    reducedNodeCollectionName = originalNodeCollectionName+'_reduced_'+this_method
    reducedEdgeCollectionName = originalEdgeCollectionName + '_reduced_'+this_method   



client = ArangoClient(hosts="http://localhost:8529")
# Connect to the Arango database as root user.
db = client.db(databaseName, username="root", password="letmein")

#AddAllGraphMetrics(db,originalGraphName,originalNodeCollectionName)

'''
CreateSimplifiedGraph(databaseName,originalGraphName,originalEdgeCollectionName,
                      originalNodeCollectionName, reducedGraphName, reducedEdgeCollectionName,reducedNodeCollectionName,
                      method=this_method,nameField='_id', 
                      selected_attribute=selected_attribute,threshold=threshold,createOutputCollections=True)
''' 
databaseList = [{'dbname':'inet_july06','nodes':'inet_july06_verts','edges':'inet_july06_edges'}]

# make looping test for a graph to try different reductions
methodList = ['labelprop','speakerlistener','attribute']
methodList = ['attribute']
attributes = ['_degree','_betweenness','_pagerank']

doOutput = False
resultsList = []


#AddAllGraphMetrics(db,originalGraphName,originalNodeCollectionName)

for method in methodList:
    if method == 'attribute':
        for attrib in attributes:
            # start with initial and then divide by ten until reaching the final
            if attrib == '_degree':
                thresholdBoundary = [10000,5]
            else:
                #maxThresh = find_percentage(originalNodeCollectionName,)
                thresholdBoundary = [0.005,1e-8]
            threshold = thresholdBoundary[0]
            while threshold > thresholdBoundary[1]:
                numNodes,numEdges = CreateSimplifiedGraph(databaseName,originalGraphName,originalEdgeCollectionName,
                        originalNodeCollectionName, reducedGraphName, reducedEdgeCollectionName,reducedNodeCollectionName,
                        method=method,nameField='_id', 
                        selected_attribute=attrib,threshold=threshold,createOutputCollections=doOutput)
                resultRec = {'method':method,'attribute':attrib,'threshold':threshold,'nodes':numNodes,'edges':numEdges}
                resultsList.append(resultRec)
                threshold = threshold / 10.0
    else:
        attrib = ''
        threshold=0
        numNodes,numEdges = CreateSimplifiedGraph(databaseName,originalGraphName,originalEdgeCollectionName,
                    originalNodeCollectionName, reducedGraphName, reducedEdgeCollectionName,reducedNodeCollectionName,
                    method=method,nameField='_id', 
                    selected_attribute=attrib,threshold=threshold,createOutputCollections=doOutput)
    resultRec = {'method':method,'attribute':'N/A','threshold':0,'nodes':numNodes,'edges':numEdges}
    resultsList.append(resultRec)

resultsDF = pd.DataFrame(resultsList)
resultsDF.to_csv(originalGraphName+'_results.csv',index=False)

