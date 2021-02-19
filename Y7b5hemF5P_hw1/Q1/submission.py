import http.client 
import json
import csv


#############################################################################################################################
# cse6242 s21
# All instructions, code comments, etc. contained within this notebook are part of the assignment instructions.
# Portions of this file will auto-graded in Gradescope using different sets of parameters / data to ensure that values are not
# hard-coded.
#
# Instructions:  Implement all methods in this file that have a return
# value of 'NotImplemented'. See the documentation within each method for specific details, including
# the expected return value
#
# Helper Functions:
# You are permitted to write additional helper functions/methods or use additional instance variables within
# the `Graph` class or `TMDbAPIUtils` class so long as the originally included methods work as required.
#
# Use:
# The `Graph` class  is used to represent and store the data for the TMDb co-actor network graph.  This class must
# also provide some basic analytics, i.e., number of nodes, edges, and nodes with the highest degree.
#
# The `TMDbAPIUtils` class is used to retrieve Actor/Movie data using themoviedb.org API.  We have provided a few necessary methods
# to test your code w/ the API, e.g.: get_movie_cast(), get_movie_credits_for_person().  You may add additional
# methods and instance variables as desired (see Helper Functions).
#
# The data that you retrieve from the TMDb API is used to build your graph using the Graph class.  After you build your graph using the
# TMDb API data, use the Graph class write_edges_file & write_nodes_file methods to produce the separate nodes and edges
# .csv files for use with the Argo-Lite graph visualization tool.
#
# While building the co-actor graph, you will be required to write code to expand the graph by iterating
# through a portion of the graph nodes and finding similar artists using the TMDb API. We will not grade this code directly
# but will grade the resulting graph data in your Argo-Lite graph snapshot.
#
#############################################################################################################################


class Graph:

    # Do not modify
    def __init__(self, with_nodes_file=None, with_edges_file=None):
        """
        option 1:  init as an empty graph and add nodes
        option 2: init by specifying a path to nodes & edges files
        """
        self.nodes = []
        self.edges = []
        if with_nodes_file and with_edges_file:
            nodes_CSV = csv.reader(open(with_nodes_file))
            nodes_CSV = list(nodes_CSV)[1:]
            self.nodes = [(n[0],n[1]) for n in nodes_CSV]

            edges_CSV = csv.reader(open(with_edges_file))
            edges_CSV = list(edges_CSV)[1:]
            self.edges = [(e[0],e[1]) for e in edges_CSV]

    def node_exists (self, id: str, name: str) -> bool:
        
        nodes_dict = dict(self.nodes)
        
        if  str(id) in nodes_dict:
            return True
        else:
            return False
        
    def add_node(self, id: str, name: str)->None:
        """
        add a tuple (id, name) representing a node to self.nodes if it does not already exist
        The graph should not contain any duplicate nodes
        """
        '''reem'''
        #if not self.node_exists(id, name):
        name = name.replace(',', '')
        self.nodes.append((str(id), name))
        
        return None
    
    def  valid_edge (self, source: str, target: str) -> bool:
        if str(source) == str(target):
            return False
        else:
            if self.edge_exists(source, target):
                return False
            return True

    def edge_exists (self, source: str, target: str)->bool:
        
        '''edges_dict = dict(self.edges)
        if  str(source) in edges_dict.keys():
            if str(edges_dict[source]) == str(target):
                return True
        elif  str(target) in edges_dict.keys():
            if str(edges_dict[target]) == str(source):
                return True
        else:
            return False
        '''
        for t in self.edges:
            if ((t[0] == source and t[1]== target) or (t[0]==target and t[1]==source)):
                return True
        
        return False
    
    def add_edge(self, source: str, target: str)->None:
        """
        Add an edge between two nodes if it does not already exist.
        An edge is represented by a tuple containing two strings: e.g.: ('source', 'target').
        Where 'source' is the id of the source node and 'target' is the id of the target node
        e.g., for two nodes with ids 'a' and 'b' respectively, add the tuple ('a', 'b') to self.edges
        """
        
        '''reem'''
        #if not ((source, target) or (target, source)) in self.edges:
        if(self.valid_edge( source, target)):
            self.edges.append((str(source), str(target)))
        ''''''
        return None


    def total_nodes(self)->int:
        """
        Returns an integer value for the total number of nodes in the graph
        """
        return len(self.nodes)


    def total_edges(self)->int:
        """
        Returns an integer value for the total number of edges in the graph
        """
        return len(self.edges)


    def max_degree_nodes(self)->dict:
        """
        Return the node(s) with the highest degree
        Return multiple nodes in the event of a tie
        Format is a dict where the key is the node_id and the value is an integer for the node degree
        e.g. {'a': 8}
        or {'a': 22, 'b': 22}
        """
        node_degrees = {}
        for edge in self.edges:
            source = edge[0]
            if source in node_degrees:
                degree = node_degrees[source] +1
                node_degrees.update({source:degree})    #(update)
            else:
                node_degrees.update({source:1})    #(add)
                
            target = edge[1]
            if target in node_degrees:
                degree = node_degrees[target] +1
                node_degrees.update({target:degree})    #(update)
            else:
                node_degrees.update({target:1})    #(add)     
                
        sortedlist = sorted(node_degrees.items(),  key=lambda x: (int(x[1]),str(x[0])), reverse  = True)
        
        max_deg_nodes_dict = {}
        if len(sortedlist) >0:
            max_degree_node = sortedlist[0]
            max_degree = max_degree_node[1]
            max_deg_nodes_dict.update({max_degree_node[0]:max_degree}) 
            for node in sortedlist:
                if node[1] == max_degree:
                  max_deg_nodes_dict.update({node[0]:max_degree})  
          
        max_deg_dict = dict(sorted (max_deg_nodes_dict.items()) )  
                  
        return max_deg_dict


    def print_nodes(self):
        """
        No further implementation required
        May be used for de-bugging if necessary
        """
        print(self.nodes)


    def print_edges(self):
        """
        No further implementation required
        May be used for de-bugging if necessary
        """
        print(self.edges)


    # Do not modify
    def write_edges_file(self, path="edges.csv")->None:
        """
        write all edges out as .csv
        :param path: string
        :return: None
        """
        edges_path = path
        edges_file = open(edges_path, 'w', encoding='utf-8')

        edges_file.write("source" + "," + "target" + "\n")

        for e in self.edges:
            edges_file.write(e[0] + "," + e[1] + "\n")
            #edges_file.write(str(e[0]) + "," + str(e[1]) + "\n")
            
        edges_file.close()
        print("finished writing edges to csv")


    # Do not modify
    def write_nodes_file(self, path="nodes.csv")->None:
        """
        write all nodes out as .csv
        :param path: string
        :return: None
        """
        nodes_path = path
        nodes_file = open(nodes_path, 'w', encoding='utf-8')

        nodes_file.write("id,name" + "\n")
        for n in self.nodes:
            nodes_file.write(n[0] + "," + n[1] + "\n")
            #nodes_file.write(str(n[0]) + "," + str(n[1]) + "\n")
        nodes_file.close()
        print("finished writing nodes to csv")



class  TMDBAPIUtils:

    # Do not modify
    def __init__(self, api_key:str):
        self.api_key=api_key


    def get_movie_cast(self, movie_id:str, limit:int=None, exclude_ids:list=None) -> list:
        """
        Get the movie cast for a given movie id, with optional parameters to exclude an cast member
        from being returned and/or to limit the number of returned cast members
        documentation url: https://developers.themoviedb.org/3/movies/get-movie-credits

        :param integer movie_id: a movie_id
        :param integer limit: maximum number of returned cast members by their 'order' attribute
            e.g., limit=5 will attempt to return the 5 cast members having 'order' attribute values between 0-4
            constrain 1--If after exluding, there are fewer cast members than the specified limit or the limit not specified, return all cast members.
            constrain 2--If cast members with 'order' attribute in the specified limit range have been excluded, do not include more cast members to reach the limit.
            e.g., if limit=5 and the actor whose id corresponds to cast member with order=1 is to be excluded,
            return cast members with order values [0, 2, 3, 4], not [0, 2, 3, 4, 5]
        :param list exclude_ids: a list of ints containing ids (not cast_ids) of cast members  that should be excluded from the returned result
            constrain 3--e.g., if exclude_ids are [353, 455] then exclude these from any result.
        :rtype: list
            return a list of dicts, one dict per cast member with the following structure:
                [{'id': '97909' # the id of the cast member
                'character': 'John Doe' # the name of the character played
                'credit_id': '52fe4249c3a36847f8012927' # id of the credit, ...}, ... ]
                Note that this is an example of the structure of the list and some of the fields returned by the API.
                The result of the API call will include many more fields for each cast member.
        """
        cast_list =[]
        #r1 = 0
        #while r1 != 200:
        conn = http.client.HTTPSConnection("api.themoviedb.org")
        
        conn.request("GET", "/3/movie/"+str(movie_id)+"/credits?api_key="+self.api_key+"&language=en-US")
        
        r1 = conn.getresponse()   
        
        if r1.status == 200:
            data1 = r1.read()
            #print(len(data1))
            y = json.loads(data1)
            cast = y["cast"]
            for c in range(len(cast)):
                c_name = cast[c]['name']
                if c_name.find(",")>-1:
                    c_name = c_name.replace(",", "")
                cast_dict = dict({"id": str(cast[c]['id']), "character": cast[c]['character'], "credit_id" : cast[c]['credit_id'], "order" : cast[c]['order']
                                  , "adult": cast[c]['adult'] , 
                                  "gender": cast[c]['gender'], "known_for_department":cast[c]['known_for_department'], "name":c_name, "original_name":cast[c]['original_name'],
                                  "popularity": cast[c]['popularity'],"profile_path": cast[c]['profile_path'] 
                                  })
                
                
                cast_list.append(cast_dict)
            
            cast_list = sorted(cast_list, key = lambda i: i['order'])
            
            
           
            
            cast_list_limited = []
            '''
            if (limit is not None) and (limit <= len(cast_list)): #constrain 1,2
                cast_list = sorted(cast_list, key = lambda i: i['order'])
                cast_list= cast_list[:limit]
            
            '''
            cast_list_filtered=[] #constrain 3
            if exclude_ids is not None:
                for c in range(len(cast_list)):
                    if int(cast_list[c]['id']) not in exclude_ids:
                        cast_list_filtered.append(cast_list[c])
                
                cast_list = cast_list_filtered.copy()
                
            if limit is not None:
                
                for c in range(len(cast_list)):
                    
                    if cast_list[c]['order'] < limit:
                        cast_list_limited.append(cast_list[c])
                
                cast_list = cast_list_limited.copy()
                
            
           
            return cast_list
            #break 
            #elif (r1.status == 504 or r1.status == 61):
                #time.sleep(2)


    def get_movie_credits_for_person(self, person_id:str, vote_avg_threshold:float=None)->list:
        """
        Using the TMDb API, get the movie credits for a person serving in a cast role
        documentation url: https://developers.themoviedb.org/3/people/get-person-movie-credits

        :param string person_id: the id of a person
        :param vote_avg_threshold: optional parameter to return the movie credit if it is >=
            the specified threshold.
           #constrain 4 e.g., if the vote_avg_threshold is 5.0, then only return credits with a vote_avg >= 5.0
        :rtype: list
            return a list of dicts, one dict per movie credit with the following structure:
                [{'id': '97909' # the id of the movie credit
                'title': 'Long, Stock and Two Smoking Barrels' # the title (not original title) of the credit
                'vote_avg': 5.0 # the float value of the vote average value for the credit}, ... ]
        """
        '''reem test'''
        movies =[]
        conn = http.client.HTTPSConnection("api.themoviedb.org")
        conn.request("GET", "/3/person/"+person_id+"/movie_credits?api_key="+self.api_key+"&language=en-US")

        r1 = conn.getresponse()
        #print(r1.status, r1.reason)
        if r1.status == 200:
            data = r1.read()
            #print(len(data1))
            y = json.loads(data)
            
            #print(json.dumps(y, indent=4, sort_keys=True))
            cast = y["cast"]
            if cast is not None or len(cast) != 0:
                #print("json len", len(cast), " cast type ", type(cast))
                for c in range(len(cast)):
                    
                    if ((vote_avg_threshold is not None and float(cast[c]['vote_average']) >= vote_avg_threshold) or (vote_avg_threshold is None)): #constrain 4
                        #print(cast[c]['title'],"  ", cast[c]['id'],"  ", cast[c]['vote_average'])
                        movie = dict({"id": str(cast[c]['id']), "title": cast[c]['title'], "vote_avg" : cast[c]['vote_average'] })
                        movies.append(movie)
               
                
        conn.close()
        ''''''
        return movies


#############################################################################################################################
#
# BUILDING YOUR GRAPH
#
# Working with the API:  See use of http.request: https://docs.python.org/3/library/http.client.html#examples
#
# Using TMDb's API, build a co-actor network for the actor's/actress' highest rated movies
# In this graph, each node represents an actor
# An edge between any two nodes indicates that the two actors/actresses acted in a movie together
# i.e., they share a movie credit.
# e.g., An edge between Samuel L. Jackson and Robert Downey Jr. indicates that they have acted in one
# or more movies together.
#
# For this assignment, we are interested in a co-actor network of highly rated movies; specifically,
# we only want the top 3 co-actors in each movie credit of an actor having a vote average >= 8.0.
# Build your co-actor graph on the actor 'Laurence Fishburne' w/ person_id 2975.
#
# You will need to add extra functions or code to accomplish this.  We will not directly call or explicitly grade your
# algorithm. We will instead measure the correctness of your output by evaluating the data in your argo-lite graph
# snapshot.
#
# GRAPH SIZE
# With each iteration of your graph build, the number of nodes and edges grows approximately at an exponential rate.
# Our testing indicates growth approximately equal to e^2x.
# Since the TMDB API is a live database, the number of nodes / edges in the final graph will vary slightly depending on when
# you execute your graph building code. We take this into account by rebuilding the solution graph every few days and
# updating the auto-grader.  We establish a bound for lowest & highest encountered numbers of nodes and edges with a
# margin of +/- 100 for nodes and +/- 150 for edges.  e.g., The allowable range of nodes is set to:
#
# Min allowable nodes = min encountered nodes - 100
# Max allowable nodes = max allowable nodes + 100
#
# e.g., if the minimum encountered nodes = 507 and the max encountered nodes = 526, then the min/max range is 407-626
# The same method is used to calculate the edges with the exception of using the aforementioned edge margin.
# ----------------------------------------------------------------------------------------------------------------------
# BEGIN BUILD CO-ACTOR NETWORK
#
# INITIALIZE GRAPH
#   Initialize a Graph object with a single node representing Laurence Fishburne
#
# BEGIN BUILD BASE GRAPH:
#   Find all of Laurence Fishburne's movie credits that have a vote average >= 8.0
#   FOR each movie credit:
#   |   get the movie cast members having an 'order' value between 0-2 (these are the co-actors)
#   |
#   |   FOR each movie cast member:
#   |   |   using graph.add_node(), add the movie cast member as a node (keep track of all new nodes added to the graph)
#   |   |   using graph.add_edge(), add an edge between the Laurence Fishburne (actress) node
#   |   |   and each new node (co-actor/co-actress)
#   |   END FOR
#   END FOR
# END BUILD BASE GRAPH
'''reem code'''

graph = Graph()
graph.add_node(id='2975', name='Laurence Fishburne')
tmdb_api_utils = TMDBAPIUtils(api_key='2cd0a668150c5e5cb91c9a5180c991a8')

movie_credits =tmdb_api_utils.get_movie_credits_for_person('2975', vote_avg_threshold = 8.0)

added_nodes = []
for movie in movie_credits:
    cast = tmdb_api_utils.get_movie_cast(str(movie['id']), 3)
    for cast_member in cast:
        
        if not graph.node_exists(cast_member['id'], cast_member['name']):
            print(cast_member['id'], cast_member['name'])
            graph.add_node(cast_member['id'], cast_member['name'])
            
            added_nodes.append((cast_member['id'], cast_member['name']))
            graph.add_edge(source='2975', target= cast_member['id'])
        
        #if graph.valid_edge(source='2975', target= cast_member['id']):   
            
        
  
        
#print("added nodes ", added_nodes)
#print("edges ", graph.edges)

#
# BEGIN LOOP - DO 2 TIMES:
#   IF first iteration of loop:
#   |   nodes = The nodes added in the BUILD BASE GRAPH (this excludes the original node of Laurence Fishburne!)
#   ELSE
#   |    nodes = The nodes added in the previous iteration:
#   ENDIF
#
#   FOR each node in nodes:
#   |  get the movie credits for the actor that have a vote average >= 8.0
#   |
#   |   FOR each movie credit:
#   |   |   try to get the 3 movie cast members having an 'order' value between 0-2
#   |   |
#   |   |   FOR each movie cast member:
#   |   |   |   IF the node doesn't already exist:
#   |   |   |   |    add the node to the graph (track all new nodes added to the graph)
#   |   |   |   ENDIF
#   |   |   |
#   |   |   |   IF the edge does not exist:
#   |   |   |   |   add an edge between the node (actor) and the new node (co-actor/co-actress)
#   |   |   |   ENDIF
#   |   |   END FOR
#   |   END FOR
#   END FOR
# END LOOP

#nodes = []


for i in range(2):
    print(i)
    if i == 0:    #first iteration
        nodes = added_nodes.copy()
        added_nodes.clear()
    else:
        nodes = added_nodes.copy()
    #print("nodes ", nodes)
    for n in nodes:
        movie_credits =tmdb_api_utils.get_movie_credits_for_person(str(n[0]), vote_avg_threshold = 8.0) 
        for movie in movie_credits:
            cast = tmdb_api_utils.get_movie_cast(str(movie['id']), limit = 3)
            for cast_member in cast:
                if (not graph.node_exists(cast_member['id'], cast_member['name'])):
                    graph.add_node(cast_member['id'], cast_member['name'])
                    added_nodes.append((cast_member['id'], cast_member['name']))
                
                #if (graph.valid_edge(source=str(n[0]), target= str(cast_member['id'])) and not graph.edge_exists(source=str(n[0]), target= str(cast_member['id']))):    
                if (graph.valid_edge(source=str(n[0]), target= str(cast_member['id']))):   
                    graph.add_edge(source=str(n[0]), target= str( cast_member['id']))
                
               

# Your graph should not have any duplicate edges or nodes
# Write out your finished graph as a nodes file and an edges file using:

graph.write_edges_file()
graph.write_nodes_file()

#print(graph.total_nodes())
#print(graph.total_edges())
#print(graph.max_degree_nodes())

#
# END BUILD CO-ACTOR NETWORK
# ----------------------------------------------------------------------------------------------------------------------

# Exception handling and best practices
# - You should use the param 'language=en-US' in all API calls to avoid encoding issues when writing data to file.
# - If the actor name has a comma char ',' it should be removed to prevent extra columns from being inserted into the .csv file
# - Some movie_credits may actually be collections and do not return cast data. Handle this situation by skipping these instances.
# - While The TMDb API does not have a rate-limiting scheme in place, consider that making hundreds / thousands of calls
#   can occasionally result in timeout errors. If you continue to experience 'ConnectionRefusedError : [Errno 61] Connection refused',
#   - wait a while and then try again.  It may be necessary to insert periodic sleeps when you are building your graph.


def return_name()->str:
    """
    Return a string containing your GT Username
    e.g., gburdell3
    Do not return your 9 digit GTId
    """
    return "rsoliman3"


def return_argo_lite_snapshot()->str:
    """
    Return the shared URL of your published graph in Argo-Lite
    """
    return "https://poloclub.github.io/argo-graph-lite/#95b58874-38b1-4263-9e3a-040f30f64986"
          


# You should modify __main__ as you see fit to build/test your graph using  the TMDBAPIUtils & Graph classes.
# Some boilerplate/sample code is provided for demonstration. We will not call __main__ during grading.

if __name__ == "__main__":

    
    
    graph = Graph()
    graph.add_node(id='2975', name='Laurence Fishburne')
    tmdb_api_utils = TMDBAPIUtils(api_key='2cd0a668150c5e5cb91c9a5180c991a8')
    print("#############################################")
    cast = tmdb_api_utils.get_movie_cast('329')
    for cast_member in cast:
        print(cast_member['order'] ," ",cast_member['id'], " "+ cast_member['name'])
        
    print(len(cast))    
    print("#############################################")
    cast = tmdb_api_utils.get_movie_cast('329', exclude_ids =[4783,4784])
    for cast_member in cast:
        print(cast_member['order']," ", cast_member['id'], " "+ cast_member['name'])
    print(len(cast))   
    print("#############################################")
    cast = tmdb_api_utils.get_movie_cast('329', 2)
    for cast_member in cast:
        print(cast_member['order']," ", cast_member['id'], " "+ cast_member['name'])
    print(len(cast))  
    print("#############################################")
    cast = tmdb_api_utils.get_movie_cast('329', 10,[4785,4790])
    for cast_member in cast:
        print(cast_member['order']," ", cast_member['id'], " "+ cast_member['name'])
    print(len(cast))     
    #reem test
    '''
    x=tmdb_api_utils.get_movie_credits_for_person('2975', 8.0)
    cast=tmdb_api_utils.get_movie_cast('464052', limit=6, exclude_ids=[41091, 2960])
    mo= tmdb_api_utils.get_movie_credits_for_person('33663')
    print("trump",len(mo))
    print(cast)
    
    
    graph.add_node('1111', 'lolo')
    graph.add_node('2222', 'toto')
    graph.add_node('3333', 'soso')
    graph.add_edge('1111', '2222')
    graph.add_edge('1111', '3333')
    graph.add_edge('2222', '3333')
    graph.add_edge('3333', '2222')
    '''
    # call functions or place code here to build graph (graph building code not graded)
    # Suggestion: code should contain steps outlined above in BUILD CO-ACTOR NETWORK

    #graph.write_edges_file()
    #graph.write_nodes_file()

    # If you have already built & written out your graph, you could read in your nodes & edges files
    # to perform testing on your graph.
    # graph = Graph(with_edges_file="edges.csv", with_nodes_file="nodes.csv")
