#
# Copyright (c) 2010, 2013, Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
#

"""
This module contains an abstraction of a topolgy map object used to discover
subordinates and down-stream replicants for mapping topologies.
"""

from mysql.utilities.common.options import parse_user_password
from mysql.utilities.exception import UtilError

_START_PORT = 3306

class TopologyMap(object):
    """The TopologyMap class can be used to connect to a running MySQL server
    and discover its subordinates. Setting the option "recurse" permits the
    class to discover a replication topology by finding the subordinates for each
    subordinate for the first main requested.

    To generate a topology map, the caller must call the generate_topology_map()
    method to build the topology. This is left as a separate state because it
    can be a lengthy process thereby too long for a constructor method.
 
    The class also includes methods for printing a graph of the topology
    as well as returning a list of main, subordinate tuples reporting the
    host name and port for each.
    """

    def __init__(self, seed_server, options={}):
        """Constructor

        seed_server[in]    Main (seed) server connection dictionary                           
        options[in]        options for controlling behavior:
          recurse          If True, check each subordinate found for add'l subordinates
                           Default = False
          prompt_user      If True, prompt user if subordinate connection fails with
                           main connection parameters
                           Default = False
          quiet            if True, print only the data
                           Default = False
          width            width of report
                           Default = 75
          num_retries      Number of times to retry a failed connection attempt
                           Default = 0
        """
        from mysql.utilities.common.server import get_connection_dictionary
        
        self.recurse = options.get("recurse", False)
        self.quiet = options.get("quiet", False)
        self.prompt_user = options.get("prompt", False)
        self.num_retries = options.get("num_retries", 0)
        self.socket_path = options.get("socket_path", None)
        self.seed_server = seed_server
        self.topology = []
        self.options = options


    def _connect(self, conn):
        """Find the attached subordinates for a list of server connections.
        
        This method connects to each server in the list and retrieves its subordinates.
        It can be called recursively if the recurse parameter is True.
        
        conn[in]           Connection dictionary used to connect to server
    
        Returns tuple - main Server class instance, main:host string
        """
        import getpass
        
        from mysql.utilities.common.server import connect_servers
    
        conn_options = {
            'quiet'     : self.quiet,
            'src_name'  : "main",
            'dest_name' : None,
            'version'   : "5.0.0",
            'unique'    : True,
        }
        
        main_info = "%s:%s" % (conn['host'],
                                 conn['port'])
        main = None
        
        # Clear socket if used with a local server
        if (conn['host'] == 'localhost' or \
            conn['host'] == "127.0.0.1"):
            conn['unix_socket'] = None
        
        # Increment num_retries if not set when --prompt is used
        if self.prompt_user and self.num_retries == 0:
            self.num_retries += 1
    
        # Attempt to connect to the server given the retry limit
        for i in range(0,self.num_retries+1):
            try:
                servers = connect_servers(conn, None, conn_options)
                main = servers[0]
                break
            except UtilError, e:
                print "FAILED.\n"
                if i < self.num_retries and self.prompt_user:
                    print "Connection to %s has failed.\n" % main_info + \
                    "Please enter the following information " + \
                    "to connect to this server."
                    conn['user'] = raw_input("User name: ")
                    conn['passwd'] = getpass.getpass("Password: ")
                else:
                    # retries expired - re-raise error if still failing
                    raise UtilError(e.errmsg)
    
        return (main, main_info)
    
    
    def _check_permissions(self, server, priv):
        """Check to see if user has permissions to execute.
        
        server[in]     Server class instance
        priv[in]       privilege to check
        
        Returns True if permissions available, raises exception if not
        """
        from mysql.utilities.common.user import User

        # Check user permissions 
        user_pass_host = server.user
        if server.passwd is not None and len(server.passwd) > 0:
            user_pass_host += ":" + server.passwd
        user_pass_host += "@" + server.host
        user = User(server, user_pass_host, False)
        if not user.has_privilege("*", "*", priv):
            raise UtilError("Not enough permissions. The user must have the "
                            "%s privilege." % priv)
    

    def _get_subordinates(self, max_depth, seed_conn=None, mains_found=[]):
        """Find the attached subordinates for a list of server connections.
        
        This method connects to each server in the list and retrieves its subordinates.
        It can be called recursively if the recurse option is True.
    
        max_depth[in]       Maximum depth of recursive search
        seed_conn[in]       Current main connection dictionary. Initially,
                            this is the seed server (original main defined
                            in constructor)
        mains_found[in]   a list of all servers in main roles - used to
                            detect a circular replication topology. Initially,
                            this is an empty list as the main detection must
                            occur as the topology is traversed.
    
        Returns list - list of subordinates connected to each server in list
        """
        topology = []
        if seed_conn is None:
            seed_conn = self.seed_server
    
        main, main_info = self._connect(seed_conn)
        if main is None:
            return []
   
        # Check user permissions 
        self._check_permissions(main, "REPLICATION SLAVE")

        # Save the main for circular replication identification
        mains_found.append(main_info)
        
        if not self.quiet:
            print "# Finding subordinates for main: %s" % main_info
    
        # See if the user wants us to discover subordinates.
        discover = self.options.get("discover", None)
        if discover is None:
            return
        
        # Get user and password (supports login-paths)
        user, password = parse_user_password(discover, options=self.options)

        # Get replication topology
        subordinates = main.get_subordinates(user, password)
        subordinate_list = []
        depth = 0
        if len(subordinates) > 0:
            for subordinate in subordinates:
                if subordinate.find(":") > 0:
                    host, port = subordinate.split(":", 1)
                else:
                    host = subordinate
                    port = _START_PORT  # Use the default
                subordinate_conn = self.seed_server.copy()
                subordinate_conn['host'] = host
                subordinate_conn['port'] = port
                
                # Now check for circular replication topology - do not recurse
                # if subordinate is also a main.
                if self.recurse and not subordinate in mains_found and \
                   ((max_depth is None) or (depth < max_depth)):
                    new_list = self._get_subordinates(max_depth, subordinate_conn,
                                                mains_found)
                    if new_list == []:
                        subordinate_list.append((subordinate, []))
                    else:
                        subordinate_list.append(new_list)
                    depth += 1
                else:
                    subordinate_list.append((subordinate, []))
        topology.append((main_info, subordinate_list))
    
        return topology


    def generate_topology_map(self, max_depth):
        """Find the attached subordinates for a list of server connections.
        
        This method generates the topology for the seed server specified at
        instantiation.

        max_depth[in]       Maximum depth of recursive search
        """
        self.topology = self._get_subordinates(max_depth)


    def depth(self):
        """Return depth of the topology tree.
        
        Returns int - depth of topology tree.
        """
        return len(self.topology)


    def subordinates_found(self):
        """Check to see if any subordinates were found.
        
        Returns bool - True if subordinates found, False if no subordinates.
        """
        return not (len(self.topology) and self.topology[0][1] == [])


    def print_graph(self, topology_list=[], mains_found=[],
                    level=0, preamble=""):
        """Prints a graph of the topology map to standard output.
        
        This method traverses a list of the topology and prints a graph. The
        method is designed to be recursive traversing the list to print the
        subordinates for each main in the topology. It will also detect a circular
        replication segment and indicate it on the graph.
        
        topology_list[in]   a list in the form (main, subordinate) of server
        mains_found[in]   a list of all servers in main roles - used to
                            detect a circular replication topology. Initially,
                            this is an empty list as the main detection must
                            occur as the toplogy is traversed.
        level[in]           the level of indentation - increases with each
                            set of subordinates found in topology
        preamble[in]        prefix calculated during recursion to indent text
        """
        # if first iteration, use the topology list generated earlier
        if topology_list == []:
            if self.topology == []:
                # topology not generated yet
                raise UtilError("You must first generate the topology.")
            topology_list = self.topology

        # Detect if we are looking at a sublist or not. Get sublist.
        if len(topology_list) == 1:
            topology_list = topology_list[0]
        main = topology_list[0]
        
        # Save the main for circular replication identification
        mains_found.append(main)

        # For each subordinate, print the graph link
        subordinates = topology_list[1]
        stop = len(subordinates)
        if stop > 0:
            # Level 0 is always the first main in the topology.
            if level == 0:
                print "%s (MASTER)" % main
            for i in range(0,stop):
                if len(subordinates[i]) == 1:
                    subordinate = subordinates[i][0]
                else:
                    subordinate = subordinates[i]
                new_preamble = preamble + "   "
                print new_preamble+"|"
                role = "(SLAVE"
                if not subordinate[1] == [] or subordinate[0] in mains_found:
                    role += " + MASTER"
                role += ")"
                print "%s+--- %s" % (new_preamble, subordinate[0]),
                
                if (subordinate[0] in mains_found):
                    print "<-->",
                else:
                    print "-",
                print "%s" % role
                    
                if not subordinate[1] == []:
                    if i < stop-1:
                        new_preamble += "|"
                    else:
                        new_preamble += " "
                    self.print_graph(subordinate, mains_found,
                                     level+1, new_preamble)


    def _get_row(self, topology_list):
        """Get a row (main, subordinate) for the topology map.
        
        topology_list[in]  The topology list
        
        Returns tuple - a row (main, subordinate)
        """
        new_row = []
        if len(topology_list) == 1:
            topology_list = topology_list[0]
        main = topology_list[0]
        subordinates = topology_list[1]
        for subordinate in subordinates:
            if len(subordinate) == 1:
                new_subordinate = subordinate[0]
            else:
                new_subordinate = subordinate
            new_row.append((main, new_subordinate[0]))
            new_row.extend(self._get_row(new_subordinate))
        return new_row


    def get_topology_map(self):
        """Get a list of the topology map suitable for export
        
        Returns list - a list of mains and their subordinates in two columns
        """
        # Get a row for the list
        # make a list from the topology
        main_subordinates = [self._get_row(row) for row in self.topology]
        return main_subordinates[0]

