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
This file contains the show replication topology functionality.
"""

import sys
from mysql.utilities.exception import UtilError

def show_topology(main_vals, options={}):
    """Show the subordinates/topology map for a main.

    This method find the subordinates attached to a server if it is a main. It
    can also discover the replication topology if the recurse option is
    True (default = False).
    
    It prints a tabular list of the main(s) and subordinates found. If the
    show_list option is True, it will also print a list of the output
    (default = False).
    
    main_vals[in]    Main connection in form user:passwd@host:port:socket
                       or login-path:port:socket.
    options[in]        dictionary of options
      recurse     If True, check each subordinate found for additional subordinates
                       Default = False
      prompt_user      If True, prompt user if subordinate connection fails with
                       main connection parameters
                       Default = False
      num_retries      Number of times to retry a failed connection attempt
                       Default = 0
      quiet            if True, print only the data
                       Default = False
      format           Format of list
                       Default = Grid
      width            width of report
                       Default = 75
      max_depth        maximum depth of recursive search
                       Default = None
    """
    from mysql.utilities.common.topology_map import TopologyMap
    
    topo = TopologyMap(main_vals, options)
    topo.generate_topology_map(options.get('max_depth', None))

    if not options.get("quiet", False) and topo.depth():
        print "\n# Replication Topology Graph"
   
    if not topo.subordinates_found():
        print "No subordinates found."
        
    topo.print_graph()
    print

    if options.get("show_list", False):
        from mysql.utilities.common.format import print_list
        
        # make a list from the topology
        topology_list = topo.get_topology_map()
        print_list(sys.stdout, options.get("format", "GRID"),
                   ["Main", "Subordinate"], topology_list, False, True)

