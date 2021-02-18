#
# Copyright (c) 2010, 2011 Oracle and/or its affiliates. All rights reserved.
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
This file contains the replicate utility. It is used to establish a
main/subordinate replication topology among two servers.
"""

import sys
from mysql.utilities.exception import UtilError

def setup_replication(main_vals, subordinate_vals, rpl_user,
                      options, test_db=None):
    """Setup replication among a main and a subordinate.
    
    main_vals[in]    Main connection in form user:passwd@host:port:sock
    subordinate_vals[in]     Subordinate connection in form user:passwd@host:port:sock
    rpl_user[in]       Replication user in the form user:passwd
    options[in]        dictionary of options (verbosity, quiet, pedantic)
    test_db[in]        Test replication using this database name (optional)
                       default = None
    """
    
    from mysql.utilities.common.server import connect_servers
    from mysql.utilities.common.replication import Replication
    
    verbosity = options.get("verbosity", 0)

    conn_options = {
        'src_name'  : "main",
        'dest_name' : 'subordinate',
        'version'   : "5.0.0",
        'unique'    : True,
    }
    servers = connect_servers(main_vals, subordinate_vals, conn_options)
    main = servers[0]
    subordinate = servers[1]
    
    rpl_options = options.copy()
    rpl_options['verbosity'] = verbosity > 0
    
    # Create an instance of the replication object
    rpl = Replication(main, subordinate, rpl_options)
    errors = rpl.check_server_ids()
    for error in errors:
        print error
            
    # Check for server_id uniqueness
    if verbosity > 0:
        print "# main id = %s" % main.get_server_id()
        print "#  subordinate id = %s" % subordinate.get_server_id()

    errors = rpl.check_server_uuids()
    for error in errors:
        print error
    
    # Check for server_uuid uniqueness
    if verbosity > 0:
        print "# main uuid = %s" % main.get_server_uuid()
        print "#  subordinate uuid = %s" % subordinate.get_server_uuid()

    # Check InnoDB compatibility
    if verbosity > 0:
        print "# Checking InnoDB statistics for type and version conflicts."

    errors = rpl.check_innodb_compatibility(options)
    for error in errors:
        print error
    
    # Checking storage engines                
    if verbosity > 0:
        print "# Checking storage engines..."
        
    errors = rpl.check_storage_engines(options)
    for error in errors:
        print error
            
    # Check main for binary logging
    print "# Checking for binary logging on main..."
    errors = rpl.check_main_binlog()
    if not errors == []:
        raise UtilError(errors[0])
        
    # Setup replication
    print "# Setting up replication..."
    if not rpl.setup(rpl_user, 10):
        raise UtilError("Cannot setup replication.")
        
    # Test the replication setup.
    if test_db:
        rpl.test(test_db, 10)
        
    print "# ...done."


