#!/usr/bin/env python
#
# Copyright (c) 2011, 2013, Oracle and/or its affiliates. All rights reserved.
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
This file contains the show replication topology utility. It is used to
find the subordinates for a given main and can traverse the list of subordinates
checking for additional main/subordinate connections.
"""

from mysql.utilities.common.tools import check_python_version

# Check Python version compatibility
check_python_version()

import os.path
import sys

from mysql.utilities.exception import UtilError
from mysql.utilities.common.options import setup_common_options
from mysql.utilities.common.options import parse_connection
from mysql.utilities.common.options import add_format_option
from mysql.utilities.command.show_rpl import show_topology
from mysql.utilities.exception import FormatError

# Constants
NAME = "MySQL Utilities - mysqlrplshow "
DESCRIPTION = "mysqlrplshow - show subordinates attached to a main"
USAGE = "%prog --main=root@localhost:3306 "

PRINT_WIDTH = 75

# Setup the command parser
parser = setup_common_options(os.path.basename(sys.argv[0]),
                              DESCRIPTION, USAGE, True, False)

# Setup utility-specific options:

# Connection information for the source server
parser.add_option("--main", action="store", dest="main",
                  type="string", default="root@localhost:3306",
                  help="connection information for main server in " + \
                  "the form: <user>[:<password>]@<host>[:<port>][:<socket>]"
                  " or <login-path>[:<port>][:<socket>].")

# Show graph option
parser.add_option("-l", "--show-list", action="store_true", dest="show_list",
                  help="print a list of the topology.", default=False)

# Output format
add_format_option(parser, "display the list in either grid (default), "
                  "tab, csv, or vertical format", "grid")

# Check subordinates option - if True, recurse subordinates from main to find
# additional main/subordinate connections
parser.add_option("-r", "--recurse", action="store_true",
                  dest="recurse",
                  help="traverse the list of subordinates to find additional "
                  "main/subordinate connections. User this option to map a "
                  "replication topology.", default=False)

# Add limit for recursion
parser.add_option("--max-depth", action="store", default=None, type="int",
                  help="limit the traversal to this depth. Valid only with "
                  "the --recurse option. Valid values are non-negative "
                  "integers.", dest="max_depth")

# Prompt for subordinate connections if default login/password fail
parser.add_option("-p", "--prompt", action="store_true", dest="prompt",
                  help="prompt for subordinate user and password if different from "
                  "main login.", default=False)

# Number of retries for failed subordinate login
parser.add_option("-n", "--num-retries", action="store", dest="num_retries",
                  type="int", help="number of retries allowed for failed "
                  "subordinate login attempt. Valid with --prompt only.",
                  default=0)

# Add quiet
parser.add_option("-q", "--quiet", action="store_true", dest="quiet",
                  help="turn off all messages for quiet execution.")

parser.add_option("--discover-subordinates-login", action="store", dest="discover",
                  default=None, type="string", help="at startup, query "
                  "main for all registered subordinates and use the user name "
                  "and password specified to connect. Supply the user and "
                  "password in the form <user>[:<password>] or <login-path>. "
                  "For example, --discover-subordinates-login=joe:secret will use "
                  "'joe' as the user and 'secret' as the password for each "
                  "discovered subordinate.")

# Now we process the rest of the arguments.
opt, args = parser.parse_args()

# Fail is --discover-subordinates-login not specified
if opt.discover is None:
    parser.error("The --discover-subordinates-login is required to test subordinate "
                 "connectivity.")

# Fail if recurse specified and max-depth is invalid
if opt.recurse and opt.max_depth is not None:
    if opt.max_depth < 0:
        parser.error("The --max-depth option needs to be >= 0.")

# Parse main connection values
try:
    m_values = parse_connection(opt.main, None, opt)
except FormatError:
    _, err, _ = sys.exc_info()
    parser.error("Main connection values invalid: %s." % err)
except UtilError:
    _, err, _ = sys.exc_info()
    parser.error("Main connection values invalid: %s." % err.errmsg)

# Create dictionary of options
options = {
    'quiet'          : opt.quiet,
    'prompt'         : opt.prompt,
    'num_retries'    : opt.num_retries,
    'recurse'        : opt.recurse,
    'show_list'      : opt.show_list,
    'format'         : opt.format,
    'max_depth'      : opt.max_depth,
    'discover'       : opt.discover,
}
  
try:
    res = show_topology(m_values, options)
    if res:
        sys.exit(1)
except UtilError:
    _, e, _ = sys.exc_info()
    print("ERROR: %s" % e.errmsg)
    sys.exit(1)

sys.exit()
