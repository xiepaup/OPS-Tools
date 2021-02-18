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
This module contains abstractions of MySQL replication functionality.
"""

import logging
import time

from mysql.utilities.common.lock import Lock
from mysql.utilities.common.my_print_defaults import MyDefaultsReader
from mysql.utilities.common.options import parse_connection
from mysql.utilities.common.options import parse_user_password
from mysql.utilities.common.replication import Main, Subordinate, Replication
from mysql.utilities.common.server import get_server_state

from mysql.utilities.exception import FormatError
from mysql.utilities.exception import UtilError
from mysql.utilities.exception import UtilRplError

_HEALTH_COLS = ["host", "port", "role", "state", "gtid_mode", "health"]
_HEALTH_DETAIL_COLS = ["version", "main_log_file", "main_log_pos",
                       "IO_Thread", "SQL_Thread", "Secs_Behind",
                       "Remaining_Delay", "IO_Error_Num", "IO_Error",
                       "SQL_Error_Num", "SQL_Error", "Trans_Behind"]

_GTID_EXECUTED = "SELECT @@GLOBAL.GTID_EXECUTED"
_GTID_WAIT = "SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS('%s', %s)"

_UPDATE_RPL_USER_QUERY = ('UPDATE mysql.user '
                          'SET password = PASSWORD("%s")'
                          'where user ="%s";')

def parse_failover_connections(options):
    """Parse the --main, --subordinates, and --candidates options

    This method returns a tuple with server connection dictionaries for
    the main, subordinates, and candidates lists.

    If no main, will return (None, ...) for main element.
    If no subordinates, will return (..., [], ...) for subordinates element.
    If no canidates, will return (..., ..., []) for canidates element.

    Will raise error if cannot parse connection options.

    options[in]        options from parser

    Returns tuple - (main, subordinates, candidates) dictionaries
    """
    # Create a basic configuration reader, without looking for the tool
    # my_print_defaults to avoid raising exceptions. This is used for
    # optimization purposes, to reuse data and avoid repeating the execution of
    # some methods in the parse_connection method (e.g. searching for
    # my_print_defaults).
    config_reader = MyDefaultsReader(options, False)

    if options.main:
        try:
            main_val = parse_connection(options.main, config_reader,
                                          options)
        except FormatError as err:
            msg = ("Main connection values invalid or cannot be parsed: %s "
                   "(%s)." % (options.main, err))
            raise UtilRplError(msg)
        except UtilError as err:
            msg = ("Main connection values invalid or cannot be parsed: %s "
                   "(using login-path authentication: %s)" % (options.main,
                                                              err.errmsg))
            raise UtilRplError(msg)
    else:
        main_val = None

    subordinates_val = []
    if options.subordinates:
        subordinates = options.subordinates.split(",")
        for subordinate in subordinates:
            try:
                s_values = parse_connection(subordinate, config_reader, options)
                subordinates_val.append(s_values)
            except FormatError as err:
                msg = ("Subordinate connection values invalid or cannot be parsed: "
                       "%s (%s)" % (subordinate, err))
                raise UtilRplError(msg)
            except UtilError as err:
                msg = ("Subordinate connection values invalid or cannot be parsed: "
                       "%s (%s)" % (subordinate, err.errmsg))
                raise UtilRplError(msg)

    candidates_val = []
    if options.candidates:
        candidates = options.candidates.split(",")
        for subordinate in candidates:
            try:
                s_values = parse_connection(subordinate, config_reader, options)
                candidates_val.append(s_values)
            except FormatError as err:
                msg = "Candidate connection values invalid or " + \
                      "cannot be parsed: %s (%s)" % (subordinate, err)
                raise UtilRplError(msg)
            except UtilError as err:
                msg = ("Candidate connection values invalid or cannot be "
                       "parsed: %s (%s)" % (subordinate, err.errmsg))
                raise UtilRplError(msg)

    return (main_val, subordinates_val, candidates_val)


class Topology(Replication):
    """The Topology class supports administrative operations for an existing
    main-to-many subordinate topology. It has the following capabilities:

        - determine the health of the topology
        - discover subordinates connected to the main provided they have
          --report-host and --report-port specified
        - switchover from main to a candidate subordinate
        - demote the main to a subordinate in the topology
        - perform best subordinate election
        - failover to a specific subordinate or best of subordinates available

    Notes:

        - the switchover and demote methods work with versions prior to and
          after 5.6.5.
        - failover and best subordinate election require version 5.6.5 and later
          and GTID_MODE=ON.

    """

    def __init__(self, main_vals, subordinate_vals, options={},
                 skip_conn_err=False):
        """Constructor

        The subordinates parameter requires a dictionary in the form:

        main_vals[in]    main server connection dictionary
        subordinate_vals[in]     list of subordinate server connection dictionaries
        options[in]        options dictionary
          verbose          print extra data during operations (optional)
                           Default = False
          ping             maximum number of seconds to ping
                           Default = 3
          max_delay        maximum delay in seconds subordinate and be behind
                           main and still be 'Ok'. Default = 0
          max_position     maximum position subordinate can be behind main's
                           binlog and still be 'Ok'. Default = 0
        skip_conn_err[in]  if True, do not fail on connection failure
                           Default = True
        """
        # Get options needed
        self.options = options
        self.verbosity = options.get("verbosity", 0)
        self.verbose = self.verbosity > 0
        self.quiet = self.options.get("quiet", False)
        self.pingtime = self.options.get("ping", 3)
        self.max_delay = self.options.get("max_delay", 0)
        self.max_pos = self.options.get("max_position", 0)
        self.force = self.options.get("force", False)
        self.before_script = self.options.get("before", None)
        self.after_script = self.options.get("after", None)
        self.timeout = int(self.options.get("timeout", 300))
        self.logging = self.options.get("logging", False)
        self.rpl_user = self.options.get("rpl_user", None)

        # Attempt to connect to all servers
        self.main, self.subordinates = self._connect_to_servers(main_vals,
                                                            subordinate_vals,
                                                            self.options,
                                                            skip_conn_err)
        self.discover_subordinates()


    def _report(self, message, level=logging.INFO, print_msg=True):
        """Log message if logging is on

        This method will log the message presented if the log is turned on.
        Specifically, if options['log_file'] is not None. It will also
        print the message to stdout.

        message[in]    message to be printed
        level[in]      level of message to log. Default = INFO
        print_msg[in]  if True, print the message to stdout. Default = True
        """
        # First, print the message.
        if print_msg and not self.quiet:
            print message
        # Now log message if logging turned on
        if self.logging:
            logging.log(int(level), message.strip("#").strip(' '))


    def _connect_to_servers(self, main_vals, subordinate_vals, options,
                            skip_conn_err=True):
        """Connect to the main and one or more subordinates

        This method will attempt to connect to the main and subordinates provided.
        For subordinates, if the --force option is specified, it will skip subordinates
        that cannot be reached setting the subordinate dictionary to None
        in the list instead of a Subordinate class instance.

        The dictionary of the list of subordinates returns is as follows.

        subordinate_dict = {
          'host'     : # host name for subordinate
          'port'     : # port for subordinate
          'instance' : Subordinate class instance or None if cannot connect
        }

        main_vals[in]    main server connection dictionary
        subordinate_vals[in]     list of subordinate server connection dictionaries
        options[in]        options dictionary
          verbose          print extra data during operations (optional)
                           Default = False
          ping             maximum number of seconds to ping
                           Default = 3
          max_delay        maximum delay in seconds subordinate and be behind
                           main and still be 'Ok'. Default = 0
          max_position     maximum position subordinate can be behind main's
                           binlog and still be 'Ok'. Default = 0
        skip_conn_err[in]  if True, do not fail on connection failure
                           Default = True

        Returns tuple - main instance, list of dictionary subordinate instances
        """
        from mysql.utilities.common.server import get_server

        main = None
        subordinates = []

        # attempt to connect to the main
        if main_vals:
            main = get_server('main', main_vals, True)

        for subordinate_val in subordinate_vals:
            host = subordinate_val['host']
            port = subordinate_val['port']
            try:
                subordinate = get_server('subordinate', subordinate_val, True)
            except:
                msg = "Cannot connect to subordinate %s:%s as user '%s'." % \
                      (host, port, subordinate_val['user'])
                if skip_conn_err:
                    if self.verbose:
                        self._report("# ERROR: %s" % msg, logging.ERROR)
                    subordinate = None
                else:
                    raise UtilRplError(msg)
            subordinate_dict = {
              'host'     : host,      # host name for subordinate
              'port'     : port,      # port for subordinate
              'instance' : subordinate,     # Subordinate class instance or None
            }
            subordinates.append(subordinate_dict)

        return (main, subordinates)


    def _is_connected(self):
        """Check to see if all servers are connected.

        Method will skip any subordinates that do not have an instance (offline)
        but requires the main be instantiated and connected.

        The method will also skip the checks altogether if self.force is
        specified.

        Returns bool - True if all connected or self.force is specified.
        """
        # Skip check if --force specified.
        if self.force:
            return True
        if self.main is None or not self.main.is_alive():
            return False
        for subordinate_dict in self.subordinates:
            subordinate = subordinate_dict['instance']
            if subordinate is not None and not subordinate.is_alive():
                return False

        return True


    def remove_discovered_subordinates(self):
        """Reset the subordinates list to the original list at instantiation

        This method is used in conjunction with discover_subordinates to remove
        any discovered subordinate from the subordinates list. Once this is done,
        a call to discover subordinates will rediscover the subordinates. This is helpful
        for when failover occurs and a discovered subordinate is used for the new
        main.
        """
        new_list = []
        for subordinate_dict in self.subordinates:
            if not subordinate_dict.get("discovered", False):
                new_list.append(subordinate_dict)
        self.subordinates = new_list


    def check_main_info_type(self, repo="TABLE"):
        """Check all subordinates for main_info_repository=repo

        repo[in]       value for main info = "TABLE" or "FILE"
                       Default is "TABLE"

        Returns bool - True if main_info_repository == repo
        """
        for subordinate_dict in self.subordinates:
            subordinate = subordinate_dict['instance']
            if subordinate is not None:
                res = subordinate.show_server_variable("main_info_repository")
                if not res or res[0][1].upper() != repo.upper():
                    return False
        return True


    def discover_subordinates(self, skip_conn_err=True):
        """Discover subordinates connected to the main

        Returns bool - True if new subordinates found
        """
        from mysql.utilities.common.replication import Subordinate

        # See if the user wants us to discover subordinates.
        discover = self.options.get("discover", None)
        if not discover or not self.main:
            return

        # Get user and password (support login-path)
        user, password = parse_user_password(discover, options=self.options)

        # Find discovered subordinates
        new_subordinates_found = False
        self._report("# Discovering subordinates for main at %s:%s" %
                     (self.main.host, self.main.port))
        discovered_subordinates = self.main.get_subordinates(user, password)
        for subordinate in discovered_subordinates:
            host, port = subordinate.split(":")
            self._report("Discovering subordinate at %s:%s" % (host, port),
                         logging.INFO, False)
            # Convert local IP to localhost
            if host == '127.0.0.1':
                host = 'localhost'
            # Skip hosts that are not registered properly
            if host == 'unknown host':
                continue
            # Check to see if the subordinate is already in the list
            else:
                found = False
                # Eliminate if already a subordinate
                for subordinate_dict in self.subordinates:
                    if subordinate_dict['host'] == host and \
                       int(subordinate_dict['port']) == int(port):
                        found = True
                        break
                if not found:
                    # Now we must attempt to connect to the subordinate.
                    conn_dict = {
                        'conn_info' : { 'user' : user, 'passwd' : password,
                                        'host' : host, 'port' : port,
                                        'socket' : None },
                        'role'      : subordinate,
                        'verbose'   : self.options.get("verbosity", 0) > 0,
                    }
                    subordinate_conn = Subordinate(conn_dict)
                    try:
                        subordinate_conn.connect()
                        # Skip discovered subordinates that are not connected
                        # to the main
                        if subordinate_conn.is_connected():
                            self.subordinates.append({ 'host' : host, 'port' : port,
                                                 'instance' : subordinate_conn,
                                                 'discovered' : True})
                            self._report("Found subordinate: %s:%s" %
                                         (host, port), logging.INFO, False)
                            new_subordinates_found = True
                        else:
                            self._report("Not found.", logging.WARN, False)
                    except UtilError, e:
                        msg = ("Cannot connect to subordinate %s:%s as user '%s'. "
                               % (host, port, user))
                        if skip_conn_err:
                            self._report(msg + e.errmsg, logging.WARN, False)
                        else:
                            raise UtilRplError(msg)

        return new_subordinates_found


    def _get_server_gtid_data(self, server, role):
        """Retrieve the GTID information from the server.

        This method builds a tuple of three lists corresponding to the three
        GTID lists (executed, purged, owned) retrievable via the global
        variables. It generates lists suitable for format and printing.

        role[in]           role of the server (used for report generation)

        Returns tuple - (executed list, purged list, owned list)
        """
        executed = []
        purged = []
        owned = []

        if server.supports_gtid() == "NO":
            return (executed, purged, owned)

        try:
            gtids = server.get_gtid_status()
        except UtilError, e:
            self._report("# ERROR retrieving GTID information: %s" % e.errmsg,
                         logging.ERROR)
            return None
        for gtid in gtids[0]:
            for row in gtid.split("\n"):
                if len(row):
                    executed.append((server.host, server.port, role,
                                     row.strip(",")))
        for gtid in gtids[1]:
            for row in gtid.split("\n"):
                if len(row):
                    purged.append((server.host, server.port, role,
                                   row.strip(",")))
        for gtid in gtids[2]:
            for row in gtid.split("\n"):
                if len(row):
                    owned.append((server.host, server.port, role,
                                  row.strip(",")))

        return (executed, purged, owned)


    def _check_switchover_prerequisites(self, candidate=None):
        """Check prerequisites for performing switchover

        This method checks the prerequisites for performing a switch from a
        main to a candidate subordinate.

        candidate[in]  if supplied, use this candidate instead of the
                       candidate supplied by the user. Must be instance of
                       Main class.

        Returns bool - True if success, raises error if not
        """
        if candidate is None:
            candidate = self.options.get("candidate", None)

        assert (candidate is not None), "A candidate server is required."
        assert (type(candidate) == Main), \
               "A candidate server must be a Main class instance."

        from mysql.utilities.common.replication import Subordinate

        # If main has GTID=ON, ensure all servers have GTID=ON
        gtid_enabled = self.main.supports_gtid() == "ON"
        if gtid_enabled:
            gtid_ok = True
            for subordinate_dict in self.subordinates:
                subordinate = subordinate_dict['instance']
                # skip dead or zombie subordinates
                if not subordinate or not subordinate.is_alive():
                    continue
                if subordinate.supports_gtid() != "ON":
                    gtid_ok = False
            if not gtid_ok:
                msg = "GTIDs are enabled on the main but not " + \
                      "on all of the subordinates."
                self._report(msg, logging.CRITICAL)
                raise UtilRplError(msg)
            elif self.verbose:
                self._report("# GTID_MODE=ON is set for all servers.")

        # Need Subordinate class instance to check main and replication user
        subordinate = self._change_role(candidate)

        # Check eligibility
        candidate_ok = self._check_candidate_eligibility(subordinate.host,
                                                         subordinate.port,
                                                         subordinate)
        if not candidate_ok[0]:
            # Create replication user if --force is specified.
            if self.force and candidate_ok[1] == "RPL_USER":
                user, passwd = subordinate.get_rpl_user()
                candidate.create_rpl_user(subordinate.host, subordinate.port,
                                          user, passwd)
            else:
                msg = candidate_ok[2]
                self._report(msg, logging.CRITICAL)
                raise UtilRplError(msg)

        return True


    def _get_rpl_user(self, server):
        """Get the replication user

        This method returns the user and password for the replication user
        as read from the Subordinate class.

        Returns tuple - user, password
        """
        # Get replication user from server if rpl_user not specified
        if self.rpl_user is None:
            subordinate = self._change_role(server)
            user, passwd = subordinate.get_rpl_user()
            return (user, passwd)

        # Get user and password (support login-path)
        user, passwd = parse_user_password(self.rpl_user, options=self.options)
        return (user, passwd)


    def run_script(self, script, quiet, options=[]):
        """Run an external script

        This method executes an external script. Result is checked for
        success (res == 0).

        script[in]     script to execute
        quiet[in]      if True, do not print messages
        options[in]    options for script
                       Default is none (no options)

        Returns bool - True = success
        """
        from mysql.utilities.common.tools import execute_script

        if script is None:
            return
        self._report("# Spawning external script.")
        res = execute_script(script, None, options, self.verbose)
        if res == 0:
            self._report("# Script completed Ok.")
        elif not quiet:
            self._report("ERROR: %s Script failed. Result = %s" %
                         (script, res), logging.ERROR)


    def _check_filters(self, main, subordinate):
        """Check filters to ensure they are compatible with the main.

        This method compares the binlog_do_db with the replicate_do_db and
        the binlog_ignore_db with the replicate_ignore_db on the main and
        subordinate to ensure the candidate subordinate is not filtering out different
        databases than the main.

        main[in]     the Main class instance of the main
        subordinate[in]      the Subordinate class instance of the subordinate

        Returns bool - True = filters agree
        """
        import sys
        from mysql.utilities.common.format import print_list

        m_filter = main.get_binlog_exceptions()
        s_filter = subordinate.get_binlog_exceptions()

        failed = False
        if len(m_filter) != len(s_filter):
            failed = True
        elif len(m_filter) == 0:
            return True
        elif m_filter[0][1] != s_filter[0][1] or \
             m_filter[0][2] != s_filter[0][2]:
            failed = True
        if failed:
            if self.verbose and not self.quiet:
                format = self.options.get("format", "GRID")
                rows = []
                if len(m_filter) == 0:
                    rows.append(('MASTER','',''))
                else:
                    rows.append(m_filter[0])
                if len(s_filter) == 0:
                    rows.append(('SLAVE','',''))
                else:
                    rows.append(s_filter[0])
                cols = ["role","*_do_db","*_ignore_db"]
                self._report("# Filter Check Failed.", logging.ERROR)
                print_list(sys.stdout, format, cols, rows)
            return False
        return True


    def _check_candidate_eligibility(self, host, port, subordinate,
                                     check_main=True, quiet=False):
        """Perform sanity checks for subordinate promotion

        This method checks the subordinate candidate to ensure it meets the
        requirements as follows.

        Check Name  Description
        ----------- --------------------------------------------------
        CONNECTED   subordinate is connected to the main
        GTID        subordinate has GTID_MODE = ON if main has GTID = ON
                    (GTID only)
        BEHIND      subordinate is not behind main
                    (non-GTID only)
        FILTER      subordinate's filters match the main
        RPL_USER    subordinate has rpl user defined
        BINLOG      subordinate must have binary logging enabled

        host[in]         host name for the subordinate (used for errors)
        port[in]         port for the subordinate (used for errors)
        subordinate[in]        Subordinate class instance of candidate
        check_main[in] if True, check that subordinate is connected to the main
        quiet[in]        if True, do not print messages even if verbosity > 0

        Returns tuple (bool, check_name, string) -
            (True, "", "") = candidate is viable,
            (False, check_name, error_message) = candidate is not viable
        """
        assert (subordinate is not None), "No Subordinate instance for eligibility check."

        gtid_enabled = subordinate.supports_gtid() == "ON"

        # Is subordinate connected to main?
        if self.verbose and not quiet:
            self._report("# Checking eligibility of subordinate %s:%s for "
                         "candidate." % (host, port))
        if check_main:
            msg = "#   Subordinate connected to main ... %s"
            if not subordinate.is_alive():
                if self.verbose and not quiet:
                    self._report(msg % "FAIL", logging.WARN)
                return (False, "CONNECTED",
                        "Connection to subordinate server lost.")
            if not subordinate.is_configured_for_main(self.main):
                if self.verbose and not quiet:
                    self._report(msg % "FAIL", logging.WARN)
                return (False, "CONNECTED",
                        "Candidate is not connected to the correct main.")
            if self.verbose and not quiet:
                self._report(msg % "Ok")

        # If GTID is active on main, ensure subordinate is on too.
        if gtid_enabled:
            msg = "#   GTID_MODE=ON ... %s"
            if subordinate.supports_gtid() != "ON":
                if self.verbose and not quiet:
                    self._report(msg % "FAIL", logging.WARN)
                return (False, "GTID",
                        "Subordinate does not have GTID support enabled.")
            if self.verbose and not quiet:
                self._report(msg % "Ok")

        # Check for subordinate behind main
        if not gtid_enabled and check_main:
            msg = "#   Subordinate not behind main ... %s"
            rpl = Replication(self.main, subordinate, self.options)
            errors = rpl.check_subordinate_delay()
            if errors != []:
                if self.verbose and not quiet:
                    self._report(msg % "FAIL", logging.WARN)
                return (False, "BEHIND", " ".join(errors))
            if self.verbose and not quiet:
                self._report(msg % "Ok")

        # Check filters unless force is on.
        if not self.force and check_main:
            msg = "#   Logging filters agree ... %s"
            if not self._check_filters(self.main, subordinate):
                if self.verbose and not quiet:
                    self._report(msg % "FAIL", logging.WARN)
                return (False, "FILTERS",
                        "Main and subordinate filters differ.")
            elif self.verbose and not quiet:
                self._report(msg % "Ok")

        # If no GTIDs, we need binary logging enabled on candidate.
        if not gtid_enabled:
            msg = "#   Binary logging turned on ... %s"
            if not subordinate.binlog_enabled():
                if self.verbose and not quiet:
                    self._report(msg % "FAIL", logging.WARN)
                return (False, "BINLOG",
                        "Binary logging is not enabled on the candidate.")
            if self.verbose and not quiet:
                self._report(msg % "Ok")

        # Check replication user - must exist with correct privileges
        try:
            user, passwd = subordinate.get_rpl_user()
        except UtilError, e:
            if not self.rpl_user:
                raise

            # Get user and password (support login-path)
            user, passwd = parse_user_password(self.rpl_user)

            # Make new main forget was a subordinate using subordinate methods
            s_candidate = self._change_role(subordinate, subordinate=False)
            res = s_candidate.get_rpl_users()
            l = len(res)
            user, host, passwd = res[l-1]
            #raise

        msg = "#   Replication user exists ... %s"
        if user is None or subordinate.check_rpl_user(user, subordinate.host) != []:
            if not self.force:
                if self.verbose and not quiet:
                    self._report(msg % "FAIL", logging.WARN)
                return (False, "RPL_USER",
                        "Candidate subordinate is missing replication user.")
            else:
                self._report("Replication user not found but --force used.",
                             logging.WARN)
        elif self.verbose and not quiet:
            self._report(msg % "Ok")

        return (True, "", "")


    def read_all_retrieved_gtids(self, subordinate):
        """Ensure any GTIDS in relay log are read

        This method iterates over all subordinates ensuring any events read from
        the main but not executed (read) from the relay log are read.

        This step is necessary for failover to ensure all transactions are
        applied to all subordinates before the new main is selected.

        subordinate[in]       Server instance of the subordinate
        """
        # skip dead or zombie subordinates
        if subordinate is None or not subordinate.is_alive():
            return
        gtids = subordinate.get_retrieved_gtid_set()
        if gtids:
            if self.verbose and not self.quiet:
                self._report("# Reading events in relay log for subordinate "
                             "%s:%s" % (subordinate.host, subordinate.port))
            try:
                res = subordinate.exec_query(_GTID_WAIT % (gtids.strip(','),
                                                     self.timeout))
            except UtilRplError as err:
                raise UtilRplError("Error executing %s: %s" %
                                   ((_GTID_WAIT % (gtids.strip(','),
                                                   self.timeout)), err.errmsg))


    def _has_missing_transactions(self, candidate, subordinate):
        """Determine if there are transactions on the subordinate not on candidate

        This method uses the function gtid_subset() to determine if there are
        GTIDs (transactions) on the subordinate that are not on the candidate.

        Return code fopr query should be 0 when there are missing
        transactions, 1 if not, and -1 if there is a non-numeric result
        code generated.

        candidate[in]   Server instance of candidate (new main)
        subordinate[in]       Server instance of subordinate to check

        Returns boolean - True if there are transactions else False
        """
        subordinate_exec_gtids = subordinate.get_executed_gtid_set()
        subordinate_retrieved_gtids = subordinate.get_retrieved_gtid_set()
        cand_subordinate = self._change_role(candidate)
        candidate_exec_gtids = cand_subordinate.get_executed_gtid_set()
        
        proceed = subordinate_exec_gtids is not None and len(subordinate_exec_gtids) >=1
        result_code = -1
        
        if proceed:
            subordinate_gtids = ",".join([subordinate_exec_gtids.strip(","),
                                    subordinate_retrieved_gtids.strip(",")])
            res = subordinate.exec_query("SELECT gtid_subset('%s', '%s')" %
                                   (subordinate_gtids, candidate_exec_gtids.strip(",")))
            if res and res[0][0].isdigit():
                result_code = int(res[0][0])
    
            if self.verbose and not self.quiet:
                if result_code != 1:
                    self._report("# Missing transactions found on %s:%s. "
                                 "SELECT gtid_subset() = %s" %
                                 (subordinate.host, subordinate.port, result_code))
                else:
                    self._report("# No missing transactions found on %s:%s. "
                                 "Skipping connection of candidate as subordinate." %
                                 (subordinate.host, subordinate.port))

        return result_code != 1


    def _prepare_candidate_for_failover(self, candidate, user, passwd=""):
        """Prepare candidate subordinate for subordinate promotion (in failover)

        This method uses the candidate subordinate specified and connects it to
        each subordinate in the topology performing a GTID_SUBSET query to wait
        for the candidate (acting as a subordinate) to catch up. This ensures
        the candidate is now the 'best' or 'most up-to-date' subordinate in the
        topology.

        Method works only for GTID-enabled candidate servers.

        candidate[in]  Subordinate class instance of candidate
        user[in]       replication user
        passwd[in]     replication user password

        Returns bool - True if successful,
                       raises exception if failure and forst is False
        """

        assert (candidate is not None), "Candidate must be a Subordinate instance."

        if candidate.supports_gtid() != "ON":
            msg = "Candidate does not have GTID turned on or " + \
                  "does not support GTIDs."
            self._report(msg, logging.CRITICAL)
            raise UtilRplError(msg)

        lock_options = {
            'locking'   : 'flush',
            'verbosity' : 3 if self.verbose else self.verbosity,
            'silent'    : self.quiet,
            'rpl_mode'  : "main",
        }

        hostport = "%s:%s" % (candidate.host, candidate.port)
        for subordinate_dict in self.subordinates:
            subset_error = False
            subset_warning = False
            s_host = subordinate_dict['host']
            s_port = subordinate_dict['port']
            catchup_msg = "Candidate unable to resolve missing " + \
                          "GTIDs with subordinate %s:%s." % (s_host, s_port)

            temp_main = subordinate_dict['instance']

            # skip dead or zombie subordinates
            if temp_main is None or not temp_main.is_alive():
                continue

            # Gather retrieved_gtid_set to execute all events on subordinates still
            # in the subordinate's relay log
            self.read_all_retrieved_gtids(temp_main)

            # Sanity check: ensure candidate and subordinate are not the same.
            if candidate.is_alias(s_host) and \
               int(s_port) == int(candidate.port):
                continue

            # Check for missing transactions. No need to connect to subordinate if
            # there are no transactions (GTIDs) to retrieve
            if not self._has_missing_transactions(candidate, temp_main):
                continue

            res = candidate.stop()
            if res is None or res != () and not self.quiet:
                self._report("Candidate %s:%s failed to stop." %
                             (hostport, res[0]))

            # Block writes to subordinate (temp_main)
            lock_ftwrl = Lock(temp_main, [], lock_options)
            temp_main.set_read_only(True)

            # Connect candidate to subordinate as its temp_main
            if self.verbose and not self.quiet:
                self._report("# Connecting candidate to %s:%s as a temporary "
                             "subordinate to retrieve unprocessed GTIDs." %
                             (s_host, s_port))

            if not candidate.switch_main(temp_main, user, passwd, False,
                                           None, None,
                                           self.verbose and not self.quiet):
                msg = "Cannot switch candidate to subordinate for " + \
                      "subordinate promotion process."
                self._report(msg, logging.CRITICAL)
                raise UtilRplError(msg)

            # Unblock writes to subordinate (temp_main).
            temp_main.set_read_only(False)
            lock_ftwrl.unlock()

            res = candidate.start()
            candidate.exec_query("COMMIT")

            if res is None or res != () and not self.quiet:
                self._report("Candidate %s:%s failed to start." %
                             (hostport, res[0]))

            if self.verbose and not self.quiet:
                self._report("# Waiting for candidate to catch up to subordinate "
                             "%s:%s." % (s_host, s_port))
            temp_main_gtid = temp_main.exec_query(_GTID_EXECUTED)
            candidate.wait_for_subordinate_gtid(temp_main_gtid, self.timeout,
                                          self.verbose and not self.quiet)

            # Disconnect candidate from subordinate (temp_main)
            candidate.stop()

        return True


    def _check_all_subordinates(self, new_main):
        """Check all subordinates for errors.

        Check each subordinate's status for errors during replication. If errors are
        found, they are printed as warning statements to stdout.

        new_main[in] the new main in Main class instance
        """
        subordinate_errors = []
        for subordinate_dict in self.subordinates:
            subordinate = subordinate_dict['instance']
            # skip dead or zombie subordinates
            if subordinate is None or not subordinate.is_alive():
                continue
            rpl = Replication(new_main, subordinate, self.options)
            # Use timeout to check subordinate status
            iteration = 0
            subordinate_ok = True
            while iteration < int(self.timeout):
                res = rpl.check_subordinate_connection()
                if not res and iteration >= self.timeout:
                    subordinate_error = None
                    if self.verbose:
                        res = subordinate.get_io_error()
                        subordinate_error = "%s:%s" % (res[1], res[2])
                    subordinate_errors.append((subordinate_dict['host'],
                                         subordinate_dict['port'],
                                         subordinate_error))
                    subordinate_ok = False
                    if self.verbose and not self.quiet:
                        self._report("# %s:%s status: FAIL " %
                                     (subordinate_dict['host'],
                                      subordinate_dict['port']), logging.WARN)
                elif res:
                    iteration = int(self.timeout) + 1
                else:
                    time.sleep(1)
                    iteration += 1
            if subordinate_ok and self.verbose and not self.quiet:
                self._report("# %s:%s status: Ok " % (subordinate_dict['host'],
                             subordinate_dict['port']))

        if len(subordinate_errors) > 0:
            self._report("WARNING - The following subordinates failed to connect to "
                         "the new main:", logging.WARN)
            for error in subordinate_errors:
                self._report("  - %s:%s" % (error[0], error[1]), logging.WARN)
                if self.verbose and error[2] is not None:
                    self._report(error[2], logging.WARN)
                else:
                    print
            return False

        return True


    def _remove_subordinate(self, subordinate):
        """Remove a subordinate from the subordinates dictionary list

        subordinate[in]      the dictionary for the subordinate to remove
        """
        i = 0
        for subordinate_dict in self.subordinates:
            if subordinate_dict['host'] == subordinate['host'] and \
               int(subordinate_dict['port']) == int(subordinate['port']):
                # Disconnect to satisfy new server restrictions on termination
                if self.subordinates[i] is not None and self.subordinates[i]['instance'] is not None:
                    self.subordinates[i]['instance'].disconnect()
                self.subordinates.pop(i)
                break
            i += 1


    def gtid_enabled(self):
        """Check if topology has GTID turned on.

        Returns bool - True = GTID_MODE=ON.
        """
        if self.main is not None:
            return self.main.supports_gtid() == "ON"
        for subordinate_dict in self.subordinates:
            subordinate = subordinate_dict['instance']
            # skip dead or zombie subordinates
            if subordinate is None or not subordinate.is_alive():
                continue
            return subordinate.supports_gtid() == "ON"
        return False


    def get_health(self):
        """Retrieve the replication health for the main and subordinates.

        This method will retrieve the replication health of the topology. This
        includes the following for each server.

          - host       : host name
          - port       : connection port
          - role       : "MASTER" or "SLAVE"
          - state      : UP = connected, WARN = cannot connect but can ping,
                         DOWN = cannot connect nor ping
          - gtid       : ON = gtid supported and turned on, OFF = supported
                         but not enabled, NO = not supported
          - rpl_health : (main) binlog enabled,
                         (subordinate) IO tread is running, SQL thread is running,
                         no errors, subordinate delay < max_delay,
                         read log pos + max_position < main's log position
                         Note: Will show 'ERROR' if there are multiple
                         errors encountered otherwise will display the
                         health check that failed.

        If verbosity is set, it will show the following additional information.

          (main)
            - server version, binary log file, position

          (subordinates)
            - server version, main's binary log file, main's log position,
              IO_Thread, SQL_Thread, Secs_Behind, Remaining_Delay,
              IO_Error_Num, IO_Error

        Returns tuple - (columns, rows)
        """
        assert (self.main is not None), "No main or connection failed."

        import operator

        # Get main health
        rpl_health = self.main.check_rpl_health()
        self._report("# Getting health for main: %s:%s." %
                     (self.main.host, self.main.port), logging.INFO, False)
        have_gtid = self.main.supports_gtid()
        rows = []
        main_data = [
            self.main.host,
            self.main.port,
            "MASTER",
            get_server_state(self.main, self.main.host, self.pingtime,
                             self.verbosity > 0),
            have_gtid,
            "OK" if rpl_health[0] else ", ".join(rpl_health[1]),
        ]

        m_status = self.main.get_status()
        if len(m_status):
            main_log, main_log_pos = m_status[0][0:2]
        else:
            main_log = None
            main_log_pos = 0

        columns = []
        columns.extend(_HEALTH_COLS)
        # Show additional details if verbosity turned on
        if self.verbosity > 0:
            columns.extend(_HEALTH_DETAIL_COLS)
            main_data.extend([self.main.get_version(), main_log,
                                main_log_pos, "","","","","","","","",""])

        rows.append(main_data)

        subordinate_rows = []
        # Get the health of the subordinates
        if have_gtid == "ON":
            main_gtids = self.main.exec_query(_GTID_EXECUTED)
        for subordinate_dict in self.subordinates:
            host = subordinate_dict['host']
            port = subordinate_dict['port']
            subordinate = subordinate_dict['instance']
            if subordinate is None:
                rpl_health = (False, ["Cannot connect to subordinate."])
            elif not subordinate.is_alive():
                # Attempt to reconnect to the database server.
                try:
                    subordinate.connect()
                    # Connection succeeded.
                    if not subordinate.is_configured_for_main(self.main):
                        rpl_health = (False,
                                      ["Subordinate is not connected to main."])
                        subordinate = None
                except UtilError:
                    # Connection failed.
                    rpl_health = (False, ["Subordinate is not alive."])
                    subordinate = None
            elif not subordinate.is_configured_for_main(self.main):
                rpl_health = (False, ["Subordinate is not connected to main."])
                subordinate = None

            if subordinate is not None:
                rpl_health = subordinate.check_rpl_health(self.main,
                                                    main_log, main_log_pos,
                                                    self.max_delay,
                                                    self.max_pos,
                                                    self.verbosity)

                # Now, see if filters are in compliance
                if not self._check_filters(self.main, subordinate):
                    if rpl_health[0]:
                        errors = rpl_health[1]
                        errors.append("Binary log and Relay log filters differ.")
                        rpl_health = (False, errors)

            subordinate_data = [
                host,
                port,
                "SLAVE",
                get_server_state(subordinate, host, self.pingtime,
                                 self.verbosity > 0),
                " " if subordinate is None else subordinate.supports_gtid(),
                "OK" if rpl_health[0] else ", ".join(rpl_health[1]),
            ]

            # Show additional details if verbosity turned on
            if self.verbosity > 0:
                if subordinate is None:
                    subordinate_data.extend([""]*13)
                else:
                    subordinate_data.append(subordinate.get_version())
                    res = subordinate.get_rpl_details()
                    if res is not None:
                        subordinate_data.extend(res)
                        if have_gtid == "ON":
                            gtid_behind = subordinate.num_gtid_behind(main_gtids)
                            subordinate_data.extend([gtid_behind])
                        else:
                            subordinate_data.extend([""])
                    else:
                        subordinate_data.extend([""]*13)

            subordinate_rows.append(subordinate_data)

        # order the subordinates
        subordinate_rows.sort(key=operator.itemgetter(0, 1))
        rows.extend(subordinate_rows)

        return (columns, rows)


    def get_server_uuids(self):
        """Return a list of the server's uuids.

        Returns list of tuples = (host, port, role, uuid)
        """
        # Get the main's uuid
        uuids = []
        uuids.append((self.main.host, self.main.port, "MASTER",
                      self.main.get_uuid()))
        for subordinate_dict in self.subordinates:
            uuids.append((subordinate_dict['host'], subordinate_dict['port'], "SLAVE",
                          subordinate_dict['instance'].get_uuid()))
        return uuids


    def get_gtid_data(self):
        """Get the GTID information from the topology

        This method retrieves the executed, purged, and owned GTID lists from
        the servers in the topology. It arranges them into three lists and
        includes the host name, port, and role of each server.

        Returns tuple - lists for GTID data
        """
        executed = []
        purged = []
        owned = []

        gtid_data = self._get_server_gtid_data(self.main, "MASTER")
        if gtid_data is not None:
            executed.extend(gtid_data[0])
            purged.extend(gtid_data[1])
            owned.extend(gtid_data[2])

        for subordinate_dict in self.subordinates:
            subordinate = subordinate_dict['instance']
            if subordinate is not None:
                gtid_data = self._get_server_gtid_data(subordinate, "SLAVE")
                if gtid_data is not None:
                    executed.extend(gtid_data[0])
                    purged.extend(gtid_data[1])
                    owned.extend(gtid_data[2])

        return (executed, purged, owned)


    def check_privileges(self, failover=False):
        """Check privileges for the main and all known servers

        failover[in]   if True, check permissions for switchover and
                       failover commands. Default is False.

        Returns list - [(user, host)] if not enough permissions,
                       [] if no errors
        """
        from mysql.utilities.common.user import User

        servers = []
        errors = []

        # Collect all users first.
        if self.main is not None:
            servers.append(self.main)
            for subordinate_conn in self.subordinates:
                subordinate = subordinate_conn['instance']
                # A subordinate instance is None if the connection failed during the
                # creation of the topology. In this case ignore the subordinate.
                if subordinate is not None:
                    servers.append(subordinate)

        # If candidates were specified, check those too.
        candidates = self.options.get("candidates", None)
        candidate_subordinates = []
        if candidates:
            self._report("# Checking privileges on candidates.")
            for candidate in candidates:
                subordinate_dict = self.connect_candidate(candidate, False)
                if subordinate_dict is not None:
                    subordinate = subordinate_dict['instance']
                    if subordinate is not None:
                        servers.append(subordinate)
                        candidate_subordinates.append(subordinate)

        for server in servers:
            user_inst = User(server, "%s@%s" % (server.user, server.host))
            if not failover:
                if not user_inst.has_privilege("*", "*", "SUPER"):
                    errors.append((server.user, server.host))
            else:
                if not user_inst.has_privilege("*", "*", "SUPER") or \
                   not user_inst.has_privilege("*", "*", "GRANT") or \
                   not user_inst.has_privilege("*", "*", "REPLICATION SLAVE"):
                    errors.append((server.user, server.host))

        # Disconnect if we connected to any candidates
        for subordinate in candidate_subordinates:
            subordinate.disconnect()

        return errors


    def run_cmd_on_subordinates(self, command, quiet=False):
        """Run a command on a list of subordinates.

        This method will run one of the following subordinate commands.

          start - START SLAVE;
          stop  - STOP SLAVE;
          reset - STOP SLAVE; RESET SLAVE;

        command[in]        command to execute
        quiet[in]          If True, do not print messges
                           Default is False
        """

        assert (self.subordinates is not None), \
               "No subordinates specified or connections failed."

        self._report("# Performing %s on all subordinates." %
                     command.upper())

        for subordinate_dict in self.subordinates:
            hostport = "%s:%s" % (subordinate_dict['host'], subordinate_dict['port'])
            msg = "#   Executing %s on subordinate %s " % (command, hostport)
            subordinate = subordinate_dict['instance']
            # skip dead or zombie subordinates
            if subordinate is None or not subordinate.is_alive():
                message = "{0}WARN - cannot connect to subordinate".format(msg)
                self._report(message, logging.WARN)
            elif command == 'reset':
                if not subordinate.is_configured_for_main(self.main) and \
                   not quiet:
                    message = ("{0}WARN - subordinate is not configured with this "
                               "main").format(msg)
                    self._report(message, logging.WARN)
                res = subordinate.reset()
                if res is None or res != () and not quiet:
                    message = "{0}WARN - subordinate failed to reset".format(msg)
                    self._report(message, logging.WARN)
                elif not quiet:
                    self._report("{0}Ok".format(msg))
            elif command == 'start':
                if not subordinate.is_configured_for_main(self.main) and \
                   not quiet:
                    message = ("{0}WARN - subordinate is not configured with this "
                               "main").format(msg)
                    self._report(message, logging.WARN)
                res = subordinate.start()
                if res is None or res != () and not quiet:
                    message = "{0}WARN - subordinate failed to start".format(msg)
                    self._report(message, logging.WARN)
                elif not quiet:
                    self._report("{0}Ok".format(msg))
            elif command == 'stop':
                if not subordinate.is_configured_for_main(self.main) and \
                   not quiet:
                    message = ("{0}WARN - subordinate is not configured with this "
                               "main").format(msg)
                    self._report(message, logging.WARN)
                elif not subordinate.is_connected() and not quiet:
                    message = ("{0}WARN - subordinate is not connected to "
                               "main").format(msg)
                    self._report(message, logging.WARN)
                res = subordinate.stop()
                if res is None or res != () and not quiet:
                    message = "{0}WARN - subordinate failed to stop".format(msg)
                    self._report(message, logging.WARN)
                elif not quiet:
                    self._report("{0}Ok".format(msg))


    def connect_candidate(self, candidate, main=True):
        """Parse and connect to the candidate

        This method parses the candidate string and returns a subordinate dictionary
        if main=False else returns a Main class instance.

        candidate[in]  candidate connection string
        main[in]     if True, make Main class instance

        Returns subordinate_dict or Main class instance
        """
        # Need instance of Main class for operation
        conn_dict = {
            'conn_info' : candidate,
            'quiet'     : True,
            'verbose'   : self.verbose,
        }
        if main:
            m_candidate = Main(conn_dict)
            m_candidate.connect()
            return m_candidate
        
        else:
            # Do not allow connection error to block
            # candidate selection
            subordinate_dict = None
            try:
                s_candidate = Subordinate(conn_dict)
                s_candidate.connect()
                subordinate_dict = {
                    'host'     : s_candidate.host,
                    'port'     : s_candidate.port,
                    'instance' : s_candidate,
                }
            except UtilError as e:
                self._report("ERROR: %s" % e.errmsg, logging.ERROR)
            return subordinate_dict

    def switchover(self, candidate):
        """Perform switchover from main to candidate subordinate.

        This method switches the role of main to a candidate subordinate. The
        candidate is checked for viability before the switch is made.

        If the user specified --demote-main, the method will make the old
        main a subordinate of the candidate.

        candidate[in]  the connection information for the --candidate option

        Return bool - True = success, raises exception on error
        """

        # Need instance of Main class for operation
        m_candidate = self.connect_candidate(candidate)

        # Switchover needs to succeed and prerequisites must be met else abort.
        self._report("# Checking candidate subordinate prerequisites.")
        try:
            self._check_switchover_prerequisites(m_candidate)
        except UtilError, e:
            self._report("ERROR: %s" % e.errmsg, logging.ERROR)
            if not self.force:
                return

        if (self.verbose and self.rpl_user):
            if self.check_main_info_type("TABLE"):
                msg = ("# When the main_info_repository variable is set to"
                       " TABLE, the --rpl-user option is ignored and the"
                       " existing replication user values are retained.")
                self._report(msg, logging.INFO)
                self.rpl_user = None
            else:
                msg = ("# When the main_info_repository variable is set to"
                       " FILE, the --rpl-user option may be used only if the"
                       " user specified matches what is shown in the SLAVE"
                       " STATUS output unless the --force option is used.")
                self._report(msg, logging.INFO)

        user, passwd = self._get_rpl_user(m_candidate)

        if not self.check_main_info_type("TABLE"):
            subordinate_candidate = self._change_role(m_candidate, subordinate=True)
            rpl_main_user = subordinate_candidate.get_rpl_main_user()

            if user != rpl_main_user and not self.force:
                msg = ("The replication user specified with --rpl-user does"
                       " not match the existing replication user values. Use"
                       " the --force option to use the replication user"
                       " specified with --rpl-user.")
                self._report("ERROR: %s" % msg, logging.ERROR)
                return
            self.main.exec_query(_UPDATE_RPL_USER_QUERY % (passwd, user))
            self.main.exec_query("FLUSH PRIVILEGES;")

        if self.verbose:
            self._report("# Creating replication user if it does not exist.")
        #user, passwd = self._get_rpl_user(m_candidate)
        res = m_candidate.create_rpl_user(m_candidate.host,
                                          m_candidate.port,
                                          user, passwd)

        # Call exec_before script - display output if verbose on
        self.run_script(self.before_script, False,
                        [self.main.host, self.main.port,
                         m_candidate.host, m_candidate.port])

        if self.verbose:
            self._report("# Blocking writes on main.")
        lock_options = {
            'locking'   : 'flush',
            'verbosity' : 3 if self.verbose else self.verbosity,
            'silent'    : self.quiet,
            'rpl_mode'  : "main",
        }
        lock_ftwrl = Lock(self.main, [], lock_options)
        self.main.set_read_only(True)

        # Wait for all subordinates to catch up.
        gtid_enabled = self.main.supports_gtid() == "ON"
        if gtid_enabled:
            main_gtid = self.main.exec_query(_GTID_EXECUTED)
        self._report("# Waiting for subordinates to catch up to old main.")
        for subordinate_dict in self.subordinates:
            main_info = self.main.get_status()[0]
            subordinate = subordinate_dict['instance']
            # skip dead or zombie subordinates
            if subordinate is None or not subordinate.is_alive():
                continue
            if gtid_enabled:
                res = subordinate.wait_for_subordinate_gtid(main_gtid, self.timeout,
                                            self.verbose and not self.quiet)
            else:
                res = subordinate.wait_for_subordinate(main_info[0], main_info[1],
                                           self.timeout)
            if not res:
                msg = "Subordinate %s:%s did not catch up to the main." % \
                      (subordinate_dict['host'], subordinate_dict['port'])
                if not self.force:
                    self._report(msg, logging.CRITICAL)
                    raise UtilRplError(msg)
                else:
                    self._report("# %s" % msg)

        # Stop all subordinates
        self._report("# Stopping subordinates.")
        self.run_cmd_on_subordinates("stop", not self.verbose)

        # Unblock main
        self.main.set_read_only(False)
        lock_ftwrl.unlock()

        # Make main a subordinate (if specified)
        if self.options.get("demote", False):
            self._report("# Demoting old main to be a subordinate to the "
                         "new main.")
            
            subordinate = self._change_role(self.main)
            subordinate.stop()
            
            subordinate_dict = {
              'host'     : self.main.host,  # host name for subordinate
              'port'     : self.main.port,  # port for subordinate
              'instance' : subordinate,             # Subordinate class instance
            }
            self.subordinates.append(subordinate_dict)

        # Move candidate subordinate to main position in lists
        self.main_vals = m_candidate.get_connection_values()
        self.main = m_candidate

        # Remove subordinate from list of subordinates
        self._remove_subordinate({'host':m_candidate.host,
                            'port':m_candidate.port,
                            'instance':m_candidate})

        # Make new main forget was an subordinate using subordinate methods
        s_candidate = self._change_role(m_candidate)
        s_candidate.reset_all()

        # Switch all subordinates to new main
        self._report("# Switching subordinates to new main.")
        new_main_info = m_candidate.get_status()[0]
        main_values = {
            'Main_Host'         : m_candidate.host,
            'Main_Port'         : m_candidate.port,
            'Main_User'         : user,
            'Main_Password'     : passwd,
            'Main_Log_File'     : new_main_info[0],
            'Read_Main_Log_Pos' : new_main_info[1],
        }
        for subordinate_dict in self.subordinates:
            if self.verbose:
                self._report("# Executing CHANGE MASTER on %s:%s." %
                             (subordinate_dict['host'], subordinate_dict['port']))
            subordinate = subordinate_dict['instance']
            # skip dead or zombie subordinates
            if subordinate is None or not subordinate.is_alive():
                continue
            change_main = subordinate.make_change_main(False, main_values)
            if self.verbose:
                self._report("# %s" % change_main)
            subordinate.exec_query(change_main)

        # Start all subordinates
        self._report("# Starting all subordinates.")
        self.run_cmd_on_subordinates("start", not self.verbose)

        # Call exec_after script - display output if verbose on
        self.run_script(self.after_script, False,
                        [self.main.host, self.main.port])

        # Check all subordinates for status, errors
        self._report("# Checking subordinates for errors.")
        if not self._check_all_subordinates(self.main):
            return False

        self._report("# Switchover complete.")

        return True


    def _change_role(self, server, subordinate=True):
        """Reverse role of Main and Subordinate classes

        This method can be used to get a Subordinate instance from a Main instance
        or a Main instance from a Subordinate instance.

        server[in]     Server class instance
        subordinate[in]      if True, create Subordinate class instance
                       Default is True

        Return Subordinate or Main instance
        """
        from mysql.utilities.common.server import get_connection_dictionary

        conn_dict = {
            'conn_info' : get_connection_dictionary(server),
            'verbose'   : self.verbose,
        }
        if subordinate and type(server) != Subordinate:
            subordinate_conn = Subordinate(conn_dict)
            subordinate_conn.connect()
            return subordinate_conn
        if not subordinate and type(server) != Main:
            main_conn = Main(conn_dict)
            main_conn.connect()
            return main_conn
        return server


    def find_best_subordinate(self, candidates=None, check_main=True,
                        strict=False):
        """Find the best subordinate

        This method checks each subordinate in the topology to determine if
        it is a viable subordinate for promotion. It returns the first subordinate
        that is determined to be eligible for promotion.

        The method uses the order of the subordinates in the topology as
        specified by the subordinates list to search for a best subordinate. If a
        candidate subordinate is provided, it is checked first.

        candidates[in]   list of candidate connection dictionaries
        check_main[in] if True, check that subordinate is connected to the main
                         Default is True
        strict[in]       if True, use only the candidate list for subordinate
                         election and fail if no candidates are viable.
                         Default = False

        Returns dictionary = (host, port, instance) for 'best' subordinate,
                             None = no candidate subordinates found
        """
        msg = "None of the candidates was the best subordinate."
        for candidate in candidates:
            subordinate_dict = self.connect_candidate(candidate, False)
            if subordinate_dict is not None:
                subordinate = subordinate_dict['instance']
                # Ignore dead or offline subordinates
                if subordinate is None or not subordinate.is_alive():
                    continue
                subordinate_ok = self._check_candidate_eligibility(subordinate.host, subordinate.port,
                                                             subordinate, check_main)
                if subordinate_ok is not None and subordinate_ok[0]:
                    return subordinate_dict
                else:
                    self._report("# Candidate %s:%s does not meet the requirements." %
                                 (subordinate.host, subordinate.port), logging.WARN)

        # If strict is on and we have found no viable candidates, return None
        if strict:
            self._report("ERROR: %s" % msg, logging.ERROR)
            return None

        if candidates is not None and len(candidates) > 0:
            self._report("WARNING: %s" % msg, logging.WARN)

        for subordinate_dict in self.subordinates:
            s_host = subordinate_dict['host']
            s_port = subordinate_dict['port']
            subordinate = subordinate_dict['instance']
            # skip dead or zombie subordinates
            if subordinate is None or not subordinate.is_alive():
                continue
            # Check eligibility
            try:
                subordinate_ok = self._check_candidate_eligibility(s_host, s_port,
                                                             subordinate,
                                                             check_main)
                if subordinate_ok is not None and subordinate_ok[0]:
                    return subordinate_dict
            except UtilError, e:
                self._report("# Subordinate eliminated due to error: %s" % e.errmsg,
                             logging.WARN)
                pass # Subordinate gone away, skip it.

        return None


    def failover(self, candidates, strict=False):
        """Perform failover to best subordinate in a GTID-enabled topology.

        This method performs a failover to one of the candidates specified. If
        no candidates are specified, the method will use the list of subordinates to
        choose a candidate. In either case, priority is given to the server
        listed first that meets the prerequisites - a sanity check to ensure if
        the candidate's GTID_MODE matches the other subordinates.

        In the event the candidates list is exhausted, it will use the subordinates
        list to find a candidate. If no servers are viable, the method aborts.

        If the strict parameter is True, the search is limited to the
        candidates list.

        Once a candidate is selected, the candidate is prepared to become the
        new main by collecting any missing GTIDs by being made a subordinate to
        each of the other subordinates.

        Once prepared, the before script is run to trigger applications,
        then all subordinates are connected to the new main. Once complete,
        all subordinates are started, the after script is run to trigger
        applications, and the subordinates are checked for errors.

        candidates[in] list of subordinate connection dictionary of candidate
        strict[in]     if True, use only the candidate list for subordinate
                       election and fail if no candidates are viable.
                       Default = False

        Returns bool - True if successful,
                       raises exception if failure and forst is False
        """
        from mysql.utilities.common.server import get_connection_dictionary

        # Get best subordinate from list of candidates
        new_main_dict = self.find_best_subordinate(candidates, False, strict)
        if new_main_dict is None:
            msg = "No candidate found for failover."
            self._report(msg, logging.CRITICAL)
            raise UtilRplError(msg)

        new_main = new_main_dict['instance']
        # All servers must have GTIDs match candidate
        gtid_mode = new_main.supports_gtid()
        if gtid_mode != "ON":
            msg = "Failover requires all servers support " + \
                   "global transaction ids and have GTID_MODE=ON"
            self._report(msg, logging.CRITICAL)
            raise UtilRplError(msg)

        for subordinate_dict in self.subordinates:
            # Ignore dead or offline subordinates
            subordinate = subordinate_dict['instance']
            # skip dead or zombie subordinates
            if subordinate is None or not subordinate.is_alive():
                continue
            if subordinate.supports_gtid() != gtid_mode:
                msg = "Cannot perform failover unless all " + \
                      "subordinates support GTIDs and GTID_MODE=ON"
                self._report(msg, logging.CRITICAL)
                raise UtilRplError(msg)

        # We must also ensure the new main and all remaining subordinates
        # have the latest GTID support.
        new_main.check_gtid_version()
        for subordinate_dict in self.subordinates:
            # Ignore dead or offline subordinates
            subordinate = subordinate_dict['instance']
            # skip dead or zombie subordinates
            if subordinate is None or not subordinate.is_alive():
                continue
            subordinate.check_gtid_version()

        host = new_main_dict['host']
        port = new_main_dict['port']
        # Use try block in case main class has gone away.
        try:
            old_host = self.main.host
            old_port = self.main.port
        except:
            pass
            old_host = "UNKNOWN"
            old_port = "UNKNOWN"

        self._report("# Candidate subordinate %s:%s will become the new main." %
                     (host, port))

        user, passwd = self._get_rpl_user(self._change_role(new_main))

        # Prepare candidate
        self._report("# Preparing candidate for failover.")
        self._prepare_candidate_for_failover(new_main, user, passwd)

        # Create replication user on candidate.
        self._report("# Creating replication user if it does not exist.")

        # Need Main class instance to check main and replication user
        self.main = self._change_role(new_main, False)
        res = self.main.create_rpl_user(host, port, user, passwd)

        # Call exec_before script - display output if verbose on
        self.run_script(self.before_script, False,
                        [old_host, old_port, host, port])

        # Stop all subordinates
        self._report("# Stopping subordinates.")
        self.run_cmd_on_subordinates("stop", not self.verbose)

        self._report("# Switching subordinates to new main.")
        for subordinate_dict in self.subordinates:
            subordinate = subordinate_dict['instance']
            # skip dead or zombie subordinates
            if subordinate is None or not subordinate.is_alive():
                continue
            subordinate.switch_main(self.main, user, passwd, False, None, None,
                                self.verbose and not self.quiet)

        # Take the server out of the list.
        self._remove_subordinate(new_main_dict)

        # Starting all subordinates
        self._report("# Starting subordinates.")
        self.run_cmd_on_subordinates("start", not self.verbose)

        # Call exec_after script - display output if verbose on
        self.run_script(self.after_script, False,
                        [old_host, old_port, host, port])

        # Check subordinates for errors
        self._report("# Checking subordinates for errors.")
        if not self._check_all_subordinates(self.main):
            return False

        self._report("# Failover complete.")

        return True
