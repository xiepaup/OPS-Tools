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

import os
import time

from mysql.utilities.common.options import parse_user_password
from mysql.utilities.common.server import Server
from mysql.utilities.exception import UtilError, UtilRplWarn, UtilRplError

_MASTER_INFO_COL = [
    'Main_Log_File', 'Read_Main_Log_Pos', 'Main_Host', 'Main_User',
    'Main_Password', 'Main_Port', 'Connect_Retry', 'Main_SSL_Allowed',
    'Main_SSL_CA_File', 'Main_SSL_CA_Path', 'Main_SSL_Cert',
    'Main_SSL_Cipher', 'Main_SSL_Key', 'Main_SSL_Verify_Server_Cert',
    'Heartbeat', 'Bind', 'Ignored_server_ids', 'Uuid', 'Retry_count',
    'SSL_CRL', 'SSL_CRL_Path', 'Enabled_auto_position',
]

_SLAVE_IO_STATE, _SLAVE_MASTER_HOST, _SLAVE_MASTER_USER, _SLAVE_MASTER_PORT, \
    _SLAVE_MASTER_LOG_FILE, _SLAVE_MASTER_LOG_FILE_POS, _SLAVE_IO_RUNNING, \
    _SLAVE_SQL_RUNNING, _SLAVE_DO_DB, _SLAVE_IGNORE_DB, _SLAVE_DELAY, \
    _SLAVE_REMAINING_DELAY, _SLAVE_IO_ERRORNO, _SLAVE_IO_ERROR, \
    _SLAVE_SQL_ERRORNO, _SLAVE_SQL_ERROR, _RETRIEVED_GTID_SET, \
    _EXECUTED_GTID_SET = \
    0, 1, 2, 3, 5, 6, 10, 11, 12, 13, 32, 33, 34, 35, 36, 37, 51, 52

_PRINT_WIDTH = 75

_MASTER_DO_DB, _MASTER_IGNORE_DB = 2, 3

_RPL_USER_QUERY = """
    SELECT user, host, password = "" as has_password
    FROM mysql.user
    WHERE repl_subordinate_priv = 'Y'
"""

_WARNING = "# WARNING: %s"
_MASTER_BINLOG = "Server '%s' does not have binary logging turned on."
_NO_RPL_USER = "No --rpl-user specified and multiple users found with " + \
               "replication privileges."
_RPL_USER_PASS = "No --rpl-user specified and the user found with " + \
                 "replication privileges requires a password."

_GTID_EXECUTED = "SELECT @@GLOBAL.GTID_EXECUTED"
_GTID_WAIT = "SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS('%s', %s)"

def _get_list(rows, cols):
    """Return a list of information in GRID format to stdout.

    rows[in]          rows of data
    cols[in]          column headings

    Returns list of strings
    """
    import StringIO
    from mysql.utilities.common.format import format_tabular_list

    ostream = StringIO.StringIO()
    format_tabular_list(ostream, cols, rows)
    return ostream.getvalue().splitlines()


def negotiate_rpl_connection(server, is_main=True, strict=True, options={}):
    """Determine replication connection

    This method attempts to determine if it is possible to build a CHANGE
    MASTER command based on the server passed. If it is possible, the method
    will return a CHANGE MASTER command. If there are errors and the strict
    option is turned on, it will throw errors if there is something missing.
    Otherwise, it will return the CHANGE MASTER command with warnings.

    If the server is a main, the following error checks will be performed.

      - if binary log is turned OFF, and strict = False, a warning message
        is added to the strings returned else an error is thrown

      - if the rpl_user option is missing, the method attempts to find a
        replication user. If more than one user is found or none are found, and
        strict = False, a warning message is added to the strings returned else
        an error is thrown

      - if a replication user is found but the user requires a password,
        the MASTER_USER and MASTER_PASSWORD options are commented out

    Note: the CHANGE MASTER command is formatted whereby each option is
          separated by a newline and indented two spaces

    Note: the make_change_main method does not support SSL connections

    server[in]        a Server class instance
    is_main[in]     if True, the server is acting as a main
                      Default = True
    strict[in]        if True, raise exception on errors
                      Default = True
    options[in]       replication options including rpl_user, quiet, multiline

    Returns list - strings containing the CHANGE MASTER command
    """

    rpl_mode = options.get("rpl_mode", "main")
    rpl_user = options.get("rpl_user", None)
    quiet = options.get("quiet", False)

    # Copy options and add connected server
    new_opts = options.copy()
    new_opts["conn_info"] = server

    main_values = {}
    change_main = []

    # If server is a main, perform error checking
    if is_main:
        main = Main(new_opts)
        main.connect()

        # Check main for binlog
        if not main.binlog_enabled():
            raise UtilError("Main must have binary logging turned on.")
        else:
            # Check rpl user
            if rpl_user is None and not quiet:
                # Try to find the replication user
                res = main.get_rpl_users()
                if len(res) > 1:
                    uname = ""
                    passwd = ""
                    # Throw error if strict but not for rpl_mode = both
                    if strict and not rpl_mode == 'both':
                        raise UtilRplError(_NO_RPL_USER)
                    else:
                        change_main.append(_WARNING % _NO_RPL_USER)
                else:
                    uname = res[0][0]
                    if res[0][2]:
                        # Throw error if strict but not for rpl_mode = both
                        if strict and not rpl_mode == 'both':
                            raise UtilRplError(_RPL_USER_PASS)
                        else:
                            change_main.append(_WARNING % _RPL_USER_PASS)
                    passwd = res[0][1]
            else:
                # Parse username and password (supports login-paths)
                uname, passwd = parse_user_password(rpl_user, options=options)
                if not passwd:
                    passwd = ''

                # Check replication user privileges
                errors = main.check_rpl_user(uname, main.host)
                if errors != []:
                    raise UtilError(errors[0])

            res = main.get_status()
            if not res:
               raise UtilError("Cannot retrieve main status.")

            # Need to get the main values for the make_change_main command
            main_values = {
                'Main_Host'          : main.host,
                'Main_Port'          : main.port,
                'Main_User'          : uname,
                'Main_Password'      : passwd,
                'Main_Log_File'      : res[0][0],
                'Read_Main_Log_Pos'  : res[0][1],
            }

    # Use subordinate class to get change main command
    subordinate = Subordinate(new_opts)
    subordinate.connect()
    cm_cmd = subordinate.make_change_main(False, main_values)

    if rpl_user is None and uname == "" and not quiet:
        cm_cmd = cm_cmd.replace("MASTER_PORT", "# MASTER_USER = '', "
                                "# MASTER_PASSWORD = '', MASTER_PORT")

    if options.get("multiline", False):
        cm_cmd = cm_cmd.replace(", ", ", \n  ") + ";"
        change_main.extend(cm_cmd.split("\n"))
    else:
        change_main.append(cm_cmd + ";")

    return change_main


class Replication(object):
    """
    The Replication class can be used to establish a replication connection
    between a main and a subordinate with the following utilities:

        - Create the replication user
        - Setup replication
        - Test prerequisites for replication
        - Conduct validation checks:
            - binlog
            - server ids
            - storage engine compatibility
            - innodb version compatibility
            - main binlog
            - lower case table name compatibility
            - subordinate connection to main
            - subordinate delay

    Replication prerequisite tests shall be constructed so that they return
    None if the check passes (no errors) or a list of strings containing the
    errors or warnings. They shall accept a dictionary of options set to
    options={}. This will allow for reduced code needed to call multiple tests.
    """

    def __init__(self, main, subordinate, options):
        """Constructor

        main[in]         Main Server object
        subordinate[in]          Subordinate Server object
        options[in]        Options for class
          verbose          print extra data during operations (optional)
                           default value = False
          main_log_file  main log file
                           default value = None
          main_log_pos   position in log file
                           default = -1 (no position specified)
          from_beginning   if True, start from beginning of logged events
                           default = False
        """
        self.verbosity = options.get("verbosity", 0)
        self.main_log_file = options.get("main_log_file", None)
        self.main_log_pos = options.get("main_log_pos", 0)
        self.from_beginning = options.get("from_beginning", False)
        self.main = main
        self.subordinate = subordinate
        self.replicating = False
        self.query_options = {
            'fetch' : False
        }


    def check_server_ids(self):
        """Check server ids on main and subordinate

        This method will check the server ids on the main and subordinate. It will
        raise exceptions for error conditions.

        Returns [] if compatible, list of errors if not compatible
        """
        main_server_id = self.main.get_server_id()
        subordinate_server_id = self.subordinate.get_server_id()
        if main_server_id == 0:
            raise UtilRplError("Main server_id is set to 0.")

        if subordinate_server_id == 0:
            raise UtilRplError("Subordinate server_id is set to 0.")

        # Check for server_id uniqueness
        if main_server_id == subordinate_server_id:
            raise UtilRplError("The subordinate's server_id is the same as the "
                                 "main.")

        return []


    def check_server_uuids(self):
        """Check UUIDs on main and subordinate

        This method will check the UUIDs on the main and subordinate. It will
        raise exceptions for error conditions.

        Returns [] if compatible or no UUIDs used, list of errors if not
        """
        main_uuid = self.main.get_uuid()
        subordinate_uuid = self.subordinate.get_uuid()

        # Check for both not supporting UUIDs.
        if main_uuid is None and subordinate_uuid is None:
            return []

        # Check for unbalanced servers - one with UUID, one without
        if main_uuid is None or subordinate_uuid is None:
            raise UtilRplError("%s does not support UUIDs." %
                               "Main" if main_uuid is None else "Subordinate")

        # Check for uuid uniqueness
        if main_uuid == subordinate_uuid:
            raise UtilRplError("The subordinate's UUID is the same as the "
                                 "main.")

        return []


    def check_innodb_compatibility(self, options):
        """Check InnoDB compatibility

        This method checks the main and subordinate to ensure they have compatible
        installations of InnoDB. It will print the InnoDB settings on the
        main and subordinate if quiet is not set. If pedantic is set, method
        will raise an error.

        options[in]   dictionary of options (verbose, pedantic)

        Returns [] if compatible, list of errors if not compatible
        """

        pedantic = options.get("pedantic", False)
        verbose = options.get("verbosity", 0) > 0

        errors = []

        main_innodb_stats = self.main.get_innodb_stats()
        subordinate_innodb_stats = self.subordinate.get_innodb_stats()

        if main_innodb_stats != subordinate_innodb_stats:
            if not pedantic:
                errors.append("WARNING: Innodb settings differ between main "
                              "and subordinate.")
            if verbose or pedantic:
                cols = ['type', 'plugin_version', 'plugin_type_version',
                        'have_innodb']
                rows = []
                rows.append(main_innodb_stats)
                errors.append("# Main's InnoDB Stats:")
                errors.extend(_get_list(rows, cols))
                rows = []
                rows.append(subordinate_innodb_stats)
                errors.append("# Subordinate's InnoDB Stats:")
                errors.extend(_get_list(rows, cols))
            if pedantic:
                for line in errors:
                    print line
                raise UtilRplError("Innodb settings differ between main "
                                     "and subordinate.")

        return errors


    def check_storage_engines(self, options):
        """Check compatibility of storage engines on main and subordinate

        This method checks that the main and subordinate have compatible storage
        engines. It will print the InnoDB settings on the main and subordinate if
        quiet is not set. If pedantic is set, method will raise an error.

        options[in]   dictionary of options (verbose, pedantic)

        Returns [] if compatible, list of errors if not compatible
        """

        pedantic = options.get("pedantic", False)
        verbose = options.get("verbosity", 0) > 0

        errors = []
        subordinate_engines = self.subordinate.get_storage_engines()
        results = self.main.check_storage_engines(subordinate_engines)
        if results[0] is not None or results[1] is not None:
            if not pedantic:
                errors.append("WARNING: The main and subordinate have differing "
                              "storage engine configurations!")
            if verbose or pedantic:
                cols = ['engine', 'support']
                if results[0] is not None:
                    errors.append("# Storage engine configuration on Main:")
                    errors.extend(_get_list(results[0], cols))
                if results[1] is not None:
                    errors.append("# Storage engine configuration on Subordinate:")
                    errors.extend(_get_list(results[1], cols))
            if pedantic:
                for line in errors:
                    print line
                raise UtilRplError("The main and subordinate have differing "
                                     "storage engine configurations!")

        return errors


    def check_main_binlog(self):
        """Check prerequisites for main for replication

        Returns [] if main ok, list of errors if binary logging turned off.
        """
        errors = []
        if not self.main.binlog_enabled():
            errors.append("Main must have binary logging turned on.")
        return errors


    def check_lctn(self):
        """Check lower_case_table_name setting

        Returns [] - no exceptions, list if exceptions found
        """
        errors = []
        subordinate_lctn = self.subordinate.get_lctn()
        main_lctn = self.main.get_lctn()
        if subordinate_lctn != main_lctn:
            return (main_lctn, subordinate_lctn)
        if subordinate_lctn == 1:
            msg = "WARNING: identifiers can have inconsistent case " + \
                  "when lower_case_table_names = 1 on the subordinate and " + \
                  "the main has a different value."
            errors.append(msg)

        return errors


    def get_binlog_exceptions(self):
        """Get any binary logging exceptions

        This method queries the main and subordinate status for the *-do-db and
        *-ignore-db settings. It returns the values of either of these for
        the main and subordinate.

        Returns [] - no exceptions, list if exceptions found
        """
        binlog_ex = []
        rows = []
        rows.extend(self.main.get_binlog_exceptions())
        rows.extend(self.subordinate.get_binlog_exceptions())
        if len(rows) > 0:
            cols = ['server', 'do_db', 'ignore_db']
            binlog_ex = _get_list(rows, cols)

        return binlog_ex


    def check_subordinate_connection(self):
        """Check to see if subordinate is connected to main

        This method will check the subordinate specified at instantiation to see if
        it is connected to the main specified. If the subordinate is connected
        to a different main, an error is returned. It will also raise an
        exception if the subordinate is stopped or if the server is not setup as a
        subordinate.

        Returns bool - True = subordinate connected to main
        """
        state = self.subordinate.get_io_running()
        if not state:
            raise UtilRplError("Subordinate is stopped.")
        if not self.subordinate.is_configured_for_main(self.main) or \
           state.upper() != "YES":
            return False
        return True


    def check_subordinate_delay(self):
        """Check to see if subordinate is behind main.

        This method checks subordinate_behind_main returning None if 0 or a
        message containing the value if non-zero. Also includes the subordinate's
        position as related to the main.

        Returns [] - no exceptions, list if exceptions found
        """
        m_log_file = None
        m_log_pos = 0
        errors = []
        res = self.main.get_status()
        if res != []:
            m_log_file = res[0][0]       # main's binlog file
            m_log_pos = res[0][1]        # main's binlog position
        else:
            raise UtilRplError("Cannot read main status.")
        delay_info = self.subordinate.get_delay()
        if delay_info is None:
            raise UtilRplError("The server specified as the subordinate is "
                                 "not configured as a replication subordinate.")


        state, sec_behind, delay_remaining, \
            read_log_file, read_log_pos = delay_info

        if not state:
            raise UtilRplError("Subordinate is stopped.")
        if delay_remaining is None: # if unknown, return the error
            errors.append("Cannot determine subordinate delay. Status: UNKNOWN.")
            return errors

        if sec_behind == 0:
            if m_log_file is not None and \
               (read_log_file != m_log_file or
                read_log_pos != m_log_pos):
                errors.append("Subordinate is behind main.")
                errors.append("Main binary log file = %s" % m_log_file)
                errors.append("Main binary log position = %s" % m_log_pos)
                errors.append("Subordinate is reading main binary log "
                              "file = %s" % read_log_file)
                errors.append("Subordinate is reading main binary log "
                              "position = %s" % read_log_pos)
            else:
                return errors
        else:
            errors.append("Subordinate is % seconds behind main." %
                          sec_behind)

        return errors


    def create_rpl_user(self, r_user, r_pass=None):
        """Create the replication user and grant privileges

        If the user exists, check privileges and add privileges as needed.
        Calls Main class method to execute.

        r_user[in]     user to create
        r_pass[in]     password for user to create (optional)

        Returns bool - True = success, False = errors
        """
        return self.main.create_rpl_user(self.subordinate.host, self.subordinate.port,
                                           r_user, r_pass, self.verbosity)


    def setup(self, rpl_user, num_tries):
        """Setup replication among a subordinate and main.

        Note: Must have connected to a main and subordinate before calling this
        method.

        rpl_user[in]       Replication user in form user:passwd
        num_tries[in]      Number of attempts to wait for subordinate synch

        Returns True if success, False if error
        """
        if self.main is None or self.subordinate is None:
            print "ERROR: Must connect to main and subordinate before " \
                  "calling replicate()"
            return False

        result = True

        # Parse user and password (support login-paths)
        r_user, r_pass = parse_user_password(rpl_user)

        # Check to see if rpl_user is present, else create her
        if not self.create_rpl_user(r_user, r_pass):
            return False

        # Read main log file information
        res = self.main.get_status()
        if not res:
            print "ERROR: Cannot retrieve main status."
            return False

        # If main log file, pos not specified, read main log file info
        read_main_info = False
        if self.main_log_file is None:
            res = self.main.get_status()
            if not res:
                print "ERROR: Cannot retrieve main status."
                return False

            read_main_info = True
            self.main_log_file = res[0][0]
            self.main_log_pos = res[0][1]
        else:
            # Check to make sure file is accessible and valid
            found = False
            res = self.main.get_binary_logs(self.query_options)
            for row in res:
                if row[0] == self.main_log_file:
                    found = True
                    break
            if not found:
                raise UtilError("Main binary log file not listed as a "
                                "valid binary log file on the main.")

        if self.main_log_file is None:
            raise UtilError("No main log file specified.")

        # Stop subordinate first
        res = self.subordinate.get_thread_status()
        if res is not None:
            if res[1] == "Yes" or res[2] == "Yes":
                res = self.subordinate.stop(self.query_options)

        # Connect subordinate to main
        if self.verbosity > 0:
            print "# Connecting subordinate to main..."
        main_values = {
            'Main_Host'          : self.main.host,
            'Main_Port'          : self.main.port,
            'Main_User'          : r_user,
            'Main_Password'      : r_pass,
            'Main_Log_File'      : self.main_log_file,
            'Read_Main_Log_Pos'  : self.main_log_pos,
        }
        change_main = self.subordinate.make_change_main(self.from_beginning,
                                                      main_values)
        res = self.subordinate.exec_query(change_main, self.query_options)
        if self.verbosity > 0:
            print "# %s" % change_main

        # Start subordinate
        if self.verbosity > 0:
            if not self.from_beginning:
                if read_main_info:
                    print "# Starting subordinate from main's last position..."
                else:
                    msg = "# Starting subordinate from main log file '%s'" % \
                          self.main_log_file
                    if self.main_log_pos >= 0:
                        msg += " using position %s" % self.main_log_pos
                    msg += "..."
                    print msg
            else:
                print "# Starting subordinate from the beginning..."
        res = self.subordinate.start(self.query_options)

        # Add commit because C/Py are auto_commit=0 by default
        self.subordinate.exec_query("COMMIT")

        # Check subordinate status
        i = 0
        while i < num_tries:
            time.sleep(1)
            res = self.subordinate.get_subordinates_errors()
            status = res[0]
            sql_running = res[4]
            if self.verbosity > 0:
                io_errorno = res[1]
                io_error = res[2]
                io_running = res[3]
                sql_errorno = res[5]
                sql_error = res[6]
                print "# IO status: %s" % status
                print "# IO thread running: %s" % io_running
                # if io_errorno = 0 and error = '' -> no error
                if not io_errorno and not io_error:
                    print "# IO error: None"
                else:
                    print "# IO error: %s:%s" % (io_errorno, io_error)
                # if io_errorno = 0 and error = '' -> no error
                print "# SQL thread running: %s" % sql_running
                if not sql_errorno and not sql_error:
                    print "# SQL error: None"
                else:
                    print "# SQL error: %s:%s" % (io_errorno, io_error)
            if status == "Waiting for main to send event" and sql_running:
                break
            elif not sql_running:
                if self.verbosity > 0:
                    print "# Retry to start the subordinate SQL thread..."
                #SQL thread is not running, retry to start it
                res = self.subordinate.start_sql_thread(self.query_options)
            if self.verbosity > 0:
                print "# Waiting for subordinate to synchronize with main"
            i += 1
        if i == num_tries:
            print "ERROR: failed to synch subordinate with main."
            result = False

        if result is True:
            self.replicating = True

        return result


    def test(self, db, num_tries):
        """Test the replication setup.

        Requires a database name which is created on the main then
        verified it appears on the subordinate.

        db[in]             Name of a database to use in test
        num_tries[in]      Number of attempts to wait for subordinate synch
        """

        if not self.replicating:
            print "ERROR: Replication is not running among main and subordinate."
        print "# Testing replication setup..."
        if self.verbosity > 0:
            print "# Creating a test database on main named %s..." % db
        res = self.main.exec_query("CREATE DATABASE %s" % db,
                                     self.query_options)
        i = 0
        while i < num_tries:
            time.sleep (1)
            res = self.subordinate.exec_query("SHOW DATABASES")
            for row in res:
                if row[0] == db:
                    res = self.main.exec_query("DROP DATABASE %s" % db,
                                                 self.query_options)
                    print "# Success! Replication is running."
                    i = num_tries
                    break
            i += 1
            if i < num_tries and self.verbosity > 0:
                print "# Waiting for subordinate to synchronize with main"
        if i == num_tries:
            print "ERROR: Unable to complete testing."


class Main(Server):
    """The Subordinate class is a subclass of the Server class. It represents a
    MySQL server performing the role of a subordinate in a replication topology.
    The following utilities are provide in addition to the Server utilities:

        - check to see if replication user is defined and has privileges
        - get binary log exceptions
        - get main status
        - reset main

    """

    def __init__(self, options={}):
        """Constructor

        The method accepts one of the following types for options['conn_info']:

            - dictionary containing connection information including:
              (user, passwd, host, port, socket)
            - connection string in the form: user:pass@host:port:socket
            - an instance of the Server class

        options[in]        options for controlling behavior:
            conn_info      a dictionary containing connection information
                           (user, passwd, host, port, socket)
            role           Name or role of server (e.g., server, main)
            verbose        print extra data during operations (optional)
                           default value = False
            charset        Default character set for the connection.
                           (default latin1)
        """

        assert not options.get("conn_info") == None

        self.options = options
        Server.__init__(self, options)


    def get_status(self):
        """Return the main status

        Returns result set
        """
        return self.exec_query("SHOW MASTER STATUS")


    def get_binlog_exceptions(self):
        """Get any binary logging exceptions

        This method queries the server status for the *-do-db and
        *-ignore-db settings.

        Returns [] - no exceptions, list if exceptions found
        """
        rows = []
        res = self.get_status()
        if res != []:
            do_db = res[0][_MASTER_DO_DB]
            ignore_db = res[0][_MASTER_IGNORE_DB]
            if len(do_db) > 0 or len(ignore_db) > 0:
                rows.append(('main', do_db, ignore_db))

        return rows


    def get_rpl_users(self, options={}):
        """Attempts to find the users who have the REPLICATION SLAVE privilege

        options[in]    query options

        Returns tuple list - (string, string, bool) = (user, host, has_password)
        """
        return self.exec_query(_RPL_USER_QUERY, options)


    def create_rpl_user(self, host, port, r_user, r_pass=None, verbosity=0):
        """Create the replication user and grant privileges

        If the user exists, check privileges and add privileges as needed.

        host[in]       host of the subordinate
        port[in]       port of the subordinate
        r_user[in]     user to create
        r_pass[in]     password for user to create (optional)
        verbosity[in]  verbosity of output
                       Default = 0

        Returns bool - True = success, False = errors
        """

        from mysql.utilities.common.user import User

        # Create user class instance
        user = User(self, "%s@%s:%s" % (r_user, host, port), verbosity)

        if not user.has_privilege("*", "*", "REPLICATION SLAVE"):
            if verbosity > 0:
                print "# Granting replication access to replication user..."
            query_str = "GRANT REPLICATION SLAVE ON *.* TO '%s'@'%s' " % \
                        (r_user, host)
            if r_pass:
                query_str += "IDENTIFIED BY '%s'" % r_pass
            try:
                self.exec_query(query_str)
            except UtilError, e:
                print "ERROR: Cannot grant replication subordinate to " + \
                      "replication user."
                return False

        return True


    def reset(self, options={}):
        """Reset the main

        options[in]    query options
        """
        return self.exec_query("RESET MASTER", options)


    def check_rpl_health(self):
        """Check replication health of the main.

        This method checks to see if the main is setup correctly to
        operate in a replication environment. It returns a tuple with a
        bool to indicate if health is Ok (True), and a list to contain any
        errors encountered during the checks.

        Returns tuple (bool, []) - (True, []) = Ok,
                                   (False, error_list) = not setup correctly
        """
        errors = []
        rpl_ok = True

        if not self.is_alive():
            return (False, ["Cannot connect to server"])

        gtid_enabled = self.supports_gtid() == "ON"

        # Check for binlogging
        if not gtid_enabled and not self.binlog_enabled():
            errors.append("No binlog on main.")
            rpl_ok = False

        # See if there is at least one user with rpl privileges
        res = self.get_rpl_users()
        if len(res) == 0:
            errors.append("There are no users with replication privileges.")
            rpl_ok = False

        return (rpl_ok, errors)


    def _check_discovered_subordinate(self, conn_dict):
        """ Check discovered subordinate is configured to this main

        This method attempts to determine if the subordinate specified is
        configured to connect to this main.

        conn_dict[in]  dictionary of connection information

        Returns bool - True - is configured, False - not configured
        """
        if conn_dict['conn_info']['host'] == '127.0.0.1':
            conn_dict['conn_info']['host'] = 'localhost'
        # Now we must attempt to connect to the subordinate.
        subordinate_conn = Subordinate(conn_dict)
        is_configured = False
        try:
            subordinate_conn.connect()
            # Skip discovered subordinates that are not configured
            # to connect to the main
            if subordinate_conn.is_configured_for_main(self, verify_state=False):
                is_configured = True
        except Exception, e:
            print "Error connecting to a subordinate as %s@%s: %s" % \
                  (conn_dict['conn_info']['user'],
                   conn_dict['conn_info']['host'],
                   e.errmsg)
        finally:
            subordinate_conn.disconnect()

        return is_configured


    def get_subordinates(self, user, password):
        """Return the subordinates registered for this main.

        This method returns a list of subordinates (host, port) if this server is
        a main in a replication topology and has subordinates registered.

        user[in]       user login
        password[in]   user password

        Returns list - [host:port, ...]
        """
        def _get_subordinate_info(host, port):
            if len(host) > 0:
                subordinate_info = host
            else:
                subordinate_info = "unknown host"
            subordinate_info += ":%s" % port
            return subordinate_info

        subordinates = []
        no_host_subordinates = []
        connect_error_subordinates = []
        res = self.exec_query("SHOW SLAVE HOSTS")
        if not res == []:
            res.sort()  # Sort for conformity
            for row in res:
                info = _get_subordinate_info(row[1], row[2])
                conn_dict = {
                    'conn_info' : { 'user' : user, 'passwd' : password,
                                    'host' : row[1], 'port' : row[2],
                                    'socket' : None },
                    'role'      : 'subordinate',
                    'verbose'   : self.options.get("verbosity", 0) > 0,
                }
                if not row[1]:
                    no_host_subordinates.append(info)
                elif self._check_discovered_subordinate(conn_dict):
                    subordinates.append(info)
                else:
                    connect_error_subordinates.append(info)

        if no_host_subordinates:
            print "WARNING: There are subordinates that have not been registered" + \
                  " with --report-host or --report-port."
            if self.options.get("verbosity", 0) > 0:
                for row in no_host_subordinates:
                    print "\t", row

        if connect_error_subordinates:
            print "\nWARNING: There are subordinates that had connection errors."
            if self.options.get("verbosity", 0) > 0:
                for row in connect_error_subordinates:
                    print "\t", row

        return subordinates


    def get_gtid_purged_statement(self):
        """General the SET @@GTID_PURGED statement for backup

        Returns string - statement for subordinate if GTID=ON, else None
        """
        if self.supports_gtid == "ON":
            gtid_executed = self.exec_query("SELECT @@GLOBAL.GTID_EXECUTED")[0]
            return 'SET @@GLOBAL.GTID_PURGED = "%s"' % gtid_executed
        else:
            return None


class MainInfo(object):
    """The MainInfo is an abstraction of the mechanism for storing the
    main information for subordinate servers. It is designed to return an
    implementation neutral representation of the information for use in
    newer servers that use a table to store the information as well as
    older servers that use a file to store the information.
    """

    def __init__(self, subordinate, options):
        """Constructor

        The method accepts one of the following types for options['conn_info']:

            - dictionary containing connection information including:
              (user, passwd, host, port, socket)
            - connection string in the form: user:pass@host:port:socket
            - an instance of the Server class

        options[in]        options for controlling behavior:
          filename         filename for main info file - valid only for
                           servers with main-info-repository=FILE or
                           versions prior to 5.6.5.
          verbosity        determines level of output. Default = 0.
          quiet            turns off all messages except errors.
                           Default is False.
        """

        assert subordinate is not None, "MainInfo requires an instance of Subordinate."
        self.subordinate = subordinate
        self.filename = options.get("main_info", "main.info")
        self.quiet = options.get("quiet", False)
        self.verbosity = options.get("verbosity", 0)
        self.values = {}      # internal dictionary of the values
        self.repo = "FILE"
        if self.subordinate is not None:
            res = self.subordinate.show_server_variable("main_info_repository")
            if res is not None and res != [] and \
               res[0][1].upper() == "TABLE":
                self.repo = "TABLE"


    def read(self):
        """Read the main information

        This method reads the main information either from a file or a
        table depending on the availability of and setting for
        main-info-repository. If missing (server version < 5.6.5), it
        defaults to reading from a file.

        Returns bool - True = success
        """
        if self.verbosity > 2:
            print "# Reading main information from a %s." % self.repo.lower()
        if self.repo == "FILE":
            import socket

            # Check host name of this host. If not the same, issue error.
            if self.subordinate.is_alias(socket.gethostname()):
                return self._read_main_info_file()
            else:
                raise UtilRplWarn("Cannot read main information file "
                                  "from a remote machine.")
        else:
            return self._read_main_info_table()


    def _check_read(self, refresh=False):
        """Check if main information has been read

        refresh[in]    if True, re-read the main information.
                       Default is False.

        If the main information has not been read, read it and populate
        the dictionary.
        """
        # Read the values if not already read or user says to refresh them.
        if self.values is None or self.values == {} or refresh:
            self.read()


    def _build_dictionary(self, rows):
        """Build the internal dictionary of values.

        rows[in]       Rows as read from the file or table
        """
        for i in range(0, len(rows)):
            self.values[_MASTER_INFO_COL[i]] = rows[i]


    def _read_main_info_file(self):
        """Read the contents of the main.info file.

        This method will raise an error if the file is missing or cannot be
        read by the user.

        Returns bool - success = True
        """
        contents = []
        res = self.subordinate.show_server_variable('datadir')
        if res is None or res == []:
            raise UtilRplError("Cannot get datadir.")
        datadir = res[0][1]
        if self.filename == 'main.info':
            self.filename = os.path.join(datadir, self.filename)

        if not os.path.exists(self.filename):
            raise UtilRplError("Cannot find main information file: "
                               "%s." % self.filename)
        try:
            mfile = open(self.filename, 'r')
            num = int(mfile.readline())
            # Protect overrun of array if main_info file length is
            # changed (more values added).
            if num > len(_MASTER_INFO_COL):
                num = len(_MASTER_INFO_COL)
        except:
            raise UtilRplError("Cannot read main information file: "
                               "%s.\nUser needs to have read access to "
                               "the file." % self.filename)
        # Build the dictionary
        for i in range(1, num):
            contents.append(mfile.readline().strip('\n'))
        self._build_dictionary(contents)
        mfile.close()

        return True


    def _read_main_info_table(self):
        """Read the contents of the subordinate_main_info table.

        This method will raise an error if the file is missing or cannot be
        read by the user.

        Returns bool - success = True
        """
        res = None
        try:
            res = self.subordinate.exec_query("SELECT * FROM "
                                        "mysql.subordinate_main_info")
        except UtilError, e:
            raise UtilRplError("Unable to read the subordinate_main_info table. "
                               "Error: %s" % e.errmsg)
        if res is None or res == []:
            return False

        # Build dictionary for the information with column information
        rows = []
        for i in range(0, len(res[0][1:])):
            rows.append(res[0][i+1])
        self._build_dictionary(rows)

        return True


    def show_main_info(self, refresh=False):
        """Display the contents of the main information.

        refresh[in]    if True, re-read the main information.
                       Default is False.
        """
        # Check to see if we need to read the information
        self._check_read(refresh)
        stop = len(self.values)
        for i in range(0, stop):
            print "{0:>30} : {1}".format(_MASTER_INFO_COL[i],
                                         self.values[_MASTER_INFO_COL[i]])


    def check_main_info(self, refresh=False):
        """Check to see if main info file matches subordinate status

        This method will return a list of discrepancies if the main.info
        file does not match subordinate status. It will also raise errors if there
        are problem accessing the main.info file.

        refresh[in]    if True, re-read the main information.
                       Default is False.

        Returns [] - no exceptions, list if exceptions found
        """
        # Check to see if we need to read the information
        self._check_read(refresh)
        errors = []
        res = self.subordinate.get_status()
        if res != []:
            state = res[0][_SLAVE_IO_STATE]
            if not state:
                raise UtilRplError("Subordinate is stopped.")
            m_host = res[0][_SLAVE_MASTER_HOST]
            m_port = res[0][_SLAVE_MASTER_PORT]
            rpl_user = res[0][_SLAVE_MASTER_USER]
            if m_host != self.values['Main_Host'] or \
               int(m_port) != int(self.values['Main_Port']) or \
               rpl_user != self.values['Main_User']:
                errors.append("Subordinate is connected to main differently "
                              "than what is recorded in the main "
                              "information file. Main information file "
                              "= user=%s, host=%s, port=%s." %
                              (self.values['Main_User'],
                               self.values['Main_Host'],
                               self.values['Main_Port']))

        return errors


    def get_value(self, key, refresh=False):
        """Returns the value found for the key or None if key not found.

        refresh[in]    if True, re-read the main information.
                       Default is False.

        Returns value - Value found for the key or None if key missing
        """
        # Check to see if we need to read the information
        self._check_read(refresh)
        try:
            return self.values[key]
        except:
            return None

    def get_main_info(self, refresh=False):
        """Returns the main information dictionary.

        refresh[in]    if True, re-read the main information.
                       Default is False.

        Returns dict - main information
        """
        # Check to see if we need to read the information
        self._check_read(refresh)
        return self.values


class Subordinate(Server):
    """The Subordinate class is a subclass of the Server class. It represents a
    MySQL server performing the role of a subordinate in a replication topology.
    The following utilities are provide in addition to the Server utilities:

        - get methods to return status, binary log exceptions, subordinate delay,
          thread status, io error, and main information
        - form the change main command with either known main or user-
          supplied values
        - check to see if subordinate is connected to a main
        - display subordinate status
        - show main information
        - verify main information matches currently connected main
        - start, stop, and reset subordinate

    """

    def __init__(self, options={}):
        """Constructor

        The method accepts one of the following types for options['conn_info']:

            - dictionary containing connection information including:
              (user, passwd, host, port, socket)
            - connection string in the form: user:pass@host:port:socket
            - an instance of the Server class

        options[in]        options for controlling behavior:
            conn_info      a dictionary containing connection information
                           (user, passwd, host, port, socket)
            role           Name or role of server (e.g., server, main)
            verbose        print extra data during operations (optional)
                           default value = False
            charset        Default character set for the connection.
                           (default latin1)
        """

        assert not options.get("conn_info") == None
        self.options = options
        Server.__init__(self, options)
        self.main_info = None


    def get_status(self, col_options={}):
        """Return the subordinate status

        col_options[in]    options for displaying columns (optional)

        Returns result set
        """
        return self.exec_query("SHOW SLAVE STATUS", col_options)


    def get_retrieved_gtid_set(self):
        """Get any events (gtids) read but not executed

        Returns list - gtids in retrieved_gtid_set
        """
        res = self.get_status()
        if res != []:
            return res[0][_RETRIEVED_GTID_SET]
        return []


    def get_executed_gtid_set(self):
        """Get any events (gtids) executed

        Returns list - gtids in retrieved_gtid_set
        """
        res = self.get_status()
        if res != []:
            return res[0][_EXECUTED_GTID_SET]

        return []


    def get_binlog_exceptions(self):
        """Get any binary logging exceptions

        This method queries the server status for the *-do-db and
        *-ignore-db settings.

        Returns [] - no exceptions, list if exceptions found
        """
        rows = []
        res = self.get_status()
        if res != []:
            do_db = res[0][_SLAVE_DO_DB]
            ignore_db = res[0][_SLAVE_IGNORE_DB]
            if len(do_db) > 0 or len(ignore_db) > 0:
                rows.append(('subordinate', do_db, ignore_db))

        return rows


    def get_main_host_port(self):
        """Get the subordinate's connected main host and port

        Returns tuple - (main host, main port) or
                        None if not acting as subordinate
        """
        res = self.get_status()
        if res == []:
            return None
        m_host = res[0][_SLAVE_MASTER_HOST]
        m_port = res[0][_SLAVE_MASTER_PORT]

        return (m_host, m_port)


    def is_connected(self):
        """Check to see if subordinate is connected to main

        This method will check the subordinate to see if it is connected to a main.

        Returns bool - True = subordinate is connected
        """
        res = self.get_status()
        if res == []:
            return False
        return res[0][10].upper() == "YES"

    def get_rpl_main_user(self):
        """Get the rpl main user from the subordinate status

        Returns the subordinate_main_user as string or False if there is
        no subordinate status.
        """
        res = self.get_status()
        if not res:
            return False
        return res[0][_SLAVE_MASTER_USER]

    def get_state(self):
        """Get the subordinate's connection state

        Returns state or None if not acting as subordinate
        """
        res = self.get_status()
        if res == []:
            return None
        state = res[0][_SLAVE_IO_STATE]

        return state


    def get_io_running(self):
        """Get the subordinate's IO thread status

        Returns IO_THREAD state or None if not acting as subordinate
        """
        res = self.get_status()
        if res == []:
            return None
        state = res[0][_SLAVE_IO_RUNNING]

        return state


    def get_delay(self):
        """Return subordinate delay values

        This method retrieves the subordinate's delay parameters.

        Returns tuple - subordinate delay values or None if not connected
        """
        res = self.get_status()
        if res == []:
            return None

        # subordinate IO state
        state = res[0][_SLAVE_IO_STATE]
        # seconds behind main
        if res[0][_SLAVE_DELAY] is None:
            sec_behind = 0
        else:
            sec_behind = int(res[0][_SLAVE_DELAY])
        # remaining delay
        delay_remaining = res[0][_SLAVE_REMAINING_DELAY]
        # main's log file read
        read_log_file = res[0][_SLAVE_MASTER_LOG_FILE]
        # position in main's binlog
        read_log_pos = res[0][_SLAVE_MASTER_LOG_FILE_POS]

        return (state, sec_behind, delay_remaining,
                read_log_file, read_log_pos)


    def get_thread_status(self):
        """Return the subordinate threads status

        Returns tuple - (subordinate_io_state, subordinate_io_running, subordinate_sql_running)
                        or None if not connected
        """
        res = self.get_status()
        if res == []:
            return None

        # subordinate IO state
        state = res[0][_SLAVE_IO_STATE]
        # subordinate_io_running
        io_running = res[0][_SLAVE_IO_RUNNING]
        # subordinate_sql_running
        sql_running = res[0][_SLAVE_SQL_RUNNING]

        return (state, io_running, sql_running)


    def get_io_error(self):
        """Return the subordinate subordinate io error status

        Returns tuple - (subordinate_io_state, io_errorno, io_error)
                        or None if not connected
        """
        res = self.get_status()
        if res == []:
            return None

        state = res[0][_SLAVE_IO_STATE]
        io_errorno = int(res[0][_SLAVE_IO_ERRORNO])
        io_error = res[0][_SLAVE_IO_ERROR]

        return (state, io_errorno, io_error)

    def get_subordinates_errors(self):
        """Return the subordinate subordinate io and sql error status

        Returns tuple - (subordinate_io_state, io_errorno, io_error, io_running,
                         sql_running, sql_errorno, sql_error)
                        or None if not connected
        """
        res = self.get_status()
        if not res:
            return None

        state = res[0][_SLAVE_IO_STATE]
        io_errorno = int(res[0][_SLAVE_IO_ERRORNO])
        io_error = res[0][_SLAVE_IO_ERROR]
        io_running = res[0][_SLAVE_IO_RUNNING]
        sql_running = res[0][_SLAVE_SQL_RUNNING]
        sql_errorno = int(res[0][_SLAVE_SQL_ERRORNO])
        sql_error = res[0][_SLAVE_SQL_ERROR]

        return (state, io_errorno, io_error, io_running, sql_running,
                sql_errorno, sql_error)


    def show_status(self):
        """Display the subordinate status from the subordinate server
        """
        col_options = {
            'columns' : True
        }
        res = self.get_status(col_options)
        if res != [] and res[1] != []:
            stop = len(res[0])
            cols = res[0]
            rows = res[1]
            for i in range(0, stop):
                print "{0:>30} : {1}".format(cols[i], rows[0][i])
        else:
            raise UtilRplError("Cannot get subordinate status or subordinate is "
                                 "not configured as a subordinate or not "
                                 "started.")


    def get_rpl_user(self):
        """Return the main user from the main info record.

        Returns - tuple = (user, password) or (None, None) if errors
        """
        self.main_info = MainInfo(self, self.options)
        m_host = self.main_info.get_value("Main_User")
        m_passwd = self.main_info.get_value("Main_Password")
        if m_host is not None:
            return (m_host, m_passwd)
        return (None, None)


    def start(self, options={}):
        """Start the subordinate

        options[in]    query options
        """
        return self.exec_query("START SLAVE", options)

    def start_io_thread(self, options={}):
        """Start the subordinate I/O thread

        options[in]    query options
        """
        return self.exec_query("START SLAVE IO_THREAD", options)

    def start_sql_thread(self, options={}):
        """Start the subordinate SQL thread

        options[in]    query options
        """
        return self.exec_query("START SLAVE SQL_THREAD", options)

    def stop(self, options={}):
        """Stop the subordinate

        options[in]    query options
        """
        return self.exec_query("STOP SLAVE", options)


    def reset(self, options={}):
        """Reset the subordinate

        options[in]    query options
        """
        return self.exec_query("RESET SLAVE", options)


    def reset_all(self, options={}):
        """Reset all information on this subordinate.

        options[in]    query options
        """
        # Must be sure to do stop first
        self.stop()
        # RESET SLAVE ALL was implemented in version 5.5.16 and later
        if not self.check_version_compat(5, 5, 16):
            return self.reset()
        return self.exec_query("RESET SLAVE ALL", options)


    def num_gtid_behind(self, main_gtids):
        """Get the number of transactions the subordinate is behind the main.

        main_gtids[in]  the main's GTID_EXECUTED list

        Returns int - number of trans behind main
        """
        subordinate_gtids = self.exec_query(_GTID_EXECUTED)[0][0]
        gtids = self.exec_query("SELECT GTID_SUBTRACT('%s','%s')" %
                               (main_gtids[0][0], subordinate_gtids))[0]
        if len(gtids) == 1 and len(gtids[0]) == 0:
            gtid_behind = 0
        else:
            gtids = gtids[0].split("\n")
            gtid_behind = len(gtids)
        return gtid_behind


    def wait_for_subordinate(self, binlog_file, binlog_pos, timeout=300):
        """Wait for the subordinate to read the main's binlog to specified position

        binlog_file[in]  main's binlog file
        binlog_pos[in]   main's binlog file position
        timeout[in]      maximum number of seconds to wait for event to occur

        Returns bool - True = subordinate has read to the file and pos,
                       False = subordinate is behind.
        """
        # Wait for subordinate to read the main log file
        _MASTER_POS_WAIT = "SELECT MASTER_POS_WAIT('%s', %s, %s)"
        res = self.exec_query(_MASTER_POS_WAIT % (binlog_file,
                                                  binlog_pos, timeout))
        if res is None or (res[0][0] is not None and int(res[0][0]) < 0):
            return False
        return True


    def wait_for_subordinate_gtid(self, main_gtid, timeout=300, verbose=False):
        """Wait for the subordinate to read the main's GTIDs.

        This method requires that the server supports GTIDs.

        main_gtid[in]  the list of gtids from the main
                         obtained via SELECT @@GLOBAL.GTID_EXECUTED on main
        timeout[in]      timeout for waiting for subordinate to catch up
                         Note: per GTID call. Default is 300 seconds (5 min.).
        verbose[in]      if True, print query used.
                         Default is False

        Returns bool - True = subordinate has read all GTIDs
                       False = subordinate is behind
        """
        m_gtid_str = " ".join(main_gtid[0][0].split('\n'))
        main_gtids = main_gtid[0][0].split('\n')
        subordinate_wait_ok = True
        for gtid in main_gtids:
            try:
                if verbose:
                    print "# Subordinate %s:%s:" % (self.host, self.port)
                    print "# QUERY =", _GTID_WAIT % (gtid.strip(','), timeout)
                res = self.exec_query(_GTID_WAIT % (gtid.strip(','), timeout))
                if verbose:
                    print "# Return Code =", res[0][0]
                if res is None or res[0] is None or res[0][0] is None or \
                   int(res[0][0]) < 0:
                    subordinate_wait_ok = False
            except UtilRplError, e:
                raise UtilRplError("Error executing %s: %s" %
                                   ((_GTID_WAIT % (gtid.strip(','), timeout)),
                                   e.errmsg))
        return subordinate_wait_ok


    def make_change_main(self, from_beginning=False, main_values={}):
        """Make the CHANGE MASTER command.

        This method forms the CHANGE MASTER command based on the current
        settings of the subordinate. If the user supplies a dictionary of options,
        the method will use those values provided by the user if present
        otherwise it will use current settings.

        Note: the keys used in the dictionary are defined in the
              _MASTER_INFO_COL list defined above.

        from_beginning[in] if True, omit specification of main's binlog info
        main_values[in] if provided, use values in the dictionary

        Returns string - CHANGE MASTER command
        """
        if main_values == {} and not self.is_connected():
            raise UtilRplError("Cannot generate CHANGE MASTER command. The "
                               "subordinate is not connected to a main and no "
                               "main information was provided.")
        elif self.is_connected():
            m_info = MainInfo(self, self.options)
            main_info = m_info.get_main_info()
            if main_info is None and main_values == {}:
                raise UtilRplError("Cannot create CHANGE MASTER command.")
        else:
            main_info = None

        # Form values for command.
        # If we cannot get the main info information, try the values passed
        if main_info is None:
            main_host = main_values['Main_Host']
            main_port = main_values['Main_Port']
            main_user = main_values['Main_User']
            main_passwd = main_values['Main_Password']
            main_log_file = main_values['Main_Log_File']
            main_log_pos = main_values['Read_Main_Log_Pos']
        else:
            main_host = main_values.get('Main_Host',
                                            main_info['Main_Host'])
            main_port = main_values.get('Main_Port',
                                            main_info['Main_Port'])
            main_user = main_values.get('Main_User',
                                            main_info['Main_User'])
            main_passwd = main_values.get('Main_Password',
                                               main_info['Main_Password'])
            main_log_file = main_values.get('Main_Log_File',
                                                main_info['Main_Log_File'])
            main_log_pos = main_values.get('Read_Main_Log_Pos',
                                            main_info['Read_Main_Log_Pos'])

        change_main = "CHANGE MASTER TO MASTER_HOST = '%s', " % main_host
        if main_user:
            change_main += "MASTER_USER = '%s', " % main_user
        if main_passwd:
            change_main += "MASTER_PASSWORD = '%s', " % main_passwd
        change_main += "MASTER_PORT = %s" % main_port
        if self.supports_gtid() == "ON":
            change_main += ", MASTER_AUTO_POSITION=1"
        elif not from_beginning:
            change_main += ", MASTER_LOG_FILE = '%s'" % main_log_file
            if main_log_pos >= 0:
                change_main += ", MASTER_LOG_POS = %s" % main_log_pos

        return change_main


    def is_configured_for_main(self, main, verify_state=False):
        """Check that subordinate is connected to the main at host, port.

        main[in]     instance of the main

        Returns bool - True = is connected
        """
        res = self.get_status()
        if res == [] or not res[0]:
            return False
        res = res[0]
        m_host, m_port = self.get_main_host_port()
        # Suppose the state is True for "Waiting for main to send event"
        # so we can ignore it if verify_state is not given as True.
        state = True
        if verify_state:
            state = self.get_state() == "Waiting for main to send event"
        if (not main.is_alias(m_host) or int(m_port) != int(main.port)
            or not state):
            return False
        return True


    def check_rpl_health(self, main, main_log, main_log_pos,
                         max_delay, max_pos, verbosity):
        """Check replication health of the subordinate.

        This method checks to see if the subordinate is setup correctly to
        operate in a replication environment. It returns a tuple with a
        bool to indicate if health is Ok (True), and a list to contain any
        errors encountered during the checks.

        main[in]         Main class instance
        main_log[in]     main's log file
        main_log_pos[in] main's log file position
        max_delay[in]      if the subordinate delay (in seconds) is greater than this
                           value, the subordinate health is not Ok
        max_pos[in]        maximum position difference from main to subordinate to
                           determine if subordinate health is not Ok
        verbosity[in]      if > 1, return detailed errors else return only
                           short phrases

        Returns tuple (bool, []) - (True, []) = Ok,
                                   (False, error_list) = not setup correctly
        """
        errors = []
        rpl_ok = True

        if not self.is_alive():
            return (False, ["Cannot connect to server"])

        res = self.get_status()
        if res != [] and res[0] != []:
            res = res[0]
            state = res[_SLAVE_IO_STATE]
            m_host, m_port = self.get_main_host_port()
            m_log = res[_SLAVE_MASTER_LOG_FILE]
            m_log_pos = res[_SLAVE_MASTER_LOG_FILE_POS]
            io_running = res[_SLAVE_IO_RUNNING]
            sql_running = res[_SLAVE_SQL_RUNNING]
            s_delay = res[_SLAVE_DELAY]
            delay = s_delay if s_delay is not None else 0
            remaining_delay = res[_SLAVE_REMAINING_DELAY]
            io_error_num = res[_SLAVE_IO_ERRORNO]
            io_error_text = res[_SLAVE_IO_ERROR]

            # Check to see that subordinate is connected to the right main
            if not self.is_configured_for_main(main):
                return (False, ["Not connected to correct main."])

            # Check subordinate status for errors, threads activity
            if io_running.upper() != "YES":
                errors.append("IO thread is not running.")
                rpl_ok = False
            if sql_running.upper() != "YES":
                errors.append("SQL thread is not running.")
                rpl_ok = False
            if int(io_error_num) > 0:
                errors.append(io_error_text)
                rpl_ok = False

            # Check subordinate delay with threshhold of SBM, and main's log pos
            if int(delay) > int(max_delay):
                errors.append("Subordinate delay is %s seconds behind main." %
                              delay)
                if len(remaining_delay):
                    errors.append(remaining_delay)
                rpl_ok = False

            # Check main position
            if self.supports_gtid() != "ON":
                if m_log != main_log:
                    errors.append("Wrong main log file.")
                    rpl_ok = False
                elif (int(m_log_pos) + int(max_pos)) < int(main_log_pos):
                    errors.append("Subordinate's main position exceeds maximum.")
                    rpl_ok = False

            # Check GTID trans behind.
            elif self.supports_gtid() == "ON":
                main_gtids = main.exec_query(_GTID_EXECUTED)
                num_gtids_behind = self.num_gtid_behind(main_gtids)
                if num_gtids_behind > 0:
                    errors.append("Subordinate has %s transactions behind main." %
                                  num_gtids_behind)
                    rpl_ok = False

        else:
            errors.append("Not connected")
            rpl_ok = False

        if len(errors) > 1:
            errors = [", ".join(errors)]

        return (rpl_ok, errors)


    def get_rpl_details(self):
        """Return subordinate status variables for health reporting

        This method retrieves the subordinate's parameters for checking relationship
        with main.

        Returns tuple - subordinate values or None if not connected
        """
        res = self.get_status()
        if res == []:
            return None

        res = res[0]
        read_log_file = res[_SLAVE_MASTER_LOG_FILE]
        read_log_pos = res[_SLAVE_MASTER_LOG_FILE_POS]
        io_thread = res[_SLAVE_IO_RUNNING]
        sql_thread = res[_SLAVE_SQL_RUNNING]

        # seconds behind main
        if res[_SLAVE_DELAY] is None:
            sec_behind = 0
        else:
            sec_behind = int(res[_SLAVE_DELAY])
        delay_remaining = res[_SLAVE_REMAINING_DELAY]

        io_error_num = res[_SLAVE_IO_ERRORNO]
        io_error_text = res[_SLAVE_IO_ERROR]
        sql_error_num = res[_SLAVE_SQL_ERRORNO]
        sql_error_text = res[_SLAVE_SQL_ERROR]

        return (read_log_file, read_log_pos, io_thread, sql_thread, sec_behind,
                delay_remaining, io_error_num, io_error_text, sql_error_num,
                sql_error_text)


    def switch_main(self, main, user, passwd="", from_beginning=False,
                      main_log_file=None, main_log_pos=None,
                      show_command=False):
        """Switch subordinate to a new main

        This method stops the subordinate and issues a new change main command
        to the main specified then starts the subordinate. No prerequisites are
        checked and it does not wait to see if subordinate catches up to the main.

        main[in]           Main class instance
        user[in]             replication user
        passwd[in]           replication user password
        from_beginning[in]   if True, start from beginning of logged events
                             Default = False
        main_log_file[in]  main's log file (not needed for GTID)
        main_log_pos[in]   main's log file position (not needed for GTID)
        show_command[in]     if True, display the change main command
                             Default = False

        returns bool - True = success
        """
        hostport = "%s:%s" % (self.host, self.port)

        main_values = {
            'Main_Host'          : main.host,
            'Main_Port'          : main.port,
            'Main_User'          : user,
            'Main_Password'      : passwd,
            'Main_Log_File'      : main_log_file,
            'Read_Main_Log_Pos'  : main_log_pos,
        }
        change_main = self.make_change_main(from_beginning, main_values)
        if show_command:
            print "# Change main command for %s:%s" % (self.host, self.port)
            print "#", change_main
        res = self.exec_query(change_main)
        if res is None or res != ():
            raise UtilRplError("Subordinate %s:%s change main failed.",
                               (hostport, res[0]))
        return True
