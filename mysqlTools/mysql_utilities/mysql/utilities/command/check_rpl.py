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
This file contains the check replication functionality to verify a replication
setup.
"""

import sys
from mysql.utilities.exception import UtilError, UtilRplError, UtilRplWarn

_PRINT_WIDTH = 75    
_RPL_HOST, _RPL_USER = 1, 2

def _get_replication_tests(rpl, options):
    """Return list of replication test function pointers.
    
    This list can be used to iterate over the replication tests for ensuring
    a properly configured main and subordinate topology.
    """
    return [
        _TestMainBinlog(rpl, options),
        _TestBinlogExceptions(rpl, options),
        _TestRplUser(rpl, options),
        _TestServerIds(rpl, options),
        _TestUUIDs(rpl, options),
        _TestSubordinateConnection(rpl, options),
        _TestMainInfo(rpl, options),
        _TestInnoDB(rpl, options),
        _TestStorageEngines(rpl, options),
        _TestLCTN(rpl, options),
        _TestSubordinateBehindMain(rpl, options),
    ]


def check_replication(main_vals, subordinate_vals, options):
    """Check replication among a main and a subordinate.
    
    main_vals[in]    Main connection in form: user:passwd@host:port:socket
                       or login-path:port:socket
    subordinate_vals[in]     Subordinate connection in form user:passwd@host:port:socket
                       or login-path:port:socket
    options[in]        dictionary of options (verbosity, quiet, pedantic)
    
    Returns bool - True if all tests pass, False if errors, warnings, failures
    """
    
    from mysql.utilities.common.server import connect_servers
    from mysql.utilities.common.replication import Replication
    
    quiet = options.get("quiet", False)
    width = options.get("width", 75)
    subordinate_status = options.get("subordinate_status", False)

    test_errors = False

    conn_options = {
        'quiet'     : quiet,
        'src_name'  : "main",
        'dest_name' : 'subordinate',
        'version'   : "5.0.0",
        'unique'    : True,
    }
    servers = connect_servers(main_vals, subordinate_vals, conn_options)
    
    rpl_options = options.copy()
    rpl_options['verbosity'] = options.get("verbosity", 0) > 0

    # Create an instance of the replication object
    rpl = Replication(servers[0], servers[1], rpl_options)
    
    if not quiet:
        print "Test Description",
        print ' ' * (width-24),
        print "Status"
        print '-' * width
    
    for test in _get_replication_tests(rpl, options):
        if not test.exec_test():
            test_errors = True
                
    if subordinate_status and not quiet:
        try:
            print "\n#\n# Subordinate status: \n#" 
            rpl.subordinate.show_status()
        except UtilRplError, e:
            print "ERROR:", e.errmsg
                        
    if not quiet:
        print "# ...done."
        
    return test_errors


class _BaseTestReplication(object):
    """
    The _BaseTestReplication class can be used to determine if two servers are
    correctly configured for replication.
    
    This class provides a rpl_test() method which can be overridden to
    execute specific tests.
    """
    
    def __init__(self, rpl, options):
        """Constructor

        rpl[in]           Replicate class instance 
        options[in]       dictionary of options to include width, verbosity,
                          pedantic, quiet
        """
        self.options = options
        self.verbosity = options.get("verbosity", 0)
        self.quiet = options.get("quiet", False)
        self.suppress = options.get("suppress", False)
        self.width = options.get("width", _PRINT_WIDTH)
        self.rpl = rpl
        self.description = ""  # Users must set this.
        self.warning = False


    def report_test(self, description):
        """Print the test category
        
        description[in]   description of test
        """
        self.description = description
        if not self.quiet:
            print self.description[0:self.width-9],
            print ' ' * (self.width-len(self.description)-8),
   

    def report_status(self, state, errors):
        """Print the results of a test.
        
        state[in]         state of the test
        errors[in]        list of errors
        
        Returns bool - True if errors detected during epilog reporting.
        """
        if not self.quiet:
            print "[%s]" % state
        if type(errors) == list and len(errors) > 0:
            print
            for error in errors:
                print error
            print
        res = False
        if state == "pass": # Only execute epilog if test passes.
            try:
                self.report_epilog()
            except UtilRplError, e:
                print "ERROR:", e.errmsg
                res = True
            
        return res


    def rpl_test(self):
        """Execute replication test.
        
        Override this method to provide specific tests for replication. For
        example, checking that binary log is turn on for the main. This
        method returns a list of strings containing test-specific errors or an
        empty list to indicate a test has passed.
        
        Note: Do not include newline characters on error message strings.
        
        To create a suite of tests, create a method that returns a list of
        function pointers to this method of each derived class. See the
        method _get_replication_tests() above for an example.        
        """
        pass


    def report_epilog(self):
        """Execute post-test reporting.
        
        Override this method for post-test reporting.
        """
        pass


    def exec_test(self):
        """Execute a test for replication prerequisites
        
        This method will report the test to be run, execute the test, and if
        the result is None, report the test as 'pass' else report the test
        as 'FAIL' and print the error messages. If warning is set, the method will
        report 'WARN' instead of 'FAIL' and print the error messages.
        
        Should the test method raise an error, the status is set to 'FAIL' and
        the exception is reported.
        
        Returns bool  True if test passes, False if warning or failure
        """
        try:
            res = self.rpl_test()
        # Any errors raised is a failed test.
        except UtilRplError, e:
            if not self.quiet:
                self.report_status("FAIL", [e.errmsg])
            else:
                print "Test: %s failed. Error: %s" % (self.description,
                                                      e.errmsg)
            return False
        # Check for warnings
        except UtilRplWarn, e:
            if not self.quiet:
                self.report_status("WARN", [e.errmsg])
            else:
                print "Test: %s had warnings. %s" % (self.description,
                                                     e.errmsg)
            return False

        # Check to see if test passed or if there were errors returned.
        if (type(res) == list and res == []) or \
           (type(res) == bool and res):
            return not self.report_status("pass", [])
        else:
            if self.warning:
                if not self.suppress:
                    if not self.quiet:
                        self.report_status("WARN", res)
                    else:
                        print "WARNING:", self.description
                        for error in res:
                            print error
                elif not self.quiet:
                    self.report_status("WARN", res)
            else:
                self.report_status("FAIL", res)
            return False
        return True


class _TestMainBinlog(_BaseTestReplication):
    """Test main has binlog enabled.
    """
    
    def rpl_test(self):
        """Execute test.
        """
        # Check main for binary logging
        self.report_test("Checking for binary logging on main")
        return self.rpl.check_main_binlog()


class _TestBinlogExceptions(_BaseTestReplication):
    """Test for binary log exceptions.
    """
    
    def rpl_test(self):
        """Execute test.
        """
        # Check binlog exceptions
        self.warning = True
        self.report_test("Are there binlog exceptions?")
        return self.rpl.get_binlog_exceptions()
    

class _TestRplUser(_BaseTestReplication):
    """Test replication user permissions.
    """
    
    def rpl_test(self):
        """Execute test.
        """
        # Check rpl_user
        self.report_test("Replication user exists?")
        res = self.rpl.subordinate.get_status()
        if res is None or res == []:
            raise UtilRplError("Subordinate is not connected to a main.")
        return self.rpl.main.check_rpl_user(res[0][_RPL_USER],
                                              self.rpl.subordinate.host)

class _TestServerIds(_BaseTestReplication):
    """Test server ids are different.
    """
    
    def rpl_test(self):
        """Execute test.
        """
        # Check server ids
        self.report_test("Checking server_id values")
        return self.rpl.check_server_ids()
        
    def report_epilog(self):
        """Report server_ids.
        """
        if self.verbosity > 0 and not self.quiet:
            main_id = self.rpl.main.get_server_id()
            subordinate_id = self.rpl.subordinate.get_server_id()
            print "\n main id = %s" % main_id
            print "  subordinate id = %s\n" % subordinate_id
            

class _TestUUIDs(_BaseTestReplication):
    """Test server uuids are different.
    """
    
    def rpl_test(self):
        """Execute test.
        """
        # Check server ids
        self.report_test("Checking server_uuid values")
        return self.rpl.check_server_uuids()
        
    def report_epilog(self):
        """Report server_ids.
        """
        if self.verbosity > 0 and not self.quiet:
            main_uuid = self.rpl.main.get_server_uuid()
            subordinate_uuid = self.rpl.subordinate.get_server_uuid()
            print "\n main uuid = %s" % \
                  (main_uuid if main_uuid is not None else "Not supported.")
            print "  subordinate uuid = %s\n" % \
                  (subordinate_uuid if subordinate_uuid is not None else "Not supported.")
            

class _TestSubordinateConnection(_BaseTestReplication):
    """Test whether subordinate can connect or is connected to the main.
    """
    
    def rpl_test(self):
        """Execute test.
        """
        # Check subordinate connection
        self.warning = True
        self.report_test("Is subordinate connected to main?")
        return self.rpl.check_subordinate_connection()


class _TestMainInfo(_BaseTestReplication):
    """Ensure main info file matches subordinate connection.
    """
    
    def rpl_test(self):
        """Execute test.
        """
        # Check main.info file
        from mysql.utilities.common.replication import MainInfo
        
        self.warning = True
        m_info = MainInfo(self.rpl.subordinate, self.options)
        self.report_test("Check main information file")
        return m_info.check_main_info()
        
    def report_epilog(self):
        """Report main info contents.
        """
        from mysql.utilities.common.replication import MainInfo
        
        if self.verbosity > 0 and not self.quiet:
            m_info = MainInfo(self.rpl.subordinate, self.options)
            print "\n#\n# Main information file: \n#" 
            main_info = m_info.show_main_info()
            print
            

class _TestInnoDB(_BaseTestReplication):
    """Test InnoDB compatibility.
    """
    
    def rpl_test(self):
        """Execute test.
        """
        # Check InnoDB compatibility
        self.report_test("Checking InnoDB compatibility")
        return self.rpl.check_innodb_compatibility(self.options)


class _TestStorageEngines(_BaseTestReplication):
    """Test storage engines lists such that subordinate has the same storage engines
    as the main.
    """
    
    def rpl_test(self):
        """Execute test.
        """
        # Checking storage engines
        self.report_test("Checking storage engines compatibility")
        return self.rpl.check_storage_engines(self.options)

class _TestLCTN(_BaseTestReplication):
    """Test the LCTN settings of main and subordinate.
    """
    
    def rpl_test(self):
        """Execute test.
        """
        # Check lctn
        self.warning = True
        self.report_test("Checking lower_case_table_names settings")
        return self.rpl.check_lctn()
        
    def report_epilog(self):
        """Report lctn settings.
        """
        if self.verbosity > 0 and not self.quiet:
            subordinate_lctn = self.rpl.subordinate.get_lctn()
            main_lctn = self.rpl.main.get_lctn()
            print "\n  Main lower_case_table_names: %s" % main_lctn
            print "   Subordinate lower_case_table_names: %s\n" % subordinate_lctn

class _TestSubordinateBehindMain(_BaseTestReplication):
    """Test for subordinate being behind main.
    """
    
    def rpl_test(self):
        """Execute test.
        """
        # Check subordinate behind main
        self.report_test("Checking subordinate delay (seconds behind main)")
        return self.rpl.check_subordinate_delay()

    

