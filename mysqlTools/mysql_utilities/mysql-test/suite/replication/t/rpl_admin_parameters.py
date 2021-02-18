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
import os
import mutlib
import rpl_admin
from mysql.utilities.exception import MUTLibError

_LOGNAME = "temp_log.txt"
_LOG_ENTRIES = [
    "2012-03-11 15:55:33 PM INFO TEST MESSAGE 1.\n",
    "2022-04-21 15:55:33 PM INFO TEST MESSAGE 2.\n",
]

class test(rpl_admin.test):
    """test replication administration commands
    This test exercises the mysqlrpladmin utility parameters.
    It uses the rpl_admin test for setup and teardown methods.
    """

    # Some of the parameters cannot be tested because they are threshold
    # values used in timing. These include --ping, --timeout, --max-position,
    # and --seconds-behind. We include a test case for regression that
    # specifies these options but does not test them.

    def check_prerequisites(self):
        return rpl_admin.test.check_prerequisites(self)

    def setup(self):
        return rpl_admin.test.setup(self)

    def run(self):
        self.res_fname = "result.txt"
        
        base_cmd = "mysqlrpladmin.py --ping=5 --timeout=7 --rpl-user=rpl:rpl " + \
                   "--seconds-behind=30 --max-position=100 "

        main_conn = self.build_connection_string(self.server1).strip(' ')
        subordinate1_conn = self.build_connection_string(self.server2).strip(' ')
        subordinate2_conn = self.build_connection_string(self.server3).strip(' ')
        subordinate3_conn = self.build_connection_string(self.server4).strip(' ')
        
        main_str = "--main=" + main_conn
        subordinates_str = "--subordinates=" + \
                     ",".join([subordinate1_conn, subordinate2_conn, subordinate3_conn])

        comment = "Test case 1 - show help"
        cmd_str = base_cmd + " --help"
        res = mutlib.System_test.run_test_case(self, 0, cmd_str, comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        comment = "Test case 2 - test subordinate discovery"
        cmd_str = "%s %s " % (base_cmd, main_str) 
        cmd_opts = " --discover-subordinates-login=root:root health"
        res = mutlib.System_test.run_test_case(self, 0, cmd_str+cmd_opts,
                                               comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)            
        
        self.server2.exec_query("GRANT REPLICATION SLAVE ON *.* TO "
                                "'rpl'@'localhost' IDENTIFIED BY 'rpl'")
        
        comment = "Test case 3 - switchover with verbosity"
        cmd_str = "%s %s " % (base_cmd, main_str)
        cmd_opts = " --discover-subordinates-login=root:root --verbose switchover "
        cmd_opts += " --demote-main --no-health --new-main=%s" % subordinate1_conn
        res = mutlib.System_test.run_test_case(self, 0, cmd_str+cmd_opts,
                                               comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        comment = "Test case 4 - switchover with quiet"
        cmd_str = "%s --main=%s " % (base_cmd, subordinate1_conn)
        cmd_opts = " --discover-subordinates-login=root:root --quiet switchover "
        cmd_opts += " --demote-main --new-main=%s" % main_conn
        cmd_opts += " --log=%s --log-age=1 " % _LOGNAME
        res = mutlib.System_test.run_test_case(self, 0, cmd_str+cmd_opts,
                                               comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)
            
        # Now check the log and dump its entries
        log_file = open(_LOGNAME, "r")
        num_log_lines = len(log_file.readlines())
        if num_log_lines > 0:
            self.results.append("Switchover has written to the log.\n")
        else:
            self.results.append("ERROR! Nothing written to the log.\n")
        log_file.close()
            
        # Now overwrite the log file and populate with known 'old' entries
        log_file = open(_LOGNAME, "w+")
        log_file.writelines(_LOG_ENTRIES)
        self.results.append("There are (before) %s entries in the log.\n" %
                            len(_LOG_ENTRIES))
        log_file.close()
        
        comment = "Test case 5 - switchover with logs"
        cmd_str = "%s %s " % (base_cmd, main_str)
        cmd_opts = " --discover-subordinates-login=root:root switchover "
        cmd_opts += " --demote-main --new-main=%s " % subordinate1_conn
        cmd_opts += " --log=%s --log-age=1 " % _LOGNAME
        res = mutlib.System_test.run_test_case(self, 0, cmd_str+cmd_opts,
                                               comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)
        
        # Now check the log and dump its entries
        log_file = open(_LOGNAME, "r")
        if len(log_file.readlines()) > num_log_lines:
            self.results.append("There are additional entries in the log.\n")
        else:
            self.results.append("ERROR: Nothing else written to the log.\n")
        log_file.close()
        try:
            os.unlink(_LOGNAME)
        except:
            pass
        
        comment = "Test case 6 - attempt risky switchover without force"
        cmd_str = "%s --main=%s " % (base_cmd, subordinate2_conn)
        new_subordinates = " --subordinates=" + ",".join([main_conn, subordinate1_conn, subordinate3_conn])
        cmd_opts = new_subordinates + " switchover "
        cmd_opts += " --new-main=%s " % subordinate2_conn
        res = mutlib.System_test.run_test_case(self, 0, cmd_str+cmd_opts,
                                               comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        comment = "Test case 7 - attempt risky switchover with --force"
        cmd_str = "%s --main=%s --force " % (base_cmd, subordinate2_conn)
        new_subordinates = " --subordinates=" + ",".join([main_conn, subordinate1_conn, subordinate3_conn])
        cmd_opts = new_subordinates + " switchover "
        cmd_opts += " --new-main=%s " % subordinate2_conn
        res = mutlib.System_test.run_test_case(self, 0, cmd_str+cmd_opts,
                                               comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)
        
        # Now we return the topology to its original state for other tests
        rpl_admin.test.reset_topology(self)

        # Mask out non-deterministic data
        rpl_admin.test.do_masks(self)
        
        self.replace_substring("%s" % self.server1.get_version(),
                               "XXXXXXXXXXXXXXXXXXXXXX")
        self.replace_result("# CHANGE MASTER TO MASTER_HOST",
                            "# CHANGE MASTER TO MASTER_HOST [...]\n")

        return True

    def get_result(self):
        return self.compare(__name__, self.results)
    
    def record(self):
        return self.save_result_file(__name__, self.results)
    
    def cleanup(self):
        try:
            os.rmdir("watchout_here")
        except:
            pass
        try:
            os.rmdir("watchout_here_too")
        except:
            pass
        return rpl_admin.test.cleanup(self)



