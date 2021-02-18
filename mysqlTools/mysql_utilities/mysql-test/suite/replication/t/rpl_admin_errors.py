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
import socket
from mysql.utilities.exception import MUTLibError

class test(rpl_admin.test):
    """test replication administration commands
    This test exercises the mysqlrpladmin utility known error conditions.
    It uses the rpl_admin test for setup and teardown methods.
    """

    def check_prerequisites(self):
        return rpl_admin.test.check_prerequisites(self)

    def setup(self):
        return rpl_admin.test.setup(self)

    def run(self):
        self.res_fname = "result.txt"
        
        base_cmd = "mysqlrpladmin.py "
        main_conn = self.build_connection_string(self.server1).strip(' ')
        subordinate1_conn = self.build_connection_string(self.server2).strip(' ')
        subordinate2_conn = self.build_connection_string(self.server3).strip(' ')
        subordinate3_conn = self.build_connection_string(self.server4).strip(' ')
        
        main_str = "--main=" + main_conn
    
        # create a user for priv check
        self.server1.exec_query("CREATE USER 'joe'@'localhost'")
        self.server1.exec_query("GRANT SELECT, SUPER ON *.* TO 'jane'@'localhost'")
        mock_main1 = "--main=joe@localhost:%s" % self.server1.port
        mock_main2 = "--main=jane@localhost:%s" % self.server1.port
        subordinates_str = "--subordinates=" + \
                     ",".join([subordinate1_conn, subordinate2_conn, subordinate3_conn])
        candidates_str = "--candidates=" + \
                         ",".join([subordinate1_conn, subordinate2_conn, subordinate3_conn])

        # List of test cases for test
        test_cases = [
            # (comment, ret_val, option1, ...),
            ("Multiple commands issued.", 2, "switchover", "start"),
            ("No commands.", 2, ""),
            ("Invalid command.", 2, "NOTACOMMAND"),
            ("Switchover but no --main, --new-main,", 2, "switchover"),
            ("No subordinates or discover-subordinates-login", 2, "switchover", main_str),
            ("Force used with failover", 2, "failover", "--force", main_str,
             subordinates_str),
            ("Bad --new-main connection string", 2, "switchover", main_str,
             subordinates_str, "--new-main=whatmeworry?"),
            ("Bad --main connection string", 1, "switchover", subordinates_str,
             "--new-main=%s" % main_conn, "--main=whatmeworry?"),
            ("Bad --subordinates connection string", 1, "switchover", main_str,
             "--new-main=%s" % main_conn, "--subordinates=what,me,worry?"),
            ("Bad --candidates connection string", 1, "failover", main_str,
             subordinates_str, "--candidates=what,me,worry?"),
            ("Not enough privileges - health joe", 1, "health", mock_main1,
             subordinates_str),
            ("Not enough privileges - health jane", 0, "health", mock_main2,
             subordinates_str),
            ("Not enough privileges - switchover jane", 1, "switchover",
             mock_main2, subordinates_str, "--new-main=%s" % subordinate3_conn),
        ]

        test_num = 1
        for case in test_cases:
            comment = "Test case %s - %s" % (test_num, case[0])
            parts = [base_cmd]
            for opt in case[2:]:
                parts.append(opt)
            cmd_str = " ".join(parts)
            res = mutlib.System_test.run_test_case(self, case[1], cmd_str,
                                                   comment)
            if not res:
                raise MUTLibError("%s: failed" % comment)
            test_num += 1

        # Now test to see what happens when main is listed as a subordinate
        comment = "Test case %s - Main listed as a subordinate - literal" % test_num
        cmd_str = "%s health %s %s,%s" % (base_cmd, main_str, subordinates_str, main_conn)
        res = mutlib.System_test.run_test_case(self, 2, cmd_str,
                                               comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)
        test_num += 1

        comment = "Test case %s - Main listed as a subordinate - alias"  % test_num
        cmd_str = "%s health %s %s" % (base_cmd, main_str,
                  "--subordinates=root:root@%s:%s" % \
                    (socket.gethostname().split('.', 1)[0], self.server1.port))
        res = mutlib.System_test.run_test_case(self, 2, cmd_str,
                                               comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)
        test_num += 1

        comment = "Test case %s - Main listed as a candiate - alias" % test_num
        cmd_str = "%s elect %s %s %s" % (base_cmd, main_str,
                  "--candidates=root:root@%s:%s" % \
                    (socket.gethostname().split('.', 1)[0], self.server1.port),
                  subordinates_str)
        res = mutlib.System_test.run_test_case(self, 2, cmd_str,
                                               comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)
        test_num += 1

        # Now we return the topology to its original state for other tests
        rpl_admin.test.reset_topology(self)

        # Mask out non-deterministic data
        rpl_admin.test.do_masks(self)

        self.replace_result("mysqlrpladmin.py: error: New main connection "
                            "values invalid",
                            "mysqlrpladmin.py: error: New main connection "
                            "values invalid\n")
        self.replace_result("ERROR: Main connection values invalid or "
                            "cannot be parsed",
                            "ERROR: Main connection values invalid or "
                            "cannot be parsed\n")
        self.replace_result("ERROR: Subordinate connection values invalid or "
                            "cannot be parsed",
                            "ERROR: Subordinate connection values invalid or "
                            "cannot be parsed\n")
        self.replace_result("ERROR: Candidate connection values invalid or "
                            "cannot be parsed",
                            "ERROR: Candidate connection values invalid or "
                            "cannot be parsed\n")

        return True

    def get_result(self):
        return self.compare(__name__, self.results)
    
    def record(self):
        return self.save_result_file(__name__, self.results)
    
    def cleanup(self):
        try:
            self.server1.exec_query("DROP USER 'joe'@'localhost'")
        except:
            pass
        try:
            self.server1.exec_query("DROP USER 'jane'@'localhost'")
        except:
            pass
        return rpl_admin.test.cleanup(self)



