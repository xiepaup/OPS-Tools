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
import replicate
import mutlib
import socket
from mysql.utilities.exception import MUTLibError

class test(replicate.test):
    """check error conditions
    This test ensures the known error conditions are tested. It uses the
    cloneuser test as a parent for setup and teardown methods.
    """

    def check_prerequisites(self):
        return replicate.test.check_prerequisites(self)

    def setup(self):
        self.server3 = None
        return replicate.test.setup(self)

    def run(self):
        self.res_fname = "result.txt"

        main_str = "--main=%s" % self.build_connection_string(self.server2)
        subordinate_str = " --subordinate=%s" % self.build_connection_string(self.server1)
        conn_str = main_str + subordinate_str

        cmd_str = "mysqlreplicate.py "

        comment = "Test case 1 - error: cannot parse server (subordinate)"
        res = mutlib.System_test.run_test_case(self, 2, cmd_str +
                        main_str + " --subordinate=wikiwokiwonky "
                        "--rpl-user=rpl:whatsit", comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        comment = "Test case 2 - error: cannot parse server (main)"
        res = mutlib.System_test.run_test_case(self, 2, cmd_str +
                        subordinate_str + " --main=wikiwakawonky " +
                        "--rpl-user=rpl:whatsit", comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        comment = "Test case 3 - error: invalid login to server (main)"
        res = mutlib.System_test.run_test_case(self, 1, cmd_str +
                        subordinate_str + " --main=nope@nada:localhost:5510 " +
                        "--rpl-user=rpl:whatsit", comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        conn_values = self.get_connection_values(self.server1)
        
        comment = "Test case 4 - error: invalid login to server (subordinate)"
        res = mutlib.System_test.run_test_case(self, 1, cmd_str +
                        main_str + " --subordinate=nope@nada:localhost:5511 " +
                        "--rpl-user=rpl:whatsit", comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        str = self.build_connection_string(self.server1)
        same_str = "--main=%s --subordinate=%s " % (str, str)

        comment = "Test case 5a - error: subordinate and main same machine"
        res = mutlib.System_test.run_test_case(self, 2, cmd_str +
                        same_str + "--rpl-user=rpl:whatsit", comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        str = self.build_connection_string(self.server1)
        same_str = "--main=%s --subordinate=root:root@%s:%s " % \
                   (str, socket.gethostname().split('.', 1)[0],
                    self.server1.port)
        comment = "Test case 5b - error: subordinate and main same alias/host"
        res = mutlib.System_test.run_test_case(self, 2, cmd_str +
                        same_str + "--rpl-user=rpl:whatsit", comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        # Now we must muck with the servers. We need to turn binary logging
        # off for the next test case.

        self.port3 = int(self.servers.get_next_port())
        res = self.servers.start_new_server(self.server0, 
                                            self.port3,
                                            self.servers.get_next_id(),
                                            "root", "temprep1")
        self.server3 = res[0]
        if not self.server3:
            raise MUTLibError("%s: Failed to create a new subordinate." % comment)

        new_server_str = self.build_connection_string(self.server3)
        new_main_str = self.build_connection_string(self.server1)
        
        cmd_str = "mysqlreplicate.py --main=%s " % new_server_str
        cmd_str += subordinate_str
        
        comment = "Test case 6 - error: No binary logging on main"
        cmd = cmd_str + "--rpl-user=rpl:whatsit "
        res = mutlib.System_test.run_test_case(self, 1, cmd, comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        self.server3.exec_query("CREATE USER dummy@localhost")
        self.server3.exec_query("GRANT SELECT ON *.* TO dummy@localhost")
        self.server1.exec_query("CREATE USER dummy@localhost")
        self.server1.exec_query("GRANT SELECT ON *.* TO dummy@localhost")

        comment = "Test case 7 - error: replicate() fails"
        
        conn = self.get_connection_values(self.server3)
        
        cmd = "mysqlreplicate.py --subordinate=dummy@localhost"
        if conn[3] is not None:
            cmd += ":%s" % conn[3]
        if conn[4] is not None and conn[4] != "":
            cmd +=  ":%s" % conn[4]
        cmd += " --rpl-user=rpl:whatsit --main=" + new_main_str 
        res = mutlib.System_test.run_test_case(self, 1, cmd, comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)
            
        cmd_str = "mysqlreplicate.py %s %s" % (main_str, subordinate_str)

        res = self.server2.show_server_variable("server_id")
        if not res:
            raise MUTLibError("Cannot get main's server id.")
        main_serverid = res[0][1]
        
        self.server2.exec_query("SET GLOBAL server_id = 0")
        
        comment = "Test case 8 - error: Main server id = 0"
        cmd = cmd_str + "--rpl-user=rpl:whatsit "
        res = mutlib.System_test.run_test_case(self, 1, cmd, comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        self.server2.exec_query("SET GLOBAL server_id = %s" % main_serverid)
            
        res = self.server1.show_server_variable("server_id")
        if not res:
            raise MUTLibError("Cannot get subordinate's server id.")
        subordinate_serverid = res[0][1]
        
        self.server1.exec_query("SET GLOBAL server_id = 0")
        
        comment = "Test case 9 - error: Subordinate server id = 0"
        cmd = cmd_str + "--rpl-user=rpl:whatsit "
        res = mutlib.System_test.run_test_case(self, 1, cmd, comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        self.server1.exec_query("SET GLOBAL server_id = %s" % subordinate_serverid)

        comment = "Test case 10 - --main-log-pos but no log file"
        cmd_opts = "--main-log-pos=96 "
        res = mutlib.System_test.run_test_case(self, 2, cmd+cmd_opts, comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        comment = "Test case 11 - --main-log-file and --start-from-beginning"
        cmd_opts = "--main-log-file='mysql_bin.00005' --start-from-beginning"
        res = mutlib.System_test.run_test_case(self, 2, cmd+cmd_opts, comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        comment = "Test case 12 - --main-log-pos and --start-from-beginning"
        cmd_opts = "--main-log-pos=96 --start-from-beginning"
        res = mutlib.System_test.run_test_case(self, 2, cmd+cmd_opts, comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        comment = "Test case 13 - --main-log-file+pos and --start-from-beginning"
        cmd_opts = "--main-log-pos=96 --start-from-beginning "
        cmd_opts += "--main-log-file='mysql_bin.00005'"
        res = mutlib.System_test.run_test_case(self, 2, cmd+cmd_opts, comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        # Mask known platform-dependent lines
        self.mask_result("Error 2005:", "(1", '#######')
        self.replace_substring(" (42000)", "")
        self.replace_result("ERROR: Query failed. 1227: Access denied;",
                            "ERROR: Query failed. 1227: Access denied;\n")

        self.replace_result("Error 2002: Can't connect to",
                            "Error ####: Can't connect to local MySQL server "
                            "####...\n")

        self.replace_result("Error 2003: Can't connect to",
                            "Error ####: Can't connect to local MySQL server "
                            "####...\n")
        self.replace_result("ERROR: Query failed. 1227",
                            "ERROR: Query failed. 1227: Access denied;\n")

        self.replace_result("mysqlreplicate.py: error: Main connection "
                            "values invalid",
                            "mysqlreplicate.py: error: Main connection "
                            "values invalid\n")
        self.replace_result("mysqlreplicate.py: error: Subordinate connection "
                            "values invalid",
                            "mysqlreplicate.py: error: Subordinate connection "
                            "values invalid\n")

        return True

    def get_result(self):
        return self.compare(__name__, self.results)
    
    def record(self):
        return self.save_result_file(__name__, self.results)
    
    def cleanup(self):
        if self.server3:
            res = self.servers.stop_server(self.server3)
            self.servers.remove_server(self.server3.role)
            self.server3 = None
        return replicate.test.cleanup(self)



