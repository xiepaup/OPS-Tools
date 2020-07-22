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
from mysql.utilities.exception import MUTLibError

class test(replicate.test):
    """check replication conditions
    This test runs the mysqlrplcheck utility on a known main-subordinate topology.
    It uses the replicate test as a parent for setup and teardown methods.
    """

    def check_prerequisites(self):
        if self.servers.get_server(0).check_version_compat(5, 6, 5):
            raise MUTLibError("Test requires server version prior to 5.6.5")
        return replicate.test.check_prerequisites(self)

    def setup(self):
        return replicate.test.setup(self)

    def run(self):
        self.res_fname = "result.txt"

        main_str = "--main=%s" % self.build_connection_string(self.server2)
        subordinate_str = " --subordinate=%s" % self.build_connection_string(self.server1)
        conn_str = main_str + subordinate_str
        
        cmd = "mysqlreplicate.py --rpl-user=rpl:rpl %s" % conn_str
        try:
            res = self.exec_util(cmd, self.res_fname)
        except MUTLibError, e:
            raise MUTLibError(e.errmsg)

        cmd_str = "mysqlrplcheck.py " + conn_str

        comment = "Test case 1 - normal run"
        res = mutlib.System_test.run_test_case(self, 0, cmd_str, comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)
            
        comment = "Test case 2 - verbose run"
        cmd_opts = " -vv"
        res = mutlib.System_test.run_test_case(self, 0, cmd_str+cmd_opts,
                                                   comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        comment = "Test case 3 - with show subordinate status"
        cmd_opts = " -s"
        res = mutlib.System_test.run_test_case(self, 0, cmd_str+cmd_opts,
                                                   comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)
            
        self.server1.exec_query("STOP SLAVE")
        self.server1.exec_query("CHANGE MASTER TO MASTER_HOST='127.0.0.1'")
        self.server1.exec_query("START SLAVE")

        comment = "Test case 4 - normal run with loopback"
        res = mutlib.System_test.run_test_case(self, 0, cmd_str, comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        self.server2.exec_query("DROP USER rpl@localhost")
        self.server2.exec_query("GRANT REPLICATION SLAVE ON *.* TO rpl@'%'"
                                " IDENTIFIED BY 'rpl'")
        self.server2.exec_query("FLUSH PRIVILEGES")

        comment = "Test case 5 - normal run with grant for rpl@'%'"
        res = mutlib.System_test.run_test_case(self, 0, cmd_str, comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)
            
        self.server2.exec_query("DROP USER rpl@'%'")
        self.server2.exec_query("GRANT REPLICATION SLAVE ON *.* TO rpl@'local%'"
                                " IDENTIFIED BY 'rpl'")
        self.server2.exec_query("FLUSH PRIVILEGES")

        comment = "Test case 6 - normal run with grant with wildcard rpl@'local%'"
        res = mutlib.System_test.run_test_case(self, 0, cmd_str, comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)
            
        self.server2.exec_query("DROP USER rpl@'local%'")
        self.server2.exec_query("GRANT REPLICATION SLAVE ON *.* TO rpl@localhost"
                                " IDENTIFIED BY 'rpl'")
        self.server2.exec_query("FLUSH PRIVILEGES")

        self.do_replacements()

        return True

    def do_replacements(self):
        
        self.replace_result(" main id = ",
                            " main id = XXXXX\n")
        self.replace_result("  subordinate id = ",
                            "  subordinate id = XXXXX\n")
        self.replace_result(" main uuid = ",
                            " main uuid = XXXXX\n")
        self.replace_result("  subordinate uuid = ",
                            "  subordinate uuid = XXXXX\n")
            
        self.replace_result("               Main_Log_File :",
                            "               Main_Log_File : XXXXX\n")
        self.replace_result("           Read_Main_Log_Pos :",
                            "           Read_Main_Log_Pos : XXXXX\n")
        self.replace_result("                   Main_Host :",
                            "                   Main_Host : XXXXX\n")
        self.replace_result("                   Main_Port :",
                            "                   Main_Port : XXXXX\n")
        
        self.replace_result("                Relay_Log_File :",
                            "                Relay_Log_File : XXXXX\n")
        self.replace_result("         Relay_Main_Log_File :",
                            "         Relay_Main_Log_File : XXXXX\n")
        self.replace_result("                 Relay_Log_Pos :",
                            "                 Relay_Log_Pos : XXXXX\n")
        self.replace_result("           Exec_Main_Log_Pos :",
                            "           Exec_Main_Log_Pos : XXXXX\n")
        self.replace_result("               Relay_Log_Space :",
                            "               Relay_Log_Space : XXXXX\n")
        
        self.replace_result("  Main lower_case_table_names:",
                            "  Main lower_case_table_names: XX\n")
        self.replace_result("   Subordinate lower_case_table_names:",
                            "   Subordinate lower_case_table_names: XX\n")
        self.remove_result("   Replicate_Ignore_Server_Ids :")
        self.remove_result("              Main_Server_Id :")
        self.remove_result("                     Heartbeat :")
        self.remove_result("                          Bind :")
        self.remove_result("            Ignored_server_ids :")
    
    def get_result(self):
        return self.compare(__name__, self.results)
    
    def record(self):
        return self.save_result_file(__name__, self.results)
    
    def cleanup(self):
        return replicate.test.cleanup(self)



