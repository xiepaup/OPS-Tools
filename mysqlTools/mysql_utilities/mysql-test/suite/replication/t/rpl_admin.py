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
from mysql.utilities.exception import MUTLibError

_DEFAULT_MYSQL_OPTS = '"--log-bin=mysql-bin --report-host=localhost --report-port=%s "'

class test(mutlib.System_test):
    """test replication administration commands
    This test runs the mysqlrpladmin utility on a known topology.
    
    Note: this test will run against older servers. See rpl_admin_gtid
    test for test cases for GTID enabled servers.
    """

    def check_prerequisites(self):
        if self.servers.get_server(0).check_version_compat(5, 6, 5):
            raise MUTLibError("Test requires server version prior to 5.6.5")
        return self.check_num_servers(1)

    def spawn_server(self, name, mysqld=None, kill=False):
        index = self.servers.find_server_by_name(name)
        if index >= 0 and kill:
            server = self.servers.get_server(index)
            if self.debug:
                print "# Killing server %s." % server.role
            self.servers.stop_server(server)
            self.servers.remove_server(server.role)
            index = -1
        if self.debug:
            print "# Spawning %s" % name
        if index >= 0:
            if self.debug:
                print "# Found it in the servers list."
            server = self.servers.get_server(index)
            try:
                res = server.show_server_variable("server_id")
            except MUTLibError, e:
                raise MUTLibError("Cannot get replication server " +
                                   "server_id: %s" % e.errmsg)
        else:
            if self.debug:
                print "# Cloning server0."
            serverid = self.servers.get_next_id()
            if mysqld is None:
                mysqld = _DEFAULT_MYSQL_OPTS % self.servers.view_next_port()
            res = self.servers.spawn_new_server(self.server0, serverid,
                                                name, mysqld)
            if not res:
                raise MUTLibError("Cannot spawn replication server '%s'." &
                                  name)
            self.servers.add_new_server(res[0], True)
            server = res[0]
            
        return server

    def setup(self):
        self.res_fname = "result.txt"
        
        # Spawn servers
        self.server0 = self.servers.get_server(0)
        self.server1 = self.spawn_server("rep_main")
        self.server2 = self.spawn_server("rep_subordinate1")
        self.server3 = self.spawn_server("rep_subordinate2")
        self.server4 = self.spawn_server("rep_subordinate3")

        self.m_port = self.server1.port
        self.s1_port = self.server2.port
        self.s2_port = self.server3.port
        self.s3_port = self.server4.port
        
        for subordinate in [self.server2, self.server3, self.server4]:
            subordinate.exec_query("GRANT REPLICATION SLAVE ON *.* TO "
                              "'rpl'@'localhost' IDENTIFIED BY 'rpl'")

        # Form replication topology - 1 main, 3 subordinates
        return self.reset_topology()

    def run(self):
        
        cmd_str = "mysqlrpladmin.py %s " % self.main_str
        
        main_conn = self.build_connection_string(self.server1).strip(' ')
        subordinate1_conn = self.build_connection_string(self.server2).strip(' ')
        subordinate2_conn = self.build_connection_string(self.server3).strip(' ')
        subordinate3_conn = self.build_connection_string(self.server4).strip(' ')
        
        subordinates_str = ",".join([subordinate1_conn, subordinate2_conn, subordinate3_conn])
        
        comment = "Test case 1 - show health before switchover"
        cmd_opts = " --subordinates=%s --format=vertical health" % subordinates_str
        res = mutlib.System_test.run_test_case(self, 0, cmd_str+cmd_opts,
                                               comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)
            
        # Build connection string with loopback address instead of localhost
        subordinate_ports = [self.server2.port, self.server3.port, self.server4.port]
        subordinates_loopback = "root:root@127.0.0.1:%s," % self.server2.port
        subordinates_loopback += "root:root@127.0.0.1:%s," % self.server3.port
        subordinates_loopback += "root:root@127.0.0.1:%s" % self.server4.port
        subordinate3_conn_ip = subordinate3_conn.replace("localhost", "127.0.0.1")

        # Perform switchover from original main to all other subordinates and back.
        test_cases = [
            # (main, [subordinates_before], candidate, new_main, [subordinates_after])
            (main_conn, [subordinate1_conn, subordinate2_conn, subordinate3_conn],
             subordinate1_conn, "subordinate1", [subordinate2_conn, subordinate3_conn, main_conn]),
            (subordinate1_conn, [subordinate2_conn, subordinate3_conn, main_conn],
             subordinate2_conn, "subordinate2", [subordinate1_conn, subordinate3_conn, main_conn]),
            (subordinate2_conn, [subordinate1_conn, subordinate3_conn, main_conn],
             subordinate3_conn, "subordinate3", [subordinate2_conn, subordinate1_conn, main_conn]),
            (subordinate3_conn_ip, ["root:root@127.0.0.1:%s" % self.server3.port,
                           subordinate1_conn, main_conn],
             main_conn, "main", [subordinate1_conn, subordinate2_conn, subordinate3_conn]),
        ]
        test_num = 2
        for case in test_cases:
            subordinates_str = ",".join(case[1])
            comment = "Test case %s - switchover to %s" % (test_num, case[3])
            cmd_str = "mysqlrpladmin.py --main=%s --rpl-user=rpl:rpl " % case[0]
            cmd_opts = " --new-main=%s --demote-main " % case[2]
            cmd_opts += " --subordinates=%s switchover" % subordinates_str
            res = mutlib.System_test.run_test_case(self, 0, cmd_str+cmd_opts,
                                                   comment)
            if not res:
                raise MUTLibError("%s: failed" % comment)
            test_num += 1
            subordinates_str = ",".join(case[4])
            cmd_str = "mysqlrpladmin.py --main=%s " % case[2]
            comment = "Test case %s - show health after switchover" % test_num
            cmd_opts = " --subordinates=%s --format=vertical health" % subordinates_str
            res = mutlib.System_test.run_test_case(self, 0, cmd_str+cmd_opts,
                                                   comment)
            if not res:
                raise MUTLibError("%s: failed" % comment)
            test_num += 1

        cmd_str = "mysqlrpladmin.py --main=%s " % main_conn
        cmd_opts = " health --disc=root:root "
        cmd_opts += "--subordinates=%s" % subordinates_loopback
        comment= "Test case %s - health with loopback and discovery" % test_num
        res = mutlib.System_test.run_test_case(self, 0, cmd_str+cmd_opts,
                                               comment)
        if not res:
            raise MUTLibError("%s: failed" % comment)
        test_num += 1

        # Perform stop, start, and reset
        commands = ['stop', 'start', 'stop', 'reset']
        for cmd in commands:
            comment = "Test case %s - run command %s" % (test_num, cmd)
            cmd_str = "mysqlrpladmin.py --main=%s " % main_conn
            cmd_opts = " --subordinates=%s %s" % (subordinates_str, cmd)
            res = mutlib.System_test.run_test_case(self, 0, cmd_str+cmd_opts,
                                                   comment)
            if not res:
                raise MUTLibError("%s: failed" % comment)
            test_num += 1
            
        # Now we return the topology to its original state for other tests
        self.reset_topology()

        # Mask out non-deterministic data
        self.do_masks()

        return True

    def do_masks(self):
        self.replace_substring(str(self.m_port), "PORT1")
        self.replace_substring(str(self.s1_port), "PORT2")
        self.replace_substring(str(self.s2_port), "PORT3")
        self.replace_substring(str(self.s3_port), "PORT4")
        
    def reset_topology(self):
        # Form replication topology - 1 main, 3 subordinates
        self.main_str = " --main=%s" % \
                          self.build_connection_string(self.server1)
        for subordinate in [self.server1, self.server2, self.server3, self.server4]:
            try:
                subordinate.exec_query("STOP SLAVE")
                subordinate.exec_query("RESET SLAVE")
            except:
                pass
        
        for subordinate in [self.server2, self.server3, self.server4]:
            subordinate_str = " --subordinate=%s" % self.build_connection_string(subordinate)
            conn_str = self.main_str + subordinate_str
            cmd = "mysqlreplicate.py --rpl-user=rpl:rpl %s" % conn_str
            res = self.exec_util(cmd, self.res_fname)
            if res != 0:
                return False

        return True

    def get_result(self):
        return self.compare(__name__, self.results)
    
    def record(self):
        return self.save_result_file(__name__, self.results)
    
    def cleanup(self):
        if self.res_fname:
            try:
                os.unlink(self.res_fname)
            except:
                pass
        return True

