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
from mysql.utilities.exception import MUTLibError

class test(replicate.test):
    """setup replication
    This test attempts to replicate among a main and subordinate whose
    innodb settings are different. It uses the replicate test for
    inherited methods.
    """

    def check_prerequisites(self):
        if self.servers.get_server(0).check_version_compat(5, 5, 0):
            raise MUTLibError("Test requires server version 5.1.")
        return self.check_num_servers(1)

    def setup(self):
        self.server0 = self.servers.get_server(0)
        self.server3 = None
        self.server2 = None
        self.server4 = None
        self.s1_serverid = None
        self.s2_serverid = None
        self.s3_serverid = None
        self.s4_serverid = None

        replicate.test.setup(self)
        
        index = self.servers.find_server_by_name("rep_subordinate_missing_engines")
        if index >= 0:
            self.server3 = self.servers.get_server(index)
            try:
                res = self.server3.show_server_variable("server_id")
            except MUTLibError, e:
                raise MUTLibError("Cannot get replication subordinate " +
                                   "server_id: %s" % e.errmsg)
            self.s3_serverid = int(res[0][1])
        else:
            self.s3_serverid = self.servers.get_next_id()
            res = self.servers.spawn_new_server(self.server0, self.s3_serverid,
                                                "rep_subordinate_missing_engines",
                                              ' --mysqld="--log-bin=mysql-bin '
                                         '--default_storage_engine=blackhole"')
            if not res:
                raise MUTLibError("Cannot spawn replication subordinate server.")
            self.server3 = res[0]
            self.servers.add_new_server(self.server3, True)
            
        index = self.servers.find_server_by_name("rep_main_missing_engines")
        if index >= 0:
            self.server4 = self.servers.get_server(index)
            try:
                res = self.server4.show_server_variable("server_id")
            except MUTLibError, e:
                raise MUTLibError("Cannot get replication main " +
                                   "server_id: %s" % e.errmsg)
            self.s4_serverid = int(res[0][1])
        else:
            self.s4_serverid = self.servers.get_next_id()
            res = self.servers.spawn_new_server(self.server0, self.s4_serverid,
                                               "rep_main_missing_engines",
                                              ' --mysqld="--log-bin=mysql-bin '
                                            '--default_storage_engine=memory"')
            if not res:
                raise MUTLibError("Cannot spawn replication subordinate server.")
            self.server4 = res[0]
            self.servers.add_new_server(self.server4, True)

        return True
    
    def run_test_case(self, subordinate, main, s_id,
                      comment, options=None, expected_result=0):
        
        main_str = "--main=%s" % self.build_connection_string(main)
        subordinate_str = " --subordinate=%s" % self.build_connection_string(subordinate)
        conn_str = main_str + subordinate_str
        
        # Test case 1 - setup replication among two servers
        self.results.append(comment+"\n")
        cmd = "mysqlreplicate.py -vvv --rpl-user=rpl:rpl %s" % conn_str
        if options:
            cmd += " %s" % options
        res = self.exec_util(cmd, self.res_fname)
        self.record_results(self.res_fname)
        if res != expected_result:
            return False

        return True
    
    def run(self):
        self.res_fname = "result.txt"
        
        comment = "Test case 1 - show warnings if subordinate has different " \
                  "default engines"
        res = self.run_test_case(self.server3, self.server2, self.s3_serverid,
                                 comment, None)
        if not res:
            raise MUTLibError("%s: failed" % comment)
            
        comment = "Test case 2 - use pedantic to fail if subordinate has " \
                  "different default engines"
        res = self.run_test_case(self.server3, self.server2, self.s3_serverid,
                                 comment, " --pedantic", 1)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        try:
            res = self.server3.exec_query("STOP SLAVE")
        except:
            raise MUTLibError("%s: Failed to stop subordinate." % comment)


        comment = "Test case 3 - show warnings if main has different " \
                  "default engines"
        res = self.run_test_case(self.server1, self.server4, self.s1_serverid,
                                 comment, None)
        if not res:
            raise MUTLibError("%s: failed" % comment)
        
        comment = "Test case 4 - use pedantic to fail if main has " \
                  "different default engines"
        res = self.run_test_case(self.server1, self.server4, self.s1_serverid,
                                 comment, " --pedantic", 1)
        if not res:
            raise MUTLibError("%s: failed" % comment)

        try:
            res = self.server1.exec_query("STOP SLAVE")
        except:
            raise MUTLibError("%s: Failed to stop subordinate." % comment)

        replicate.test.mask_results(self)

        # Mask out inconsistent results when run on slower machines
        self.remove_result("# status: Queueing main event to the relay log")
        self.remove_result("# error: 0:")
        self.remove_result("# Waiting for subordinate to synchronize with main")
        
        return True

    def get_result(self):
        return self.compare(__name__, self.results)
    
    def record(self):
        return self.save_result_file(__name__, self.results)
    
    def cleanup(self):
        if self.res_fname:
            os.unlink(self.res_fname)
        return True

