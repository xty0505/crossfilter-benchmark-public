import pymonetdb
import datetime, time
import itertools
import csv
import json
import os
import queue
import multiprocessing
import threading
from threading import Thread
import logging
from subprocess import call
from common import util

logger = logging.getLogger("idebench")
class IDEBenchDriver:

    def init(self, options, schema, driver_arg):
        self.time_of_latest_request = 0
        self.isRunning = False
        self.requests = queue.LifoQueue()

        #monetdb properties
        print("mysql initialization")
        print("mysql db name: %s" % driver_arg['db'])
        print("mysql table name: %s" % driver_arg['table'])
        self.host = driver_arg['host']
        self.port = driver_arg['port']
        self.user = driver_arg['user']
        self.password = driver_arg['password']
        self.db = driver_arg['db']
        self.table = driver_arg['table']
        self.table_to_replace = driver_arg['table-to-replace']
        

    def create_connection(self):
        #conn = pymonetdb.connect(username="monetdb", password="monetdb", hostname="localhost", port=50000, database="crossfilter")
        conn = pymonetdb.connect(username=self.user, password=self.password, hostname=self.host, port=self.port, database=self.db)
        return conn

    def execute_vizrequest(self, viz_request, options, schema, result_queue):

        viz = viz_request.viz
        sql_statement = viz.get_computed_filter_as_sql(schema)
        sql_statement = sql_statement.replace(self.table_to_replace, self.table)
        print(sql_statement)
        
        #calculate connection time
        cursor = self.conn.cursor()
        viz_request.start_time = util.get_current_ms_time()
        cursor.execute(sql_statement)
        # data = cursor.fetchall()
        viz_request.end_time = util.get_current_ms_time()
        print('query time: '+str(viz_request.end_time-viz_request.start_time))

        cursor.close()

        # results = {}
        # for row in data:
        #     keys = []
        #     for i, bin_desc in enumerate(viz.binning):
        #         if "width" in bin_desc:
        #             bin_width = bin_desc["width"]
        #             keys.append(str(int(row[i])))
        #         else:
        #             keys.append(str(row[i]))
        #     key = ",".join(keys)
        #     results[key] = row[len(viz.binning):]
        viz_request.result = {}
        result_queue.put(viz_request)

    def process_request(self, viz_request, options, schema, result_queue):
        self.requests.put((viz_request, options, schema, result_queue))

    def process(self):
        # while the workflow is running, pop the latest request from the stack and execute it
        while self.isRunning:
            try:
                request = self.requests.get(timeout=1)
                viz_request = request[0]
                options = request[1]
                schema = request[2]
                result_queue = request[3]


                # only execute requests that are newer than the last one we processed (drops old/no longer needed queries)
                if viz_request.expected_start_time < self.time_of_latest_request:
                    viz_request.dropped = True
                    result_queue.put(viz_request)
                    continue

                self.time_of_latest_request = viz_request.expected_start_time
                self.execute_vizrequest(viz_request, options, schema, result_queue)
            except:
                # ignore queue-empty exceptions
                pass


    def workflow_start(self):
         # pool a number of db connections
        self.isRunning = True
        self.time_of_latest_request = 0
        # connection       
        self.conn = self.create_connection()
        thread = Thread(target = self.process)
        thread.start()

    def workflow_end(self):
        self.isRunning = False
        # self.conn.close()
