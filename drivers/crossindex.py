import os
import queue
import time
import sys
import traceback
from threading import Thread

sys.path.extend(['/home/xty/pj/CrossIndex/crossindex'])

import pandas as pd

from common import util
from crossindex_main import CrossIndex
from crossindex_main import Query

class IDEBenchDriver:

    def init(self, options, schema, driver_arg):
        self.time_of_latest_request = 0
        self.isRunning = False
        self.requests = queue.LifoQueue()

        # load cube
        print("crossindex initialization")
        print("table name: %s" % schema.get_fact_table_name())
        print("crossindex name: %s" % driver_arg['name'])
        print("crossindex dimensions: %s" % driver_arg['dimensions'])
        print("crossindex types: %s" % driver_arg['types'])
        print("crossindex cube-dir: %s" % driver_arg['cube_dir'])
        print("crossindex method: %s" % driver_arg['method'])

        self.crossindex = CrossIndex(driver_arg['name'], driver_arg['dimensions'], driver_arg['types'])
        if os.path.exists(driver_arg['cube_dir'] + driver_arg['name'] + '_csv.csv'):
            # self.crossindex.load(driver_arg['cube_dir'], driver_arg['name'])
            self.crossindex.load_csv(driver_arg)
        else:
            raise Exception("no crossindex exist!")
        self.method = driver_arg['method']
        self.cached_q = Query(cube=self.crossindex)
        # self.cnt = 0
        # self.threshold = 4

    def workflow_start(self):
        self.isRunning = True
        self.time_of_latest_request = 0
        thread = Thread(target=self.process)
        thread.start()

    def workflow_end(self):
        self.isRunning = False

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
            except Exception as ex:
                # ignore queue-empty exceptions
                traceback.print_exc()
                pass

    def execute_vizrequest(self, viz_request, options, schema, result_queue):
        print("processsing...")

        # print SQL translation of request and simulate query execution
        sql_statement = viz_request.viz.get_computed_filter_as_sql(schema)
        print(sql_statement)

        q = Query(cube=self.crossindex)
        q.parse(sql_statement)

        # record start time
        viz_request.start_time = util.get_current_ms_time()

        if self.method == "direct":
            res = self.direct_query(q)
        elif self.method == "backward":
            flag, res = self.backward_query(q)
            viz_request.backward = flag
            
        if res == 'dropped':
            viz_request.dropped = True

        # record end time
        viz_request.end_time = util.get_current_ms_time()

        # write an empty result to the viz_request
        viz_request.result = {}

        # notify IDEBench that processing is done by writing it to the result buffer
        result_queue.put(viz_request)

    def direct_query(self, q):
        start = time.time()
        res = self.crossindex.query_csv(q)
        end = time.time()
        print('direct query time:' + str(end - start))
        return res

    def backward_query(self, q):
        start = time.time()
        flag, res = self.crossindex.backward_query_csv(self.cached_q, q)
        end = time.time()
        print('backward query time:' + str(end - start))
        # update cache
        if not flag:
            self.cached_q = q
        return flag, res
