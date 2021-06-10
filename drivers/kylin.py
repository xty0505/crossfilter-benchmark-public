from logging import setLogRecordFactory
import queue
import traceback
from threading import Thread

import sqlalchemy as sa

from common import util
class IDEBenchDriver:


    def init(self, options, schema, driver_arg):
        self.time_of_latest_request = 0
        self.isRunning = False
        self.requests = queue.LifoQueue()
        # kylin properties
        print("kylin configuration")
        print("kylin host: %s" % driver_arg['host'])
        print("kylin database: %s" % driver_arg['db'])
        print("kylin table name: %s" % driver_arg['table'])
        self.host = driver_arg['host']
        self.port = driver_arg['port']
        self.user = driver_arg['user']
        self.password = driver_arg['password']
        self.db = driver_arg['db']
        self.table = driver_arg['table']
        self.table_to_replace = driver_arg['table_to_replace']

    def workflow_start(self):
        self.isRunning = True
        self.time_of_latest_request = 0
        # connection
        self.kylin_engine = sa.create_engine('kylin://{}:{}@{}:{}/{}'.format(self.user, self.password, self.host,self.port, self.db), connect_args={'is_ssl': False, 'timeout': 60})
        thread = Thread(target=self.process)
        thread.start()

    def workflow_end(self):
        self.isRunning = False
        # close connection when done
        self.conn.close()

    def execute_vizrequest(self, viz_request, options, schema, result_queue):
        print("processsing...")

        viz = viz_request.viz
        sql_statement = viz.get_computed_filter_as_sql(schema)
        # make sql acceptable to kylin
        sql_statement = sql_statement.replace(self.table_to_replace, self.table)
        sql_statement = sql_statement.replace('count', '_count')
        print(sql_statement)

        viz_request.start_time = util.get_current_ms_time()
        self.kylin_engine.execute(sql_statement)
        viz_request.end_time = util.get_current_ms_time()
        print('kylin query time: '+str(viz_request.end_time-viz_request.start_time))

        # write an empty result to the viz_request
        viz_request.result = {}
        # notify IDEBench that processing is done by writing it to the result buffer
        result_queue.put(viz_request)

    def process_request(self, viz_request, options, schema, result_queue):
        self.requests.put((viz_request, options, schema, result_queue))

    def process(self):
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
            except queue.Empty as e:
                # ignore queue-empty exceptions
                print('requests queue empty.')
            except Exception as e:
                traceback.print_exc()
                # kylin 出现未知错误, 重获连接
                self.kylin_engine = sa.create_engine('kylin://{}:{}@{}:{}/{}'.format(self.user, self.password, self.host,self.port, self.db), connect_args={'is_ssl': False, 'timeout': 60})
                pass
