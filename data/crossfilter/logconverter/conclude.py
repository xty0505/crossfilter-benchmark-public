import argparse
import json
import os
from collections import defaultdict
from textwrap import indent
import pandas as pd

query_per_interactions = {
    "flights": 5,
    "movies": 7,
    "weather": 7
}

class Interaction:
    def __init__(self, name, time, selection, predicates=None):
        if predicates is None:
            predicates = {}
        self.name = name
        self.time = time
        self.selection = selection
        self.predicates = predicates
        self.calc_predicates()

    def calc_predicates(self):
        left = self.selection.split('AND')[0].split('>=')[1].strip()
        right = self.selection.split('AND')[1].split('<')[1].strip()
        self.predicates[self.name] = Predicate(self.name, [left, right])

    def is_contained(self, predicates):
        overlapped = []
        for p in predicates:
            if self.predicates.keys().__contains__(p.name) and self.predicates[p.name].is_contained(p):
                overlapped.append(self.predicates[p.name])
        return overlapped


class Predicate:
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def is_contained(self, d):
        if d.value[0] <= self.value[0] and d.value[1] >= self.value[1]:
            return True
        return False


def conclude_workflow(workflow_path, result):
    interactions = []
    interaction_in_session = []  # 某一个 session 中若干 interaction, 一个 interaction 会带来多个 query
    sessions = []
    pre = []

    with open(workflow_path) as f:
        workflows = json.load(f)['interactions']
        if len(workflows) == 0:
            return
        interaction = Interaction(workflows[0]['name'], workflows[0]['time'], workflows[0]['selection'])
        all_predicates = interaction.predicates  # 所有图表的筛选条件
        for key in interaction.predicates.keys():
            pre.append(interaction.predicates[key])
        interactions.append(interaction)
        interaction_in_session.append(interaction)
        for workflow in workflows[1:]:
            interaction = Interaction(workflow['name'], workflow['time'], workflow['selection'], all_predicates)
            all_predicates = interaction.predicates
            overlapped = interaction.is_contained(pre)
            if len(overlapped) > 0:
                interaction_in_session.append(interaction)
                pre = overlapped
            else:
                pre = list(interaction.predicates.values())
                sessions.append(interaction_in_session)
                interaction_in_session = [interaction]
            interactions.append(interaction)
        if len(interaction_in_session) > 0:
            sessions.append(interaction_in_session)
            del interaction_in_session

        dataset = workflow_path.split('/')[-1].split('_')[1]
        query_cnt = len(interactions)*query_per_interactions[dataset]
        print(query_cnt)
        print(len(sessions))
        sum = 0
        freq = defaultdict(lambda: 0)
        for s in sessions:
            sum += len(s)*query_per_interactions[dataset]
            freq[len(s)*query_per_interactions[dataset]] += 1
        print(sum / len(sessions))

        conclusion = {
            "task_id": workflow_path.split('/')[-1],
            "queries": query_cnt,
            "sessions": len(sessions),
            "query_per_session": sum / len(sessions),
            "frequency": freq
        }
        result.append(conclusion)


def conclude_workflows(workflows_dir):
    result = []
    for user_dir in os.listdir(workflows_dir):
        user_dir = workflows_dir + '/' + user_dir
        for task in os.listdir(user_dir):
            print(task + ' start.')
            workflow_path = user_dir + '/' + task
            conclude_workflow(workflow_path, result)
            print(task + ' finished.')
    return result


if __name__ == '__main__':
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--workflows-dir', dest='workflows_dir', help='directory path of workflow')
    argparser.add_argument('--run', dest='run', action='store_true', help='whether to conclude from workflows',
                           default=False)
    argparser.add_argument('--evaluate', dest='evaluate', action='store_true',
                           help='whether to evaluate from conclusion.json', default=False)

    args = vars(argparser.parse_args())

    if args['run']:
        result = conclude_workflows(args['workflows_dir'])
        json.dump(result, open('conclusion.json', 'w'), indent=4)
    elif args['evaluate']:
        df_data = []
        with open('conclusion.json', 'r') as f:
            conclusion = json.load(f)
            for con in conclusion:
                print(con['task_id'])
                dataset = con['task_id'].split('_')[1]
                task = con['task_id'].split('_')[2]
                if task == '0': # skip warm-up workflows
                    continue
                freq = con['frequency']
                for k, v in freq.items():
                    for i in range(v):
                        df_data.append([dataset, task, int(k)])
        f.close()

        df = pd.DataFrame(df_data, columns=['dataset', 'task', 'qps'])
        print(df)
        df_describe = df.groupby(['dataset', 'task'])['qps'].describe().reset_index()
        print(df_describe.to_json(orient='records', indent=4))
        df_describe.to_json('qps_describe.json', orient='records', indent=4)
        
        #     origin = {"queries": 0, "sessions": 0, "query_per_session": 0, "task_cnt": 0}
        #     result = {
        #         "flights": {"0": dict(origin), "1": dict(origin), "2": dict(origin), "3": dict(origin), "4": dict(origin)},
        #         "movies": {"0": dict(origin), "1": dict(origin), "2": dict(origin), "3": dict(origin), "4": dict(origin)},
        #         "weather": {"0": dict(origin), "1": dict(origin), "2": dict(origin), "3": dict(origin), "4": dict(origin)}
        #     }
        #     for con in conclusion:
        #         print(con['task_id'])
        #         dataset = con['task_id'].split('_')[1]
        #         task = con['task_id'].split('_')[2]
        #         result[dataset][task]['queries'] += con['queries']
        #         result[dataset][task]['sessions'] += con['sessions']
        #         result[dataset][task]['task_cnt'] += 1
        #     f.close()
        # datasets = ['flights', 'movies', 'weather']
        # tasks = ['0', '1', '2', '3', '4']
        # for dataset in datasets:
        #     for task in tasks:
        #         result[dataset][task]['queries'] /= result[dataset][task]['task_cnt']
        #         result[dataset][task]['sessions'] /= result[dataset][task]['task_cnt']
        #         result[dataset][task]['query_per_session'] = \
        #             result[dataset][task]['queries'] / result[dataset][task]['sessions']
        # json.dump(result, open('result.json', 'w'), indent=4)
