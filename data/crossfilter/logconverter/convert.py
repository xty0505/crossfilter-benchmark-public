import csv
import json
import os
import time
import itertools

from optparse import OptionParser

def is_reset(row):
    return row[0] == "" and row[1] == "" and row[2] == ""

def is_brush_start(row):
    return row[2] == "brushStart"

def is_brush(row):
    return (not row[0] == "") and (not row[1] == "") and (row[2] == "brushStart" or row[2] == "brushEnd" or row[2] == "brush")

def get_range(row):
    return min(row[1], row[0]), max(row[1], row[0])

def get_dimension(row):
    return row[6]

def get_timestamp(row):
    return int(row[5])

def create_interaction(viz, time, selection, brush_id):
    return { "name": viz, "time": time, "selection": selection, "metadata": { "brush_id" : brush_id}}

def brush_to_selection(row):
    dimension = get_dimension(row)
    value_range = get_range(row)
    return "%s >= %s AND %s < %s" % (dimension, value_range[0], dimension, value_range[1])

def convert_view(views_file):
    vizs = []
    with open(views_file) as view_file:
        views = json.load(view_file)
        names = []
        for view in views:
            dimension = view["name"]
            names.append(view["name"])
            binning = [signal for signal in view["spec"]["signals"] if signal["name"] == "bin"][0]["value"]
            bin_width = binning["step"]
            reference = binning["start"]
            vizs.append( { "name": dimension,
            "binning": [{
                "dimension": dimension,
                "width": bin_width,
                "reference": reference
                }],
            "perBinAggregates": [
                {
                    "type": "count"
                }
            ]
             })

        for viz in vizs:
            viz["source"] = " and ".join([name for name in names if name != viz["name"]])

    return vizs

def convert_interaction(brush_file):
    interactions = []
    with open(brush_file) as brush_file:
        reader = csv.reader(brush_file, delimiter=",")
        start_time = 0
        brush_id = 0
        for row in reader:
            if is_brush_start(row):
                brush_id += 1
            if is_brush(row):
                if len(interactions) == 0:
                    start_time = get_timestamp(row)

                dimension = get_dimension(row)
                selection = brush_to_selection(row)
                interactions.append(create_interaction(dimension, get_timestamp(row) - start_time, selection, brush_id))

    time_array = time.localtime(start_time / 1000)
    start = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
    return start, interactions

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("--views-file", dest="views_file", action="store", help="the views.json file")
    parser.add_option("--brush-file", dest="brush_file", action="store", help="the brush.csv file")
    parser.add_option("--output", dest="output", action="store", help="the file that stored the generate workflow")
    parser.add_option("--batch", dest="batch", action="store_true", help="whether to convert batch files", default=False)
    parser.add_option("--log_dir", dest="log_dir", action="store", help="the interactions log file directory")

    (options, args) = parser.parse_args()

    # batch convert
    if options.batch:
        if not options.log_dir:
            parser.error("No log file directory specified")

        if not os.path.exists(options.output):
            os.mkdir(options.output)

        for user_dir in os.listdir(options.log_dir):
            interactions_log_dir = options.log_dir + user_dir + '/logs'
            views_log_dir = options.log_dir + user_dir + '/viewLogs'

            for views_file_name in os.listdir(views_log_dir):
                views_file_name_list = views_file_name.split('_')
                brush_file_name = '_'.join([views_file_name_list[0], views_file_name_list[1], views_file_name_list[2], 'brush']) + '.csv'

                views_file = views_log_dir + '/' + views_file_name
                brush_file = interactions_log_dir + '/' + brush_file_name
                print(views_file)
                print(brush_file)

                # convert
                vizs = convert_view(views_file)
                start, interactions = convert_interaction(brush_file)

                # json.dump
                workflow_file_name = '_'.join([views_file_name_list[0], views_file_name_list[1], views_file_name_list[2], 'workflow']) + '.json'
                workflow_dir = options.output + '/' + user_dir.split('_')[1]
                if not os.path.exists(workflow_dir):
                    os.mkdir(workflow_dir)
                with open(workflow_dir + '/' + workflow_file_name, "w") as fp:
                    json.dump({"start": start, "setup": vizs, "interactions": interactions}, fp, indent=4)
    else:
        if not options.views_file:
            parser.error("No views file specified.")

        if not options.brush_file:
            parser.error("No brush file specified.")

        if not options.output:
            parser.error("No output file specified.")

        vizs = convert_view(options.views_file)
        start, interactions = convert_interaction(options.brush_file)

        with open(options.output, "w") as fp:
            json.dump({"start": start, "setup": vizs, "interactions": interactions}, fp, indent=4)
