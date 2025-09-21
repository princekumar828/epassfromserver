import os
import pandas as pd
import re

benchmark_settings = {
    'HDFS': {
        'log_file': 'HDFS/HDFS_full.log',
        'log_format': '<Date> <Time> <Pid> <Level> <Component>: <Content>',
    },

    'Hadoop': {
        'log_file': 'Hadoop/Hadoop_full.log',
        'log_format': '<Date> <Time> <Level> \[<Process>\] <Component>: <Content>',
    },

    'Spark': {
        'log_file': 'Spark/Spark_full.log',
        'log_format': '<Date> <Time> <Level> <Component>: <Content>',
    },

    'Zookeeper': {
        'log_file': 'Zookeeper/Zookeeper_full.log',
        'log_format': '<Date> <Time> - <Level>  \[<Node>:<Component>@<Id>\] - <Content>',
    },
    'OpenStack': {
        'log_file': 'OpenStack/OpenStack_full.log',
        'log_format': '<Logrecord> <Date> <Time> <Pid> <Level> <Component> \[<ADDR>\] <Content>',
    },

    'BGL': {
        'log_file': 'BGL/BGL_full.log',
        'log_format': '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>',
    },

    'HPC': {
        'log_file': 'HPC/HPC_full.log',
        'log_format': '<LogId> <Node> <Component> <State> <Time> <Flag> <Content>',
    },

    'Thunderbird': {
        'log_file': 'Thunderbird/Thunderbird_full.log',
        'log_format': '<Label> <Timestamp> <Date> <User> <Month> <Day> <Time> <Location> <Component>(\[<PID>\])?: <Content>',
    },

    'Linux': {
        'log_file': 'Linux/Linux_full.log',
        'log_format': '<Month> <Date> <Time> <Level> <Component>(\[<PID>\])?: <Content>',
    },

    'Mac': {
        'log_file': 'Mac/Mac_full.log',
        'log_format': '<Month>  <Date> <Time> <User> <Component>\[<PID>\]( \(<Address>\))?: <Content>',
    },

    'HealthApp': {
        'log_file': 'HealthApp/HealthApp_full.log',
        'log_format': '<Time>\|<Component>\|<Pid>\|<Content>',
    },

    'Apache': {
        'log_file': 'Apache/Apache_full.log',
        'log_format': '\[<Time>\] \[<Level>\] <Content>',
    },

    'Proxifier': {
        'log_file': 'Proxifier/Proxifier_full.log',
        'log_format': '\[<Time>\] <Program> - <Content>',
    },

    'OpenSSH': {
        'log_file': 'OpenSSH/OpenSSH_full.log',
        'log_format': '<Date> <Day> <Time> <Component> sshd\[<Pid>\]: <Content>',
    },

   'hbase': {
        'log_file': 'data/hbase/hbase.log',
        'log_format': '<Content>',
    },

    'hive': {
        'log_file': 'data/hive/hive.log',
        'log_format': '<Content>',
    },

    'OpenSearch': {
        'log_file': 'data/OpenSearch/OpenSearch_full.log',
        'log_format': '<Content>',
    },

    'camel': {
        'log_file': 'data/camel/camel.log',
        'log_format': '<Content>',
    },

    'activemq': {
        'log_file': 'data/activemq/activemq.log',
        'log_format': '<Content>',
    },

    'CoreNLP': {
        'log_file': 'data/CoreNLP/CoreNLP.log',
        'log_format': '<Content>',
    },
}

# datasets = ['Apache', 'BGL', 'HDFS', 'HPC', 'Hadoop', 'HealthApp', 'Linux', 'Mac', 'OpenSSH', 'OpenStack', 'Proxifier',
#             'Spark', 'Thunderbird', 'Zookeeper', 'hbase', 'hive', 'OpenSearch', 'camel', 'activemq', 'CoreNLP']

datasets = [ 'hbase', 'hive', 'OpenSearch', 'camel', 'activemq', 'CoreNLP']


def generate_logformat_regex(logformat):
    headers = []
    splitters = re.split(r'(<[^<>]+>)', logformat)
    regex = ''
    for k in range(len(splitters)):
        if k % 2 == 0:
            splitter = re.sub(' +', '\\\s+', splitters[k])
            regex += splitter
        else:
            header = splitters[k].strip('<').strip('>')
            regex += '(?P<%s>.*?)' % header
            headers.append(header)
    regex = re.compile('^' + regex + '$')
    return headers, regex

def log_to_dataframe(log_file, regex, headers):
    log_messages = []
    linecount = 0
    with open(log_file, 'r', encoding='utf-8') as fin:
        lines = fin.readlines()
        for line in lines:
            try:
                match = regex.search(line.strip())
                message = [match.group(header) for header in headers]
                log_messages.append(message)
                linecount += 1
            except Exception as e:
                pass
    logdf = pd.DataFrame(log_messages, columns=headers)
    logdf.insert(0, 'LineId', None)
    logdf['LineId'] = [i + 1 for i in range(linecount)]
    return logdf


def load_data_full(dataset, data_path=""):
    print(f"=============== Loading dataset {dataset} ===============")
    if (dataset not in datasets):
        print("Error: dataset not in datasets")
        return None
    log_path = os.path.join(data_path, benchmark_settings[dataset]['log_file'])
     
    headers, regex = generate_logformat_regex(benchmark_settings[dataset]['log_format'])
    df_log = log_to_dataframe(log_path, regex, headers)
    print(f"============= Loading dataset {dataset} done =============")
    return df_log

def load_groundtruth_full(dataset, data_path=""):
    # ground_truth_path = f"{data_path}/{dataset}/{dataset}_full.log_structured.csv"
    ground_truth_path = f"{data_path}/{dataset}/{dataset}.GeneralAnnotation.csv"
    ground_truth = pd.read_csv(ground_truth_path)
    return ground_truth


benchmark_settings_2k = {
    'HDFS': {
        'log_file': 'HDFS/HDFS_2k.log',
        'log_format': '<Date> <Time> <Pid> <Level> <Component>: <Content>',
    },

    'Hadoop': {
        'log_file': 'Hadoop/Hadoop_2k.log',
        'log_format': '<Date> <Time> <Level> \[<Process>\] <Component>: <Content>',
    },

    'Spark': {
        'log_file': 'Spark/Spark_2k.log',
        'log_format': '<Date> <Time> <Level> <Component>: <Content>',
    },

    'Zookeeper': {
        'log_file': 'Zookeeper/Zookeeper_2k.log',
        'log_format': '<Date> <Time> - <Level>  \[<Node>:<Component>@<Id>\] - <Content>',
    },
    'OpenStack': {
        'log_file': 'OpenStack/OpenStack_2k.log',
        'log_format': '<Logrecord> <Date> <Time> <Pid> <Level> <Component> \[<ADDR>\] <Content>',
    },

    'BGL': {
        'log_file': 'BGL/BGL_2k.log',
        'log_format': '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>',
    },

    'HPC': {
        'log_file': 'HPC/HPC_2k.log',
        'log_format': '<LogId> <Node> <Component> <State> <Time> <Flag> <Content>',
    },

    'Thunderbird': {
        'log_file': 'Thunderbird/Thunderbird_2k.log',
        'log_format': '<Label> <Timestamp> <Date> <User> <Month> <Day> <Time> <Location> <Component>(\[<PID>\])?: <Content>',
    },

    'Windows': {
        'log_file': 'Windows/Windows_2k.log',
        'log_format': '<Date> <Time>, <Level>                  <Component>    <Content>',
    },

    'Linux': {
        'log_file': 'Linux/Linux_2k.log',
        'log_format': '<Month> <Date> <Time> <Level> <Component>(\[<PID>\])?: <Content>',
    },

    'Mac': {
        'log_file': 'Mac/Mac_2k.log',
        'log_format': '<Month>  <Date> <Time> <User> <Component>\[<PID>\]( \(<Address>\))?: <Content>',
    },

    'Android': {
        'log_file': 'Android/Android_2k.log',
        'log_format': '<Date> <Time>  <Pid>  <Tid> <Level> <Component>: <Content>',
    },

    'HealthApp': {
        'log_file': 'HealthApp/HealthApp_2k.log',
        'log_format': '<Time>\|<Component>\|<Pid>\|<Content>',
    },

    'Apache': {
        'log_file': 'Apache/Apache_2k.log',
        'log_format': '\[<Time>\] \[<Level>\] <Content>',
    },

    'Proxifier': {
        'log_file': 'Proxifier/Proxifier_2k.log',
        'log_format': '\[<Time>\] <Program> - <Content>',
    },

    'OpenSSH': {
        'log_file': 'OpenSSH/OpenSSH_2k.log',
        'log_format': '<Date> <Day> <Time> <Component> sshd\[<Pid>\]: <Content>',
    },
    'hbase': {
          'log_file': 'hbase/hbase_2k.log',
          'log_format': '<Content>',
     },
    'hive': {
          'log_file': 'hive/hive_2k.log',
          'log_format': '<Content>',
     },
    'OpenSearch': {
          'log_file': 'OpenSearch/OpenSearch_2k.log',
          'log_format': '<Content>',
     },
    'camel': {
          'log_file': 'camel/camel_2k.log',
          'log_format': '<Content>',
     },
    'activemq': {
          'log_file': 'activemq/activemq_2k.log',
          'log_format': '<Content>',
     },
    'CoreNLP': {
          'log_file': 'CoreNLP/CoreNLP_2k.log',
          'log_format': '<Content>',
     },
    
}



def load_data_2k(dataset, data_path=""):
    print(f"=============== Loading dataset {dataset} ===============")
    if (dataset not in datasets):
        print("Error: dataset not in datasets")
        return None
    log_path = os.path.join(data_path, benchmark_settings_2k[dataset]['log_file'])
    headers, regex = generate_logformat_regex(benchmark_settings_2k[dataset]['log_format'])
    df_log = log_to_dataframe(log_path, regex, headers)
    print(f"============= Loading dataset {dataset} done =============")
    return df_log
