import json
import re
import os

from dataloader import load_data_full, load_data_2k
from prefix_tree import index_tree
from prompt import EXTRACT_TEMPLATE, MERGE_TEMPLATES
from langchain import PromptTemplate
import pandas as pd
from extract_wild import match_wildcard_with_content, template_invert_index, Jccard_similarity, merge_two_template, \
    cover, merge_wilds, lcs_similarity
import threading
from timeit import default_timer as timer
from PostProcess import correct_single_template, load_regs
from KNN import invert_index
import csv

class LogCluster:
    def __init__(self, logIDL=None, template=""):
        if logIDL is None:
            logIDL = []
        self.template = template
        self.logIDL = logIDL
        self.logs = []
        self.delete = False
        self.single = False
        self.last_log = ""


def get_template(template):
    if(template is None):
        return None

    if ("<END>" not in template):
        template += "<END>"
    if (len(re.findall("<START>", template)) == 1 and len(re.findall("<END>", template)) == 1):
        pattern = re.compile('<START>(.+)<END>')
        result = pattern.findall(template)
        result = result[0]
    else:
        parts = re.split("<START>", template)
        result = ""
        for p in parts:
            if ("<END>" not in p):
                continue
            else:
                result = p.replace("<END>", "")
    return result


class parser:
    def __init__(self, output_dir, dataset, llm, dataset_scale, df_data=None, candidates=None, k=3, pst=0.4):
        self.output_dir = output_dir
        self.dataset = dataset

        if (df_data is not None):
            self.df_log = df_data
        elif (dataset_scale == "full"):
            self.df_log = load_data_full(dataset=dataset, data_path="")
        else:
            self.df_log = load_data_2k(dataset=dataset, data_path="")
        self.CI_tree = index_tree()
        self.llm = llm
        self.prompt_extract = PromptTemplate(template=EXTRACT_TEMPLATE, input_variables=["log", "examples"])
        self.prompt_merge = PromptTemplate(template=MERGE_TEMPLATES, input_variables=["examples", "log1", "log2"])
        self.result_queue = {}
        self.logClusters = []
        self.k = k
        self.Invert_Index = template_invert_index()
        self.KNN = invert_index(candidates)

        self.parsing_logs = []
        self.waiting_idxs = []
        self.template_extract_results = {}
        self.ret = {}
        self.waiting_for_merge_query = []
        self.calls = 0
        self.cidmap = {}
        self.asking = 0
        self.singles = {}
        self.reg_common = load_regs()
        self.pst=pst



    def parse_log_with_LLM(self, idx, log):
        self.calls += 1
        examples = self.KNN.query(log, k=self.k)
        example_prompt = ""
        for example in examples:
            example_prompt += "Log:\n<START>{}<END>\nTemplate:\n<START>{}<END>\n\n".format(example['log'],
                                                                                           example['template'])
        query = self.prompt_extract.format(log=log, examples=example_prompt)
        try:
            response = self.llm.predict(query)
            if(response is None):
                response = re.sub("\d+", "<*>", log)
        except:
            response = re.sub("\d+", "<*>", log)

        template = get_template(template=response)

        template, wildcards, wild_content = match_wildcard_with_content(template, log)
        template = correct_single_template(template, self.reg_common)
        self.asking -= 1

        if(len(re.findall("[^<>* ]",template)) == 0):
            template = self.heuristic_parse(log)

        self.result_queue[idx] = {"type": "template_extract", "template": template, "log": log}

    def parse_result(self, response):
        response = response.lower()
        if ('yes' in response):
            return True
        else:
            return False

    def ask_LLM_whether_merge(self, idx, log1, log2, template_id):
        template_merge, wilds = merge_two_template(log1, self.logClusters[template_id].template)

        if(len(re.findall("[^<>* ]", template_merge))==0):
            response = False
        else:
            examples = self.KNN.query(log1, k=self.k)
            example_prompt = ""
            for example in examples:
                example_prompt += f"Log 1:\n<START>{example['log']}<END>\nLog 2:\n<START>{example['Postive_Example']}<END>\nAnswer:<START>Yes<END>\n\n"
                example_prompt += f"Log 1:\n<START>{example['log']}<END>\nLog 2:\n<START>{example['Negative_Example']}<END>\nAnswer:<START>No<END>\n\n"

            query = self.prompt_merge.format(examples = example_prompt, log1=log1, log2=log2)

            ret = self.llm.predict(query)
            ret = get_template(template=ret)
            response = self.parse_result(ret)

        self.asking -= 1
        self.result_queue[idx] = {"type": "merge_query", "response": response, "template_id": template_id,
                                  "template_merge": template_merge, "log": log1, "template": log2}

    def collect_and_process(self, waiting_now):

        result_now = self.result_queue[waiting_now]
        if (result_now['type'] == 'template_extract'):
            template = result_now["template"]
            template = merge_wilds(template)

            template, wildcards, wild_content = match_wildcard_with_content(template, result_now["log"])
            tag = False
            for cid in range(len(self.logClusters)):
                cluster = self.logClusters[cid]
                if(cluster.delete):
                    continue
                if (template == cluster.template):
                    cluster.logIDL.append(waiting_now)
                    cluster.last_log = result_now['log']
                    self.ret[waiting_now] = cid
                    self.template_extract_results[waiting_now] = template
                    self.CI_tree.insert_template(template, wildcards, wild_content, cid)
                    tag = True
                    break
            if (not tag):
                cid = len(self.logClusters)
                newCluster = LogCluster(logIDL=[waiting_now],
                                        template=template)
                newCluster.last_log = result_now["log"]
                self.logClusters.append(newCluster)
                self.cidmap[cid] = cid

                self.CI_tree.insert_template(template, wildcards, wild_content, cid)
                self.logClusters[cid].logs.append(result_now["log"])
                self.Invert_Index.insert_template(template, cid)
                self.template_extract_results[waiting_now] = template
                self.ret[waiting_now] = cid
                self.check_same_templates(cid)
                self.check_merge_templates(cid)

            self.waiting_idxs.remove(waiting_now)
            self.parsing_logs.remove([waiting_now, result_now["log"]])
            self.result_queue.pop(waiting_now)



        elif (result_now['type'] == 'merge_query'):

            if (result_now['response'] == True):
                cid = result_now['template_id']
                cid_new = self.cidmap[cid]
                template_merge = result_now['template_merge']
                template_merge, wildcards, wild_content = match_wildcard_with_content(template_merge,
                                                                                      result_now['log'])
                template_merge = merge_wilds(template_merge)

                self.CI_tree.insert_template(template_merge, wildcards, wild_content, cid_new)

                self.logClusters[cid_new].logs.append(result_now["log"])
                self.logClusters[cid_new].logIDL.append(waiting_now)
                self.logClusters[cid_new].last_log = result_now['log']

                self.ret[waiting_now] = cid_new

                self.waiting_idxs.remove(waiting_now)
                self.result_queue.pop(waiting_now)

                self.waiting_for_merge_query.remove(cid)
                self.parsing_logs.remove([waiting_now, result_now["log"]])
                self.template_extract_results[waiting_now] = template_merge
                self.check_same_templates(cid_new)
                self.check_merge_templates(cid_new)
            else:
                match_id = self.CI_tree.retrieval_template(result_now['log'])
                if (match_id != -1):
                    match_id = self.cidmap[match_id]
                    self.waiting_idxs.remove(waiting_now)
                    self.result_queue.pop(waiting_now)
                    self.waiting_for_merge_query.remove(result_now['template_id'])
                    self.parsing_logs.remove([waiting_now, result_now["log"]])
                    self.logClusters[match_id].logIDL.append(waiting_now)
                    self.logClusters[match_id].last_log = result_now['log']
                    self.ret[waiting_now]=match_id
                    self.template_extract_results[waiting_now] = self.logClusters[match_id].template
                else:
                    if ([waiting_now, result_now['log']] not in self.parsing_logs):
                        self.parsing_logs.append([waiting_now, result_now['log']])
                    self.result_queue.pop(waiting_now)

                    thread = threading.Thread(target=self.parse_log_with_LLM,
                                              args=(waiting_now, result_now['log']))
                    thread.daemon = True
                    self.asking += 1
                    thread.start()
        elif (result_now['type'] == 'waiting_for_merge_query'):
            match_id = self.CI_tree.retrieval_template(result_now['log'])
            if (match_id != -1):
                match_id = self.cidmap[match_id]
                self.logClusters[match_id].logIDL.append(waiting_now)
                self.logClusters[match_id].last_log = result_now['log']
                self.ret[waiting_now] = match_id
                self.waiting_idxs.remove(waiting_now)
                self.result_queue.pop(waiting_now)
                self.template_extract_results[waiting_now] = self.logClusters[match_id].template
            else:
                cover_tag, template_merge, wilds = cover(result_now['log'],
                                                         self.logClusters[result_now['wait_template_id']].template)
                if (cover_tag):
                    cid = result_now['wait_template_id']
                    cid = self.cidmap[cid]
                    self.logClusters[cid].logIDL.append(waiting_now)
                    self.logClusters[cid].last_log = result_now['log']
                    self.ret[waiting_now] = cid
                    self.waiting_idxs.remove(waiting_now)
                    self.result_queue.pop(waiting_now)
                    self.template_extract_results[waiting_now] = self.logClusters[cid].template
                else:
                    if ([waiting_now, result_now['log']] not in self.parsing_logs):
                        self.parsing_logs.append([waiting_now, result_now['log']])
                    self.waiting_for_merge_query.append(result_now['wait_template_id'])
                    self.result_queue.pop(waiting_now)
                    thread = threading.Thread(target=self.ask_LLM_whether_merge,
                                              args=(waiting_now,
                                                    result_now['log'],
                                                    self.logClusters[result_now['wait_template_id']].last_log,
                                                    result_now['wait_template_id']))
                    thread.daemon = True
                    self.asking += 1
                    thread.start()



        elif (result_now['type'] == "waiting"):

            match_id = self.CI_tree.retrieval_template(result_now['log'])
            if (match_id != -1):
                match_id = self.cidmap[match_id]
                self.logClusters[match_id].logIDL.append(waiting_now)
                self.logClusters[match_id].last_log = result_now['log']
                self.ret[waiting_now] = match_id
                self.waiting_idxs.remove(waiting_now)
                self.result_queue.pop(waiting_now)
            else:
                cover_tag, template_merge, wilds = cover(result_now['log'],
                                                         self.template_extract_results[result_now['wait_log_id']])
                if (cover_tag):
                    cid = self.ret[result_now['wait_log_id']]
                    cid = self.cidmap[cid]
                    self.logClusters[cid].logIDL.append(waiting_now)
                    self.logClusters[cid].last_log = result_now['log']
                    self.ret[waiting_now] = cid
                    self.waiting_idxs.remove(waiting_now)
                    self.result_queue.pop(waiting_now)
                else:
                    self.result_queue.pop(waiting_now)
                    if ([waiting_now, result_now['log']] not in self.parsing_logs):
                        self.parsing_logs.append([waiting_now, result_now['log']])
                    self.waiting_for_merge_query.append(self.cidmap[self.ret[result_now['wait_log_id']]])
                    cid = self.cidmap[self.ret[result_now['wait_log_id']]]
                    thread = threading.Thread(target=self.ask_LLM_whether_merge,
                                              args=(waiting_now,
                                                    result_now['log'],
                                                    self.logClusters[cid].last_log,
                                                    cid))
                    thread.daemon = True
                    self.asking += 1
                    thread.start()

    def check_same_templates(self, cid):
        candidate_cluster = [cid]

        for id in range(len(self.logClusters)):
            if (id in candidate_cluster):
                continue
            if (self.logClusters[id].delete):
                continue
            cover_tag, template_merge, wilds = cover(self.logClusters[cid].template, self.logClusters[id].template)
            if (cover_tag):
                candidate_cluster.append(id)

        if (len(candidate_cluster) == 1):
            return

        for id in candidate_cluster:
            if (id == cid):
                continue
            self.cidmap[id] = self.cidmap[cid]
            self.logClusters[id].delete = True
            self.logClusters[cid].logIDL = self.logClusters[cid].logIDL + self.logClusters[id].logIDL
            self.logClusters[cid].logs = self.logClusters[cid].logs + self.logClusters[id].logs

        self.CI_tree = index_tree()
        self.Invert_Index = template_invert_index()

        for id in range(len(self.logClusters)):
            cluster = self.logClusters[id]
            if (cluster.delete):
                continue
            if (cluster.single):
                continue
            for log in cluster.logs:
                template, wildcards, wild_content = match_wildcard_with_content(cluster.template, log)
                self.CI_tree.insert_template(template, wildcards, wild_content, id)

            self.Invert_Index.insert_template(self.logClusters[id].template, id)

    def check_merge_templates(self, cid):
        most_similar_index = self.Invert_Index.query(self.logClusters[cid].template, 1)
        if (most_similar_index):
            most_similar_cid, score = most_similar_index[0]
        else:
            most_similar_cid = -1
            score = 0
        if (most_similar_cid == -1):
            return
        most_similar_template = self.logClusters[most_similar_cid].template
        template_merge, _ = merge_two_template(most_similar_template, self.logClusters[cid].template)
        template_merge = merge_wilds(template_merge)

        extra_wild = len(re.findall("<\*>", template_merge)) - len(
            re.findall("<\*>", self.logClusters[most_similar_cid].template))

        if (extra_wild == 0):
            return
        candidate_cluster = [cid, most_similar_cid]

        for id in range(len(self.logClusters)):
            if (id in candidate_cluster):
                continue
            if (self.logClusters[id].delete):
                continue
            cover_tag, template_merge, wilds = cover(template_merge, self.logClusters[id].template)
            if (cover_tag):
                candidate_cluster.append(id)

        if (len(candidate_cluster) / extra_wild < 5):
            return

        self.logClusters[cid].template = template_merge

        for id in candidate_cluster:
            if (id == cid):
                continue
            self.cidmap[id] = self.cidmap[cid]
            self.logClusters[id].delete = True
            self.logClusters[cid].logIDL = self.logClusters[cid].logIDL + self.logClusters[id].logIDL
            self.logClusters[cid].logs = self.logClusters[cid].logs + self.logClusters[id].logs

        self.CI_tree = index_tree()
        self.Invert_Index = template_invert_index()

        for id in range(len(self.logClusters)):
            cluster = self.logClusters[id]
            if (cluster.delete):
                continue
            if (cluster.single):
                continue
            for log in cluster.logs:
                template, wildcards, wild_content = match_wildcard_with_content(cluster.template, log)
                self.CI_tree.insert_template(template, wildcards, wild_content, id)
            self.Invert_Index.insert_template(self.logClusters[id].template, id)

    def heuristic_parse(self, log):
        parts = re.split(r'(\W+)', log)
        result = [("<*>" if re.search(r'\d', part) else part) for part in parts]
        return ''.join(result)

    def process_single(self, idx, log):
        if (" " in log):
            return False
        else:
            template = self.heuristic_parse(log)
            if (template not in self.singles.keys()):
                cid = len(self.logClusters)
                newCluster = LogCluster(logIDL=[idx],
                                        template=template)
                newCluster.last_log = log
                newCluster.single = True
                self.logClusters.append(newCluster)
                self.cidmap[cid] = cid
                self.singles[template] = cid
                self.logClusters[cid].logs.append(log)
                self.template_extract_results[idx] = template
                self.ret[idx] = cid
            else:
                cid = self.cidmap[self.singles[template]]
                self.logClusters[cid].logIDL.append(idx)
                self.logClusters[cid].last_log = log
                self.ret[idx] = cid
            return True

    def parse(self):
        print("=============== Parsing logs on dataset {} =================.".format(self.dataset))
        time_start = timer()

        for idx, line in self.df_log.iterrows():

            if idx % 100000 == 0:
                print('Processed {0:.1f}% of log lines.'.format(idx * 100.0 / len(self.df_log)))

            logmessage = line['Content'].strip()
            logmessage = re.sub("\s+", " ", logmessage)

            if (self.process_single(idx, logmessage)):
                continue

            match_id = self.CI_tree.retrieval_template(logmessage)

            if (match_id != -1):

                match_id = self.cidmap[match_id]
                self.logClusters[match_id].logIDL.append(idx)
                self.logClusters[match_id].last_log = logmessage
                self.ret[idx] = match_id
            else:

                max_score = 0
                max_id = -1

                for id, template in self.parsing_logs:
                    score = Jccard_similarity(logmessage, template)
                    if (score > max_score):
                        max_score = score
                        max_id = id
                if (max_score > self.pst):
                    self.result_queue[idx] = {"type": "waiting", "wait_log_id": max_id, "log": logmessage}
                    self.waiting_idxs.append(idx)
                else:
                    most_similar_index = self.Invert_Index.query(logmessage, 1)
                    if (most_similar_index):
                        most_similar_cid, score = most_similar_index[0]
                        most_similar_cid = self.cidmap[most_similar_cid]
                    else:
                        most_similar_cid = -1
                        score = 0

                    if (score > self.pst):
                        if (most_similar_cid not in self.waiting_for_merge_query):
                            thread = threading.Thread(target=self.ask_LLM_whether_merge,
                                                      args=(idx, logmessage, self.logClusters[most_similar_cid].last_log, most_similar_cid))
                            thread.daemon = True
                            self.asking += 1
                            thread.start()
                            self.waiting_idxs.append(idx)
                            self.waiting_for_merge_query.append(most_similar_cid)
                            self.parsing_logs.append([idx, logmessage])
                        else:
                            self.result_queue[idx] = {"type": "waiting_for_merge_query",
                                                      "wait_template_id": most_similar_cid, "log": logmessage}
                            self.waiting_idxs.append(idx)
                    else:
                        self.parsing_logs.append([idx, logmessage])
                        self.waiting_idxs.append(idx)
                        thread = threading.Thread(target=self.parse_log_with_LLM,
                                                  args=(idx, logmessage))
                        thread.daemon = True
                        self.asking += 1
                        thread.start()

            while (self.waiting_idxs):

                waiting_now = self.waiting_idxs[0]
                if (waiting_now not in self.result_queue.keys()):
                    if (self.asking > 50):
                        continue
                    break
                else:
                    self.collect_and_process(waiting_now)



        print('Processed 100% of log lines.')
        print(f"parsing_logs: {len(self.parsing_logs)}")
        print(f"time: {timer() - time_start}")
        print(f"llm now: {self.asking}")

        if (self.waiting_idxs):
            print(f"waiting for {self.waiting_idxs[0]}")
        a = 0
        while (self.waiting_idxs):
            waiting_now = self.waiting_idxs[0]

            if (waiting_now not in self.result_queue.keys()):
                continue
            else:
                a += 1
                self.collect_and_process(waiting_now)

        time_end = timer()
        print(f"total time: {time_end - time_start}")
        print(f"call llm for {self.calls} times")
        self.outputResults()

    def outputResults(self):

        if (not os.path.exists(self.output_dir)):
            os.makedirs(self.output_dir)

        filename = self.dataset
        df_event = []
        ids = [-1] * self.df_log.shape[0]
        templates = [""] * self.df_log.shape[0]

        new_cid = 0

        for cid in range(len(self.logClusters)):
            if (self.logClusters[cid].delete):
                continue
            cluster = self.logClusters[cid]
            df_event.append([new_cid, cluster.template, len(cluster.logIDL)])

            for id in cluster.logIDL:
                ids[id] = new_cid
                templates[id] = cluster.template
            new_cid += 1

        df_event = pd.DataFrame(df_event, columns=['EventId', 'EventTemplate', 'Occurrences'])

        self.df_log['EventId'] = ids
        self.df_log['EventTemplate'] = templates
        try:
            self.df_log.to_csv(os.path.join(self.output_dir, filename + '_structured.csv'), index=False,
                               encoding="utf-8")
        except:
            self.df_log.to_csv(os.path.join(self.output_dir, filename + '_structured.csv'), index=False,
                               encoding="utf-8", quotechar='"', quoting=csv.QUOTE_MINIMAL, escapechar='\\')
        try:
            df_event.to_csv(os.path.join(self.output_dir, filename + '_templates.csv'), index=False, encoding="utf-8")
        except:
            df_event.to_csv(os.path.join(self.output_dir, filename + '_templates.csv'), index=False, encoding="utf-8",
                            quotechar='"', quoting=csv.QUOTE_MINIMAL, escapechar='\\')
