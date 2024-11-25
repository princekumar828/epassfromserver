from dataloader import load_groundtruth_full
import random
from extract_wild import match_wildcard_with_content
import Levenshtein
import re
import json


def sample_logs(datas, num):
    candidates = random.sample(datas, 1)
    for i in range(num - 1):
        sub_candidates = random.sample(datas, num)
        max_dis = 0
        max_log = ""
        for log in sub_candidates:
            sub_min_dis = 10000000000
            for c in candidates:
                dis = Levenshtein.distance(log, c)
                if (sub_min_dis >= dis):
                    sub_min_dis = dis
            if (sub_min_dis > max_dis):
                max_dis = sub_min_dis
                max_log = log
        candidates.append(max_log)
    return candidates


def candidate_set_construction(path, dataset_now, num, rate):
    all_logs = []
    log_template_map = {}
    df_log = load_groundtruth_full(dataset_now, data_path=path)
    print("load done")
    for idx in range(len(df_log)):
        if idx / len(df_log) > rate:
            print(idx)
            break
        all_logs.append(df_log.iloc[idx]['Content'])
        log_template_map[df_log.iloc[idx]['Content']] = df_log.iloc[idx]['EventTemplate']
    rets = {}
    for n in num:
        candidates = sample_logs(all_logs, n)
        ret = []
        for log in candidates:
            ret.append({"content": log, "template": log_template_map[log]})
        rets[n] = ret
    return rets


def template_to_regex(template):
    template = template.replace('\\', '\\\\')
    template = template.replace('(', '\(')
    template = template.replace(')', '\)')
    template = template.replace('[', '\[')
    template = template.replace(']', '\]')
    template = template.replace('{', '\{')
    template = template.replace('}', '\}')
    template = template.replace('.', '\.')
    template = template.replace('+', '\+')
    template = template.replace('?', '\?')
    template = template.replace('|', '\|')
    template = template.replace('^', '\^')
    template = template.replace('$', '\$')

    template = template.replace('<*>', '(.+)')
    template = template.replace('*', '\*')
    regex = template
    regex = '^' + regex + '$'
    return regex


def replace_numbers_with_random(log, template):
    def random_number_replacer(match):
        return str(random.randint(0, 9))

    template, _, wild_content = match_wildcard_with_content(template, log)

    templateL = template.split("<*>")

    idx = 0

    result_str = templateL[idx]

    for i in range(1, len(templateL)):
        result_str += re.sub(r'\d', random_number_replacer, wild_content[i - 1])
        result_str += templateL[i]
    return result_str


def sample_logs_step2(datas, log, template, id, NP, log_template_map):
    regex = template_to_regex(template)
    candidates = random.sample(datas, 32)
    if (id + 50 < len(datas)):
        candidates += datas[id: id + 50]
    else:
        candidates += datas[id: len(datas) - 1]
    if (id - 50 > 0):
        candidates += datas[id - 50: id]
    else:
        candidates += datas[0: id]
    log_candidates = []
    for l in candidates:
        sim = Levenshtein.distance(log, l)
        log_candidates.append([sim, l])

    if (NP == "N"):
        sorted_candidates = sorted(log_candidates, key=lambda x: x[0], reverse=False)
        for tuple in sorted_candidates:
            if (not re.match(regex, tuple[1])):
                if(log_template_map[tuple[1]] != log_template_map[log]):
                    return tuple[1]
    else:
        sorted_candidates = sorted(log_candidates, key=lambda x: x[0], reverse=True)
        for tuple in sorted_candidates:
            if (re.match(regex, tuple[1])):
                if (log_template_map[tuple[1]] == log_template_map[log]):
                    return tuple[1]
    return None


def candidate_set_construction_step2(path, dataset_now, candidate_path, rate):
    with open(f"{candidate_path}/{dataset_now}.json", 'r') as f:
        candidates_pairs = json.load(f)
    candidates = []
    for pair in candidates_pairs:
        candidates.append(pair['content'])

    all_logs = []
    log_template_map = {}
    df_log = load_groundtruth_full(dataset_now, data_path=path)
    id_map = {}
    print("load done")
    for idx in range(len(df_log)):
        if idx / len(df_log) > rate:
            print(idx)
            break
        all_logs.append(df_log.iloc[idx]['Content'])
        log_template_map[df_log.iloc[idx]['Content']] = df_log.iloc[idx]['EventTemplate']
        if (df_log.iloc[idx]['Content'] in candidates):
            id_map[df_log.iloc[idx]['Content']] = idx

    candidates2 = []

    for candidate in candidates:
        print(candidate)
        NE = None
        while (NE is None):
            NE = sample_logs_step2(all_logs, candidate, log_template_map[candidate], id_map[candidate], "N", log_template_map)

        PE = None
        count = 0
        while (PE is None):
            count += 1
            if (count > 10):
                PE = replace_numbers_with_random(candidate, log_template_map[candidate])
                break
            PE = sample_logs_step2(all_logs, candidate, log_template_map[candidate], id_map[candidate], "P", log_template_map)
            if(PE == candidate):
                continue
            if (PE is None):
                continue
        candidates2.append({"content": candidate, "template": log_template_map[candidate], "Postive_Example": PE,
                            "Negative_Example": NE})

    return candidates2

