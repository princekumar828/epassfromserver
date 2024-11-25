import re


def content2List(content):
    StrList = []
    index = 0
    while (index < len(content)):
        word_now = content[index]
        if (content[index:index + 3] == "<*>"):
            StrList.append("<*>")
            index = index + 3
        elif (content[index:index + 2] == "<*"):
            StrList.append("<*>")
            index = index + 2
        elif (content[index:index + 2] == "*>"):
            StrList.append("<*>")
            index = index + 2
        else:
            StrList.append(word_now)
            index += 1
    return StrList


def lcs(strL1_o, strL2_o):
    strL1 = strL1_o.copy()
    strL2 = strL2_o.copy()
    strL1.reverse()
    strL2.reverse()
    len1 = len(strL1)
    len2 = len(strL2)

    dp = [[0 for column in range(len2 + 1)] for row in range(len1 + 1)]
    trace_back = [["None" for column in range(len2 + 1)] for row in range(len1 + 1)]

    for i in range(len(dp)):
        trace_back[i][0] = 'up'
    for i in range(len(dp[0])):
        trace_back[0][i] = 'left'
    trace_back[0][0] = 'start'

    for i in range(1, len1 + 1):
        for j in range(1, len2 + 1):
            if (strL1[i - 1] == strL2[j - 1]):
                dp[i][j] = dp[i - 1][j - 1] + 1
                trace_back[i][j] = 'diag'
            else:
                if (dp[i - 1][j] >= dp[i][j - 1]):
                    dp[i][j] = dp[i - 1][j]
                    trace_back[i][j] = 'up'
                else:
                    dp[i][j] = dp[i][j - 1]
                    trace_back[i][j] = 'left'
    pairs = []
    i_now = len1
    j_now = len2
    while (trace_back[i_now][j_now] != 'start'):
        if (trace_back[i_now][j_now] == 'diag'):
            pairs.append([i_now - 1, j_now - 1])
            i_now -= 1
            j_now -= 1
        elif (trace_back[i_now][j_now] == 'up'):
            i_now -= 1
        else:
            j_now -= 1
    new_pairs = []
    for pair in pairs:
        if (strL1[pair[0]] != strL2[pair[1]] or strL1[pair[0]] == "<*>"):
            continue
        new_pairs.append([len1 - 1 - pair[0], len2 - 1 - pair[1]])
    return new_pairs


def check_characters(content):
    character_types = set()
    for c in content:
        if (c.isdigit()):
            for i in "0123456789":
                character_types.add(i)
        elif (c.isalpha()):
            alphas = "qwertyuioplkjhgfdsazxcvbnmQWERTYUIOPLKJHGFDSAZXCVBNM"
            for i in alphas:
                character_types.add(i)
        else:
            character_types.add(c)
    return list(character_types)


def process_space_in_wild(tmp):
    wildstr = "<*>"
    if (tmp and tmp[0] == " "):
        tmp = tmp[1:]
        wildstr = " " + wildstr
    if (tmp and tmp[-1] == " "):
        tmp = tmp[:-1]
        wildstr = wildstr + " "
    return tmp, wildstr


def match_wildcard_with_content(template, log):
    if (len(template) > 4 and template[:3] == "<*>" and template[3] != " "):
        template = "<*>" + template[template.find(" "):]
    templateL = content2List("※ " + template + " ※")
    logL = content2List("※ " + log + " ※")
    alignPairL = lcs(logL, templateL)
    T_index = 0
    L_index = 0
    A_index = 0
    template_new = ""
    wildcards = []
    wild_content = []
    while (T_index < len(templateL) and L_index < len(logL) and A_index < len(alignPairL)):
        pair = alignPairL[A_index]
        if (T_index == pair[1] and L_index == pair[0]):
            template_new += templateL[T_index]
            T_index += 1
            L_index += 1
            A_index += 1
            continue

        if ('<*>' in templateL[T_index:pair[1]]):
            tmp = ""
            for i in range(L_index, pair[0]):
                tmp += logL[i]
            tmp, wildstr = process_space_in_wild(tmp)
            if(tmp):
                if(len(re.findall("[a-zA-Z0-9]", tmp)) ==0):
                    template_new += tmp
                else:
                    template_new += wildstr
                    wild_content.append(tmp)
                    wildcards.append(check_characters(tmp))
        else:
            for i in range(L_index, pair[0]):
                template_new += logL[i]
        T_index = pair[1]
        L_index = pair[0]

    template_new = template_new.replace("※", "").strip()

    if (template_new[-3:] == "<*>" and wild_content and wild_content[-1] == ""):
        wild_content = wild_content[:-1]
        wildcards = wildcards[:-1]
        template_new = template_new[:-3].strip()

    return template_new, wildcards, wild_content


def split_content(content, no_delimiters):
    StrList = []
    index = 0
    tmp = ""
    while (index < len(content)):
        word_now = content[index]
        if (content[index:index + 3] == "<*>"):
            if (tmp):
                StrList.append(tmp)
                tmp = ""
            StrList.append("<*>")
            index += 3
        elif (word_now in no_delimiters or word_now.isalnum()):
            tmp += word_now
            index += 1
        else:
            if (tmp):
                StrList.append(tmp)
                tmp = ""
            StrList.append(word_now)
            index += 1
    if (tmp):
        StrList.append(tmp)
    return StrList


class template_invert_index:
    def __init__(self):
        self.word_table = {}
        self.id_table = {}
        self.inverted = {}

    def delete(self, tid):
        self.id_table.pop(tid)
        for token in self.inverted[tid]:
            self.word_table[token].pop(tid)
            if(len(self.word_table[token].keys())==0):
                self.word_table.pop(token)


    def insert_template(self, template, tid):
        templateL = split_content(template, ['-', '_', '/', '.'])
        count = 0
        for token in templateL:
            if (token.isalpha()):
                count += 1
                if (token not in self.word_table.keys()):
                    self.word_table[token] = {}
                if (tid not in self.word_table[token].keys()):
                    self.word_table[token][tid] = 0

                if(tid not in self.inverted.keys()):
                    self.inverted[tid] = []
                if(token not in self.inverted[tid]):
                    self.inverted[tid].append(token)

                self.word_table[token][tid] += 1
        self.id_table[tid] = count

    def query(self, template, k):
        templateL = split_content(template, ['-', '_', '/', '.'])
        result = {}
        count = 0
        for token in templateL:
            if (token.isalpha()):
                count += 1
                if (token in self.word_table.keys()):
                    for tid in self.word_table[token]:
                        if (tid not in result.keys()):
                            result[tid] = {'value': 0, 'contents': {}}
                        if (token not in result[tid]['contents'].keys()):
                            result[tid]['contents'][token] = 0
                        if (result[tid]['contents'][token] < self.word_table[token][tid]):
                            result[tid]['value'] += 1
                            result[tid]['contents'][token] += 1

        ret = {}
        for tid in result.keys():
            ret[tid] = 2 * (result[tid]['value'] * 1.0) / ((self.id_table[tid] + count) * 1.0)
        ret = sorted(ret.items(), key=lambda x: x[1], reverse=True)
        return ret[:k]

def Jccard_similarity(log, template):
    logL_ = split_content(log, ['-', '_', '/', '.'])
    templateL_ = split_content(template, ['-', '_', '/', '.'])

    logL = []
    for l in logL_:
        if (len(re.findall("[a-zA-Z]+", l)) > 0):
            logL.append(l)

    templateL = []
    for l in templateL_:
        if (len(re.findall("[a-zA-Z]+", l)) > 0):
            templateL.append(l)

    length = int((len(logL) + len(templateL)) / 2)
    if (length == 0):
        return 0
    count = 0
    for content in logL:
        if (content in templateL):
            count += 1
    return count / length


def merge_two_template(template1, template2):
    template1L = split_content("※ " + template1 + " ※", [])
    template2L = split_content("※ " + template2 + " ※", [])
    alignPairL = lcs(template1L, template2L)
    T1_index = 0
    T2_index = 0
    A_index = 0
    wilds = []
    template = ""

    while (T1_index < len(template1L) and T2_index < len(template2L) and A_index < len(alignPairL)):
        pair = alignPairL[A_index]
        if (T1_index == pair[0] and T2_index == pair[1]):
            template += template1L[T1_index]
            T1_index += 1
            T2_index += 1
            A_index += 1
        else:
            template += "<*>"
            tmp1 = ""
            for i in range(T1_index, pair[0]):
                tmp1 += template1L[i]
            tmp2 = ""
            for i in range(T2_index, pair[1]):
                tmp2 += template2L[i]
            wilds.append([tmp1, tmp2])
            T1_index = pair[0]
            T2_index = pair[1]
    template = template.replace("※", "").strip()
    return template, wilds


def cover(template1, template2):
    template_merge, wilds = merge_two_template(template1, template2)
    if (len(re.findall('[a-zA-Z0-9]', template_merge)) == 0):
        return False, template_merge, wilds
    if (template1 == template2):
        return True, template_merge, wilds
    for w1, w2 in wilds:
        w1, w2 = delete_common(w1, w2)
        if (w1 != "<*>" and w2 != "<*>"):
            return False, template_merge, wilds
        if (" " in w1 or " " in w2):
            return False, template_merge, wilds
    return True, template_merge, wilds


def delete_common(str1, str2):
    prefix = []
    for c1, c2 in zip(str1, str2):
        if (c1 == c2 and c1 != "<"):
            prefix.append(c1)
        else:
            break
    common_prefix = ''.join(prefix)

    suffix = []
    for c1, c2 in zip(str1[::-1], str2[::-1]):
        if (c1 == c2 and c1 != ">"):
            suffix.append(c1)
        else:
            break
    common_suffix = ''.join(suffix[::-1])
    l1 = len(common_prefix)
    l2 = len(common_suffix)
    if (l1 == 0 and l2 == 0):
        str1 = str1
        str2 = str2
    elif (l2 == 0):
        str1 = str1[l1:]
        str2 = str2[l1:]
    else:
        str1 = str1[l1:l2 * -1]
        str2 = str2[l1:l2 * -1]
    return str1, str2


def contain_alnum(content):
    for ch in content:
        if (ch.isalnum()):
            return True
    return False


def merge_stars(s):
    pattern = r'(<\*>\s){10,}<\*>'
    return re.sub(pattern, '<*>', s)

def merge_wilds(template):
    template_new = ""
    templateL = content2List(template)
    ids = []
    for id in range(len(templateL)):
        ch = templateL[id]
        if (ch == "<*>"):
            ids.append(id)

    for i in range(len(ids) - 1):
        id1 = ids[i]
        id2 = ids[i + 1]
        have_alnum = False
        for j in range(id1, id2):
            if (contain_alnum(templateL[j]) or templateL[j] == " "):
                have_alnum = True
                break
        if (not have_alnum):
            for j in range(id1, id2):
                templateL[j] = ""
    for ch in templateL:
        if (ch == "<*>"):
            if (len(template_new) > 3 and template_new[-3:] == "<*>"):
                continue
        template_new += ch
    template_new = merge_stars(template_new)
    return template_new



def lcs_len(X, Y):
    m = len(X)
    n = len(Y)
    dp = [[0] * (n + 1) for _ in range(m + 1)]

    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if X[i - 1] == Y[j - 1] or X[i - 1] == "<*>" or Y[j - 1] == "<*>":
                dp[i][j] = dp[i - 1][j - 1] + 1
            else:
                dp[i][j] = max(dp[i - 1][j], dp[i][j - 1])

    return dp[m][n]

def lcs_similarity(str1, str2):
    seq1 = str1.split()
    seq2 = str2.split()
    lcs_length = lcs_len(seq1, seq2)
    max_length = max(len(seq1), len(seq2))
    if max_length == 0:
        return 1.0
    return lcs_length / max_length