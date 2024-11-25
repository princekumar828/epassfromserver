class tree_node:
    def __init__(self, type, content, stop_ch, wilds, cid):
        self.type = type
        self.content = content
        self.wilds = wilds
        self.stop_ch = stop_ch
        self.next = {}
        self.cid = cid

    def match(self, log):
        if (self.type == "wild"):
            for i in range(len(log)):
                ch = log[i]
                if (ch in self.stop_ch):
                # if (ch not in self.wilds):
                    return -1, True, log[i:]
            return -1, True, ""
        elif (self.type == "constant"):
            if (log[:len(self.content)] == self.content):
                return -1, True, log[len(self.content):]
            else:
                return -1, False, ""
        elif (self.type == "template"):
            if (log != ""):
                return -1, False, ""
            else:
                return self.cid, True, ""
        else:
            return -1, True, log


def segment_template(template, wilds):
    Unit_List = []
    index = 0
    wild_index = 0
    while (index < len(template)):
        word_now = template[index]
        if (template[index:index + 3] == "<*>"):
            Unit_List.append(segment_unit("<*>", wilds[wild_index]))
            wild_index += 1
            index = index + 3
        else:
            Unit_List.append(segment_unit(word_now, None))
            index += 1
    return Unit_List


class segment_unit:
    def __init__(self, content, wilds):
        self.content = content
        self.wilds = wilds


class index_tree:
    def __init__(self):
        self.root = tree_node(type="root", content="", stop_ch=[], wilds=[], cid=-1)

    def retrieval_template(self, log, node_=None):
        if (node_ is None):
            node_now = self.root
        else:
            node_now = node_
        cid, not_fail, log = node_now.match(log)
        if (cid != -1):
            return cid
        if (not not_fail):
            return -1
        if ("template" in node_now.next.keys()):
            cid = self.retrieval_template(log, node_now.next["template"])
            if (cid != -1):
                return cid
        if (log == ""):
            return -1
        if (log[0] in node_now.next.keys()):
            cid = self.retrieval_template(log, node_now.next[log[0]])
            if (cid != -1):
                return cid
        if ("<*>" in node_now.next.keys()):
            cid = self.retrieval_template(log, node_now.next["<*>"])
            if (cid != -1):
                return cid
        return -1

    def insert_template(self, template, wildcards, wild_content, cid):

        template, wilds, wild_content = preprocess_before_insert_into_index(template, wildcards, wild_content)

        if(len(re.findall("[^<>* ]", template)) == 0):
            return

        node_now = self.root
        Unit_List = segment_template(template, wilds)
        Unit_index = 0
        while (Unit_index < len(Unit_List)):
            unit = Unit_List[Unit_index]
            if (unit.content in node_now.next.keys()):
                last_node = node_now
                node_now = node_now.next[unit.content]
                if (node_now.type == "wild"):
                    stop_word = ""
                    if (Unit_index + 1 < len(Unit_List)):
                        stop_word = Unit_List[Unit_index + 1].content
                    if (stop_word and stop_word in node_now.wilds):
                        wild_ = node_now.wilds.copy()
                        wild_.remove(stop_word)
                        new_node = tree_node(type="wild", content="<*>", stop_ch=[stop_word], wilds=wild_, cid=-1)
                        node_now.wilds.remove(stop_word)
                        new_node2 = tree_node(type="constant", content=stop_word, stop_ch=[], wilds=[], cid=-1)
                        new_node.next[stop_word] = new_node2
                        new_node2.next["<*>"] = node_now
                        node_now = new_node
                        last_node.next["<*>"] = node_now
                    if(stop_word not in node_now.stop_ch):
                        node_now.stop_ch.append(stop_word)
                    p=False
                    for symbol in unit.wilds:
                        if(symbol.isalnum()):
                            continue
                        if(symbol in node_now.next.keys()):
                            p=True
                            while(1):
                                node_now.wilds = node_now.wilds + [item for item in unit.wilds if
                                                                   item not in (node_now.wilds + [symbol])]
                                if(symbol in node_now.next.keys() and "<*>" in node_now.next[symbol].next.keys() and node_now.next[symbol].content==symbol):
                                    node_now=node_now.next[symbol].next["<*>"]
                                else:
                                    break
                            break
                    if(not p):
                        node_now.wilds = node_now.wilds + [item for item in unit.wilds if item not in node_now.wilds]
                    Unit_index += 1
                elif (node_now.type == "constant"):
                    content_to_match = node_now.content
                    ch_index = 0
                    while (ch_index < len(content_to_match)):
                        if (Unit_index >= len(Unit_List)):
                            node_now.content = content_to_match[:ch_index]
                            new_node = tree_node(type="constant", content=content_to_match[ch_index:], stop_ch=[],
                                                 wilds=[], cid=-1)
                            new_node.next = node_now.next
                            node_now.next = {content_to_match[ch_index:][0]: new_node}
                            new_node = tree_node(type="template", content=template, stop_ch=[], wilds=[], cid=cid)
                            node_now.next['template'] = new_node
                            return

                        ch1 = content_to_match[ch_index]
                        ch2 = Unit_List[Unit_index].content
                        if (ch1 == ch2):
                            ch_index += 1
                            Unit_index += 1
                        else:
                            node_now.content = content_to_match[:ch_index]
                            new_node = tree_node(type="constant", content=content_to_match[ch_index:], stop_ch=[],
                                                 wilds=[], cid=-1)
                            new_node.next = node_now.next
                            node_now.next = {content_to_match[ch_index:][0]: new_node}
                            break
            else:
                tmp = ""
                for i in range(Unit_index, len(Unit_List)):
                    unit = Unit_List[i]
                    if (unit.content != "<*>"):
                        tmp += unit.content
                    else:
                        if (tmp):
                            new_node = tree_node(type="constant", content=tmp, stop_ch=[], wilds=[], cid=-1)
                            node_now.next[tmp[0]] = new_node
                            tmp = ""
                            node_now = new_node

                        stop_ch = []
                        if (i + 1 < len(Unit_List)):
                            stop_ch.append(Unit_List[i + 1].content)
                        new_node = tree_node(type="wild", content="<*>", stop_ch=stop_ch, wilds=unit.wilds, cid=-1)
                        node_now.next["<*>"] = new_node
                        node_now = new_node
                if (tmp):
                    new_node = tree_node(type="constant", content=tmp, stop_ch=[], wilds=[], cid=-1)
                    node_now.next[tmp[0]] = new_node
                    node_now = new_node

                new_node = tree_node(type="template", content=template, stop_ch=[], wilds=[], cid=cid)
                node_now.next['template'] = new_node
                return
        new_node = tree_node(type="template", content=template, stop_ch=[], wilds=[], cid=cid)
        node_now.next['template'] = new_node
        return



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

import re
def preprocess_before_insert_into_index(template, wildcards, wild_content):
    template_new = ""
    wildcards_new = []
    wild_content_new = []
    index = 0
    wild_index = 0
    while (index < len(template)):
        word_now = template[index]
        if (template[index:index + 3] == "<*>"):
            wild_now = wildcards[wild_index]
            wild_content_now = wild_content[wild_index]
            next_word = ""
            if (index + 3 < len(template) and not template[index + 3].isalnum()):
                next_word = template[index + 3]
            if (next_word and next_word in wild_now):
                wild_list = wild_content_now.split(next_word)
                for w in wild_list:
                    template_new += "<*>" + next_word
                    wildcards_new.append(check_characters(w))
                    wild_content_new.append(w)
                template_new = template_new[:-1]
            else:
                if(wild_now):
                    template_new += "<*>"
                    wildcards_new.append(wild_now)
                    wild_content_new.append(wild_content_now)
            wild_index += 1
            index = index + 3
        else:
            template_new += word_now
            index += 1
    template_new = template_new.strip()

    tokens = template_new.split(' ', 2)
    first_token = tokens[0]
    second_token = tokens[1] if len(tokens) > 1 else ''

    if ("<*>" == first_token[:3] or ("<*>" in first_token and re.findall("[a-zA-Z]", first_token) == 0)):
        if(first_token != "<*>"):
            first_space_index = template_new.find(' ')
            template_new = "<*>" + template_new[first_space_index:]
            first_token_content = re.sub("<\*>", wild_content_new[0], first_token)
            wild_content_new[0] = first_token_content
            wildcards_new[0] = check_characters(first_token_content)
        if (second_token != "<*>" and "<*>" == second_token[:3]):
            first_space_index = template_new.find(' ')
            second_space_index = template_new.find(' ', first_space_index + 1)
            template_new = template_new[:first_space_index + 1] + "<*>" + template_new[second_space_index:]
            second_token_content = re.sub("<\*>", wild_content_new[1], second_token)
            wild_content_new[1] = second_token_content
            wildcards_new[1] = check_characters(second_token_content)
    return template_new, wildcards_new, wild_content_new

