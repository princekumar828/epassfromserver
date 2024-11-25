import pandas as pd
import re

Equivalence_template = [
    {"parser": "authentication failure; logname= uid=<*> euid=<*> tty=<*> ruser= rhost=<*> user=<*>",
     "groundtruth": "authentication failure; logname=<*> uid=<*> euid=<*> tty=<*> ruser=<*> rhost=<*> user=<*>"},
    {"parser": "<*> more authentication failure; logname= uid=<*> euid=<*> tty=<*> ruser= rhost=<*> user=<*>",
     "groundtruth": "<*> more authentication failure; logname= uid=<*> euid=<*> tty=<*> ruser=<*> rhost=<*> user=<*>"},
    {"parser": "Connection broken for id <*>, my id = <*>, error =",
     "groundtruth": "Connection broken for id <*>, my id = <*>, error =<*>"},
    {"parser": "New election. My id = <*>, proposed zxid=<*>",
     "groundtruth": "New election. My id = <*>, proposed <*>"},
    {"parser": "Sending snapshot last zxid of peer is <*> zxid of leader is <*>sent zxid of db as <*>",
     "groundtruth": "Sending snapshot last zxid of peer is <*> zxid of leader is <*> zxid of db as <*>"},
    {
        "parser": "PrepareForService is being done on this Midplane (mLctn(<*>), mCardSernum(<*>), iWhichCardsToPwrOff(<*>)) by <*>",
        "groundtruth": "PrepareForService is being done on this Midplane (<*>, <*>, <*>) by <*>"},
    {
        "parser": "Error sending result ChunkFetchSuccess{streamChunkId=StreamChunkId{streamId=<*>, chunkIndex=<*>}, buffer=FileSegmentManagedBuffer{file=<*>, offset=<*>, length=<*>}} to <*>; closing connection",
        "groundtruth": "Error sending result ChunkFetchSuccess{streamChunkId=<*>, buffer=<*>} to <*>; closing connection"},
    {
        "parser": "SparkListenerBus has already stopped! Dropping event SparkListenerBlockUpdated(BlockUpdatedInfo(BlockManagerId(<*>, <*>, <*>,StorageLevel(<*>, <*>, <*>, <*>, <*>))",
        "groundtruth": "SparkListenerBus has already stopped! Dropping event <*>"},
    {"parser": "Submitting <*> missing tasks from ResultStage <*> (<*> at <*> at <*>)",
     "groundtruth": "Submitting <*> missing tasks from ResultStage <*> (<*> at <*>)"},
    {"parser": "<*> more authentication failure; logname= uid=<*> euid=<*> tty=<*> ruser= rhost=<*>",
     "groundtruth": "<*> more authentication failure; logname=<*> uid=<*> euid=<*> tty=<*> ruser=<*> rhost=<*>"},
    {"parser": "[<*>]: Failed to bring port to ACTIVE for node=<*>, port= <*>, no response",
     "groundtruth": "[<*>]: Failed to bring port to ACTIVE for node=<*>, port=<*>, no response"},
    {"parser": "[<*>]: Failed to bring port to ACTIVE for node=<*>, port= <*>, mad status <*>",
     "groundtruth": "[<*>]: Failed to bring port to ACTIVE for node=<*>, port=<*>, mad status <*>"},
    {
        "parser": "[<*>]: Program port state, node=<*>, port= <*>, current state <*>, neighbor node=<*>, port= <*>, current state <*>",
        "groundtruth": "[<*>]: Program port state, node=<*>, port=<*>, current state <*>, neighbor node=<*>, port=<*>, current state <*>"},
    {"parser": "[<*>]: Failed to negotiate MTU, op_vl for node=<*>, port= <*>, no response",
     "groundtruth": "[<*>]: Failed to negotiate MTU, op_vl for node=<*>, port=<*>, no response"},
    {"parser": "PSU status ( <*> <*> )",
     "groundtruth": "PSU status (<*> <*>)"},
    {
        "parser": "pam_unix(sshd:auth): authentication failure; logname= uid=<*> euid=<*> tty=<*> ruser= rhost=<*> user=<*>",
        "groundtruth": "pam_unix(sshd:auth): authentication failure; logname=<*> uid=<*> euid=<*> tty=<*> ruser=<*> rhost=<*> user=<*>"},
    {"parser": "PAM <*> more authentication failures; logname= uid=<*> euid=<*> tty=<*> ruser= rhost=<*> user=<*>",
     "groundtruth": "PAM <*> more authentication failures; logname=<*> uid=<*> euid=<*> tty=<*> ruser=<*> rhost=<*> user=<*>"},
    {"parser": "authentication failure; logname=<*> uid=<*> euid=<*> tty= ruser= rhost= user=<*>",
     "groundtruth": "authentication failure; logname=<*> uid=<*> euid=<*> tty=<*> ruser=<*> rhost=<*> user=<*>"},
    {"parser": "connection from <*> () at <*>",
     "groundtruth": "connection from <*> (<*>) at <*>"},
    {"parser": "session opened for user <*> by <*>(uid=<*>)",
     "groundtruth": "session opened for user <*> by <*>"},
    {
        "parser": "Releasing unassigned and invalid container Container: [ContainerId: <*>, NodeId: <*>, NodeHttpAddress: <*>, Resource: <*>, Priority: <*>, Token: Token { kind: <*>, service: <*> }, ]. RM may have assignment issues",
        "groundtruth": "Releasing unassigned and invalid container Container: [ContainerId: <*>, NodeId: <*>, NodeHttpAddress: <*>, Resource: <*>, Priority: <*>, Token: <*>]. RM may have assignment issues"},
    {
        "parser": "Final resource view: name=<*> phys_ram=<*> used_ram=<*> phys_disk=<*> used_disk=<*> total_vcpus=<*> used_vcpus=<*> pci_stats=[]",
        "groundtruth": "Final resource view: name=<*> phys_ram=<*> used_ram=<*> phys_disk=<*> used_disk=<*> total_vcpus=<*> used_vcpus=<*> pci_stats=<*>"}, ]
Equivalence_situation = [
    ["<\*> <\*>-<\*>-<\*> <\*> <\*> \(<\*>\) --> <\*> <\*>-<\*>-<\*> <\*> <\*> \(<\*>\)", "<*>"],
    ["\( <\*>,", "(<*>,"], ["\{ <\*> <\*> \}", "<*>"], ["\"<\*>\"", "<*>"], ["<\*> [MKG]B", "<*>"],
    ["<\*>\$<\*>", "<*>"], ["\(<\*>\)", "<*>"], ["\{ <\*> \}", "<*>"], ["BP-<\*>", "<*>"],
    ["[a-zA-Z]+\.<\*>", "<*>"], ["\.\.<\*>", ".<*>"], ["http://<\*>", "<*>"],
    ["<\*>\)", "<*>"], ["\(<\*>", "<*>"], ["<\*>\+<\*>", "<*>"], ["<\*>:", "<*>"],
    ["\+<\*>", "<*>"], ["\{<\*>\}", "<*>"], ["<\*>-", "<*>"],
    ["\[<\*>\]", "<*>"], ["\[<\*>", "<*>"], ["<\*>\]", "<*>"], ["<\*>\.<\*>", "<*>"],
    ["'<\*>'", "<*>"], ["<\*>'", "<*>"], ["'<\*>", "<*>"], ["<\*>,", "<*>"],
    ["<\*>#", "<*>"], ["#<\*>", "<*>"],
    ["<\*>;", "<*>"], ["<\*> \]", "<*>"], ["<\*>\.", "<*>"], ["<\*>-<\*>", "<*>"],
    ["<\*>/", "<*>"], ["<\*> <ok>", "<*>"], ["<\*>:", "<*>"],
    ["<\*>/<\*>", "<*>"], ["<\*>##<\*>", "<*>"],
    ["<\*> <\*>", "<*>"], ["<\*><\*>", "<*>"]]


def get_TA(EIDS, ground_truth, parser, length):
    parsermap = {}
    groundmap = {}
    df_parser = pd.read_csv(parser)
    df_groundtruth = pd.read_csv(ground_truth)

    for idx, line in df_parser.iterrows():
        eid = line['EventId']
        template = line['EventTemplate']
        parsermap[eid] = template
    for idx, line in df_groundtruth.iterrows():
        eid = line['EventId']
        template = line['EventTemplate']
        groundmap[eid] = template

    truth_count = 0
    truth_count_PA = 0
    turthtemplate = []
    errortemplate = []
    for (parserEid, groundEid, count) in EIDS:

        template_parser = parsermap[parserEid]
        template_ground_truth = groundmap[groundEid]

        conti = False
        for condition in Equivalence_template:
            if (template_parser == condition['parser'] and template_ground_truth == condition['groundtruth']):
                truth_count += 1
                truth_count_PA += count
                turthtemplate.append([parsermap[parserEid], groundmap[groundEid]])
                conti = True
                break
        if (conti):
            continue
        if (type(template_parser) != str):
            continue
        for (before, after) in Equivalence_situation:
            while (re.findall(before, template_parser)):
                template_parser = re.sub(before, after, template_parser)
            while (re.findall(before, template_ground_truth)):
                template_ground_truth = re.sub(before, after, template_ground_truth)

        if (template_parser == template_ground_truth):
            truth_count += 1
            truth_count_PA += count
            turthtemplate.append([parsermap[parserEid], groundmap[groundEid]])

        else:
            errortemplate.append([parsermap[parserEid], groundmap[groundEid], count])

    PTA = (truth_count * 1.0) / (len(parsermap.keys()) * 1.0)
    RTA = (truth_count * 1.0) / (len(groundmap.keys()) * 1.0)
    PA = (truth_count_PA * 1.0) / (1.0 * length)
    return PTA, RTA, PA


def get_accuracy(series_groundtruth, series_parsedlog, debug=False):
    num_parsed_template = series_parsedlog['EventId'].nunique()
    num_ground_template = series_groundtruth['EventId'].nunique()

    parsed_log = series_parsedlog.groupby('EventId')['LineId'].apply(lambda x: sorted(x.tolist())).to_dict()
    parsed_log_map_E2L = series_parsedlog.groupby('EventId')['LineId'].min().to_dict()
    parsed_log_map_L2E = {event_id: min_lineid for min_lineid, event_id in parsed_log_map_E2L.items()}

    ground_truth = series_groundtruth.groupby('EventId')['LineId'].apply(lambda x: sorted(x.tolist())).to_dict()
    ground_truth_map_E2L = series_groundtruth.groupby('EventId')['LineId'].min().to_dict()
    # ground_truth_map_L2E = {event_id: min_lineid for min_lineid, event_id in ground_truth_map_E2L.items()}

    EIDS = []
    accurate_events = 0
    acc_template = 0

    for eid in ground_truth.keys():
        min_lineid = ground_truth_map_E2L[eid]
        if (min_lineid not in parsed_log_map_L2E.keys()):
            continue
        corresponding_eid = parsed_log_map_L2E[min_lineid]
        if (len(parsed_log[corresponding_eid]) != len(ground_truth[eid])):
            continue
        if (parsed_log[corresponding_eid] == ground_truth[eid]):
            acc_template += 1
            accurate_events += len(parsed_log[corresponding_eid])
            EIDS.append([corresponding_eid, eid, len(parsed_log[corresponding_eid])])

    print("GA cacluate done")
    GA = float(accurate_events) / series_groundtruth['EventId'].size
    PGA = float(acc_template) / num_parsed_template
    RGA = float(acc_template) / num_ground_template
    return EIDS, GA, PGA, RGA


def evaluate(groundtruth, parsedresult, ground_template, parsed_template):
    df_parsedlog = pd.read_csv(parsedresult, index_col=False)
    df_groundtruth = pd.read_csv(groundtruth)
    null_logids = df_groundtruth[~df_groundtruth['EventId'].isnull()].index
    df_groundtruth = df_groundtruth.loc[null_logids]
    df_parsedlog = df_parsedlog.loc[null_logids]
    EIDS, GA, PGA, RGA = get_accuracy(df_groundtruth, df_parsedlog)
    length = len(df_parsedlog['EventId'])
    PTA, RTA, PA = get_TA(EIDS, ground_template, parsed_template, length)

    FGA = (2 * PGA * RGA) / (PGA + RGA)
    FPA = (2 * PTA * RTA) / (PTA + RTA)

    return GA, PGA, RGA, FGA, PA, PTA, RTA, FPA
