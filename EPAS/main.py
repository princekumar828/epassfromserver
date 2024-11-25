from logparser import parser
from LLM import create_open_llm
from dataloader import datasets
import json


tag = False

for dataset in datasets:
    try:
        with open(f"candidates/{dataset}.json", 'r+') as f:
            candidates = json.load(f)
        llm = create_open_llm("")
        logparser = parser(output_dir=f"result", dataset=dataset, dataset_scale='full',
                           llm=llm, candidates=candidates, k=3, pst=0.5)
        logparser.parse()
    except:
        print(dataset)
