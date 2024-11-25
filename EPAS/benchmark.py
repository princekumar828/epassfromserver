from dataloader import benchmark_settings
from evaluation import evaluate
import pandas as pd

parsed_path = ""
bechmark_result=[]

for dataset, setting in benchmark_settings.items():

    print('\n=== Evaluation on %s ===' % dataset)
    result_path = parsed_path + dataset + '_structured.csv'
    result_path_template = parsed_path + dataset + '_templates.csv'
    ground_path = f"loghubV2/{dataset}/{dataset}_full.log_structured.csv"
    ground_path_template = f"loghubV2/{dataset}/{dataset}_full.log_templates.csv"
    try:
        GA, PGA, RGA, FGA, PA, PTA, RTA, FPA = evaluate(ground_path, result_path, ground_path_template, result_path_template)
        print(dataset, GA, PGA, RGA, FGA, PA, PTA, RTA, FPA)
        bechmark_result.append([dataset, GA, FGA, PA, FPA])
        print('\n=== Overall evaluation results ===')
    except:
        print(dataset)
df_result = pd.DataFrame(bechmark_result, columns=['Dataset', 'GA', 'FGA', 'PA', 'FTA'])
print(df_result)
df_result.T.to_csv(parsed_path + f'bechmark_result.csv')
