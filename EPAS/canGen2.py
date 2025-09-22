from dataloader import datasets
from few_shot_sampling import candidate_set_construction, candidate_set_construction_step2
import json
import os

candidate_path = "./candidates1"  
rate_step2 = 0.2
# Create the folder if it doesn't exist
os.makedirs(f"candidates", exist_ok=True)

for dataset in datasets:
    try:
        candidates2 = candidate_set_construction_step2(f"./data", dataset, candidate_path, rate_step2)
        # Write JSON file
        with open(f"candidates/{dataset}.json", "w") as f:
            json.dump(candidates2, f, indent=4)

    except Exception as e:
        print(f"Error processing {dataset}: {e}")

'''
    
candidate_path = "./candidates1"  # folder where initial candidates are saved
rate_step2 = 0.2

candidates2 = candidate_set_construction_step2("./data", f"Apache", candidate_path, rate_step2)

# save step2 candidates
with open(f"candidates1/Apache_step2.json", "w") as f:
    json.dump(candidates2, f, indent=4)

print(f"Step2 candidates saved to candidates/Apache_step2.json")


'''