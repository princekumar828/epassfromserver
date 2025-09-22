from dataloader import datasets
from few_shot_sampling import candidate_set_construction
import json
import os

# Create the folder if it doesn't exist
os.makedirs("candidates1", exist_ok=True)
num_list = [32]  # number of candidates to sample for each run
rate = 0.2          # fraction of logs to use
for dataset in datasets:
    try:
        candidates = candidate_set_construction(f"./data", dataset, num_list, rate)
        
        # Extract the list from the dictionary format and save in direct list format
        # candidates is in format {"32": [...]} but we need just the list [...]
        candidates_list = candidates[32]  # Extract the list for 32 candidates
        
        # Write JSON file in the correct format (direct list)
        with open(f"candidates1/{dataset}.json", "w") as f:
            json.dump(candidates_list, f, indent=4)

    except Exception as e:
        print(f"Error processing {dataset}: {e}")


'''
a
dataset = "Apache"  # example dataset
anum_list = [32]  # number of candidates to sample for each runa
rate = 0.2          # fraction of logs to usea
os.makedirs("candidates1", exist_ok=True)
# generate candidates
candidates = candidate_set_construction("./full_dataset", dataset, num_list, rate)

# save candidates to JSON
with open(f"candidates1/{dataset}.json", "w") as f:
    json.dump(candidates, f, indent=4)

print(f"Initial candidates saved to candidates/{dataset}.json")
'''