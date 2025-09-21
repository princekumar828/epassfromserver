
"""
Step 1: Generate initial candidate files for EPAS
This script creates the basic candidate files needed before running candidate_set_construction_step2
"""


import sys
import os
import json

from dataloader import load_groundtruth_full
from few_shot_sampling import candidate_set_construction
from dataloader import datasets

# Your available datasets
target_datasets = datasets
data_path = "data"
candidate_path = "candidates"


# Create candidates directory
os.makedirs(candidate_path, exist_ok=True)


print("=" * 60)
print("STEP 1: Generating Initial Candidate Files")
print("=" * 60)


for dataset in target_datasets:
   print(f"\n>>> Generating candidates for {dataset} <<<")
  
   try:
       # Generate candidates with different numbers of examples
       # Using [3, 5, 10] as default numbers for k-shot learning
       num_candidates = [3, 5, 10]
       rate = 0.2  # Use 20% of data for sampling
      
       print(f"Loading ground truth for {dataset}...")
       candidates_dict = candidate_set_construction(
           path=data_path,
           dataset_now=dataset,
           num=num_candidates,
           rate=rate
       )
      
       # Save the candidates with 5 examples by default
       selected_candidates = candidates_dict[5]  # Use 5-shot examples
      
       # Save to JSON file
       output_file = f"{candidate_path}/{dataset}.json"
       with open(output_file, 'w') as f:
           json.dump(selected_candidates, f, indent=2)
          
       print(f"✓ Generated {len(selected_candidates)} candidates for {dataset}")
       print(f"✓ Saved to: {output_file}")
      
       # Show sample candidates
       print(f"Sample candidates:")
       for i, candidate in enumerate(selected_candidates[:2]):
           print(f"  {i+1}. Content: {candidate['content'][:60]}...")
           print(f"     Template: {candidate['template'][:60]}...")
          
   except Exception as e:
       print(f"✗ Error with {dataset}: {e}")
       import traceback
       traceback.print_exc()


print("\n" + "=" * 60)
print("Step 1 completed! Initial candidate files generated.")
print("=" * 60)
print("\nNext: Run Step 2 to enhance candidates with positive/negative examples")

