import os
import json
from dataloader import datasets

from few_shot_sampling import candidate_set_construction_step2


# Your available datasets
target_datasets = datasets
data_path = "data"
candidate_path = "candidates"


print("=" * 60)
print("STEP 2: Enhancing Candidates with Positive/Negative Examples")
print("=" * 60)


for dataset in target_datasets:
   print(f"\n>>> Enhancing candidates for {dataset} <<<")
  
   try:
       # Check if step 1 file exists
       step1_file = f"{candidate_path}/{dataset}.json"
       if not os.path.exists(step1_file):
           print(f"✗ Step 1 file not found: {step1_file}")
           print(f"  Please run generate_candidates_step1.py first")
           continue
          
       print(f"Using candidates from: {step1_file}")
      
       # Generate enhanced candidates
       rate = 0.2  # Use 20% of data for sampling
      
       enhanced_candidates = candidate_set_construction_step2(
           path=data_path,
           dataset_now=dataset,
           candidate_path=candidate_path,
           rate=rate
       )
      
       # Save enhanced candidates (overwrite the original)
       output_file = f"{candidate_path}/{dataset}.json"
       with open(output_file, 'w') as f:
           json.dump(enhanced_candidates, f, indent=2)
           print(f"✓ Enhanced {len(enhanced_candidates)} candidates for {dataset}")
       print(f"✓ Saved to: {output_file}")
      
       # Show sample enhanced candidates
       print(f"Sample enhanced candidates:")
       for i, candidate in enumerate(enhanced_candidates[:1]):
           print(f"  {i+1}. Content: {candidate['content'][:50]}...")
           print(f"     Template: {candidate['template'][:50]}...")
           print(f"     Positive Example: {candidate['Postive_Example'][:50]}...")
           print(f"     Negative Example: {candidate['Negative_Example'][:50]}...")
          
   except Exception as e:
       print(f"✗ Error with {dataset}: {e}")
       import traceback
       traceback.print_exc()


print("\n" + "=" * 60)
print("Step 2 completed! Enhanced candidate files ready for EPAS.")
print("=" * 60)

  