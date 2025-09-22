import pandas as pd
import sys
import os

def create_lilac_ground_truth_robust(log_file_path, annotation_csv_path, output_csv_path):
    """
    Robustly creates a LILAC-compatible ground truth file. It reads the .log file
    for sequence and content, and uses the annotation CSV as a lookup for templates,
    ensuring a perfect line-by-line match.
    """
    print(f"Reading annotation file: {annotation_csv_path}")
    try:
        annotation_df = pd.read_csv(annotation_csv_path)
        # Create a dictionary for fast lookups: {log_content: event_template}
        content_to_template_map = pd.Series(
            annotation_df.EventTemplate.values,
            index=annotation_df.Content
        ).to_dict()
    except Exception as e:
        print(f"Error reading or processing annotation file: {e}")
        return

    print(f"Reading log file for sequence: {log_file_path}")
    structured_data = []
    try:
        with open(log_file_path, 'r') as f:
            for line_id, line_content in enumerate(f, 1):
                content = line_content.strip()
                # Find the template for the current log content from our map
                template = content_to_template_map.get(content, "NO_TEMPLATE_FOUND")
                structured_data.append([line_id, content, template])
    except Exception as e:
        print(f"Error reading log file: {e}")
        return

    # Create a DataFrame from the sequenced data
    final_df = pd.DataFrame(structured_data, columns=['LineId', 'Content', 'EventTemplate'])

    # Now, generate EventId based on the final, ordered templates
    unique_templates = final_df['EventTemplate'].unique()
    template_to_id_map = {template: f'E{i+1}' for i, template in enumerate(unique_templates)}
    final_df['EventId'] = final_df['EventTemplate'].map(template_to_id_map)

    # Reorder columns to the final LILAC format
    output_df = final_df[['LineId', 'Content', 'EventId', 'EventTemplate']]

    # Save the correctly formatted file
    output_df.to_csv(output_csv_path, index=False)
    print(f"Successfully created LILAC-compatible ground truth file at {output_csv_path}")
    print(f"Total lines processed: {len(output_df)}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_structured_file.py <dataset_name1> <dataset_name2> ...")
        print("Example: python generate_structured_file.py brave airflow dbeaver")
        sys.exit(1)

    datasets = sys.argv[1:]  # Get all dataset names from arguments
    
    # The script will look for dataset folders in its own directory.
    base_path = os.path.dirname(os.path.abspath(__file__))

    for dataset_name in datasets:
        print(f"\n--- Processing dataset: {dataset_name} ---")

        # Use original LogBase filenames as input
        original_log_file = os.path.join(base_path, dataset_name, f'{dataset_name}.log')
        annotation_file = os.path.join(base_path, dataset_name, f'{dataset_name}.GeneralAnnotation.csv')
        
        # Define LILAC-compatible output filenames
        lilac_log_file = os.path.join(base_path, dataset_name, f'{dataset_name}_full.log')
        output_file = os.path.join(base_path, dataset_name, f'{dataset_name}_full.log_structured.csv')

        # Check for original input files
        if not os.path.exists(original_log_file):
            print(f"Error: Original log file not found at {original_log_file}")
            print(f"Skipping dataset {dataset_name}.")
            continue  # Move to the next dataset
        if not os.path.exists(annotation_file):
            print(f"Error: Annotation file not found at {annotation_file}")
            print(f"Skipping dataset {dataset_name}.")
            continue  # Move to the next dataset

        # --- Create the structured ground truth file ---
        create_lilac_ground_truth_robust(original_log_file, annotation_file, output_file)

        # --- Automatically rename the log file for LILAC parser ---
        if os.path.exists(original_log_file):
            print(f"Renaming {original_log_file} to {lilac_log_file} for LILAC compatibility.")
            os.rename(original_log_file, lilac_log_file)
