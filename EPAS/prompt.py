EXTRACT_TEMPLATE="""Extract templates from the given logs. Keep the constants in the logs unchanged and replace the variables with <*>.
If there are no variable (all contents are constants), the template is same as the log.
Every logs and templates are put between tags <START> and <END>.
For example:
{examples}
Based on the above examples and templates, please extract templates from the logs.
Just extract templates from logs. Never output other informations.
Log:
<START>{log}<END>
Template:
"""




MERGE_TEMPLATES="""Determine whether the two given logs belong to the same template. 
If any of the differences between them is a constant, then answer 'No'. 
If all the differences between them are variables, then answer 'Yes'. 
Each log is placed between the tags <START> and <END>.
For example:
{examples}

Based on the above examples, do you think the following two logs belong to the same template? 
Please provide your response in "Answer" (choose between "Yes" and "No"). Do not output any other information. 
Log 1: <START>{log1}<END>
Log 2: <START>{log2}<END>
Answer:
"""