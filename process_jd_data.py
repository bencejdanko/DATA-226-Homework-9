import pandas as pd

def process_tmdb_csv(input_file, output_file):
  """
  Processes a TMDB movies CSV file to create a Vespa-compatible JSON format.

  This function reads a CSV file containing TMDB movie data, processes the data to
  generate new columns for text search, and outputs a JSON file with the necessary
  fields (`put` and `fields`) for indexing documents in Vespa.

  Args:
    input_file (str): The path to the input CSV file containing the TMDB movies data.
                      Expected columns are 'id', 'original_title', 'overview', and 'genres'.
    output_file (str): The path to the output JSON file to save the processed data in
                       Vespa-compatible format.

  Workflow:
    1. Reads the CSV file into a Pandas DataFrame.
    2. Processes the 'genres' column, extracting genre names into a new 'genres_name' column.
    3. Fills missing values in 'original_title', 'overview', and 'genres_name' columns with empty strings.
    4. Creates a "text" column that combines specified features using the `combine_features` function.
    5. Selects and renames columns to match required Vespa format: 'doc_id', 'title', and 'text'.
    6. Constructs a JSON-like 'fields' column that includes the record's data.
    7. Creates a 'put' column based on 'doc_id' to uniquely identify each document.
    8. Outputs the processed data to a JSON file in a Vespa-compatible format.

  Returns:
    None. Writes the processed DataFrame to `output_file` as a JSON file.

  Notes:
    - The function requires the helper function `combine_features` to be defined, which is expected to combine text features for the "text" column.
    - Output JSON file is saved with `orient='records'` and `lines=True` to create line-delimited JSON.

  Example Usage:
    >>> process_tmdb_csv("tmdb_movies.csv", "output_vespa.json")
  """

  jobs = pd.read_csv(input_file)
  jobs = jobs[['position', 'description']]
  jobs.fillna('', inplace=True)
  jobs['id'] = jobs.index
  jobs.rename(columns={'id': 'doc_id', 'position': 'title', 'description': 'text'}, inplace=True)

  jobs['fields'] = jobs.apply(lambda row: row.to_dict(), axis=1)
  jobs['put'] = jobs['doc_id'].apply(lambda x: f"id:hybrid-search:doc::{x}")

  df_result = jobs[['put', 'fields']]
  print(df_result.head())
  df_result.to_json(output_file, orient='records', lines=True)

process_tmdb_csv("JD_data.csv", "clean_jd_data.jsonl")