import re

def read_query(file_path: str) -> str:
  """
  Read SQL query from a file and convert to inline format.
  
  :param file_path: file name
  :return: a SQL query text inline
  """
  folder = "bin/"
  path_to_file = folder + file_path
  with open(path_to_file, 'r', encoding='utf-8') as file:
    query = file.read()
  query = query.replace('\n', ' ')
  query = re.sub(r'\s+', ' ', query).strip()
  return query