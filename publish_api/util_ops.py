
import os 

def is_path_exists(path: str):
    
    return True if os.path.exists(path) else False

def create_folders(path: str):
    
    if not is_path_exists(path):
        try:
            os.makedirs(path)
        except Exception:
            print("Folders Cannot Created in the given path")
    else:
        return False
    
def create_json_file(path: str):
    
    if not is_path_exists(path):
        open(path, "w").close()
    else:
        return False

def is_file_empty(path: str):
    
    return True if os.stat(path).st_size == 0 else False
  