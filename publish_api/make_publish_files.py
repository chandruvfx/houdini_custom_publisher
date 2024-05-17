import os 
from . import util_ops
import json


class PublishRegisterFolder:
    
    def __init__(self,
                 sub_task: str,
                 fx_publish_cache_dir: str) -> None:
        
        self.sub_task = sub_task
        self.fx_publish_cache_dir = fx_publish_cache_dir
         
    def get_publish_folder(self) -> str:
        
        return f"{self.fx_publish_cache_dir}/{self.sub_task}"
        
    
    def generate_publish_folder(self) -> None:
        
        util_ops.create_folders(
            self.get_publish_folder()
        )
    
    

class JsonOPS(PublishRegisterFolder):
    
    def __init__(self,
                 sub_task: str,
                 fx_publish_cache_dir: str,
                 json_file_name: str) -> None:
        
        super().__init__(sub_task, fx_publish_cache_dir)
        super().generate_publish_folder()
        
        self.json_file_name = json_file_name
        self.json_file =  os.path.join(
            self.get_publish_folder(), self.json_file_name
        )
        util_ops.create_json_file(
                self.json_file
            )
    
    def get_json_path(self) -> str:
        
        return self.json_file

    def write_json(self, dict_object) -> None:
        
        with open(self.json_file, "w") as json_file:
            json.dump(dict_object, json_file, indent=4)
            
    def read_json(self) -> dict:
        
        with open(self.json_file, "r") as json_file:
            dict_object = json.load(json_file)
        
        return dict_object
    
    

if __name__  == "__main__":
    
    version_json = JsonOPS(
        sub_task="meteor1",
        fx_publish_cache_dir= r"R:\fx_cache\publishes\aln\Shot\SC_1A\ALN_SC_1A_SH_070\FX1",
        json_file_name='v001.json'
    )
    print(version_json.get_json_path())
    dict_o =   {
        'employees' : [
            {
                'name' : 'John Doe',
                'department' : 'Marketing',
                'place' : 'Remote'
            },
            {
                'name' : 'Jane Doe',
                'department' : 'Software Engineering',
                'place' : 'Remote'
            }
        ]
    }
    # version_json.write_json(dict_o)
    # print(version_json.read_json())
    pass
    
    