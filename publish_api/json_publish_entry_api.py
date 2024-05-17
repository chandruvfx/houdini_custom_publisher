import os
from . import util_ops
from functools import reduce
from .make_publish_files import JsonOPS

VERSIONS_JSON = 'versions_db.json'
ELEMENTS_PATH_JSON = 'elements_src_db.json'


class VersionsDB(JsonOPS):
    
    version = 1
    def __init__(self,
                 sub_task: str,
                 fx_publish_cache_dir: str,
                 element_names: list) -> None:
        
        super().__init__(sub_task, 
                         fx_publish_cache_dir, 
                         VERSIONS_JSON)

        self.element_names = element_names
        self.initial_version = 1
    
    def get_max_version(self):
        
        overall_version_list = set()
        read_element_version_dict = super().read_json()
        
        for _, element_versions in read_element_version_dict.items(): 
            for versions, _ in element_versions.items():
                overall_version_list.add(int(versions))
                
        return max(overall_version_list)
    
    @staticmethod
    def resolve_relative_json_path(version: int):
        
        return 'v' + f'{version}'.zfill(3) + \
                    '.json'
                
    def write_data(self):
        
        version_path = super().get_json_path()
        file_empty_status = util_ops.is_file_empty(version_path)
        element_version_dict = {}
        if file_empty_status:
            for element_name in self.element_names:
                element_version_dict.update({
                    element_name : {
                        str(self.initial_version): self.resolve_relative_json_path(
                                    self.initial_version
                        )
                    }
                })
        else:
        
            element_version_dict = super().read_json()
            for element_name in self.element_names:
                if element_name in element_version_dict:
                    
                    version_dict = element_version_dict[element_name]
                    version_dict = {int(k): v for k,v in version_dict.items()}
                    max_version = self.get_max_version()
                    max_version_key = max(list(range(1,max_version+1))) 
                    max_version_value = max(list(range(1,max_version+1))) 

                    version_up_key = max_version_key + 1
                    version_up_value = max_version_value + 1
                        
                    element_version_dict[element_name].update(
                    {str(version_up_key): self.resolve_relative_json_path(version_up_value)}
                    )
                else:
                    max_version = self.get_max_version() + 1
                    element_version_dict.update({
                        element_name : {
                            str(max_version): self.resolve_relative_json_path(max_version)
                            }
                        })
    
        super().write_json(element_version_dict)
        pass
    
    def elements_latest_versions(self):
        
        read_element_version_dict = super().read_json()
        max_element_versions = {}
        
        for element_name in self.element_names:
            if element_name in read_element_version_dict:
                
                max_version = max(list(map(int, read_element_version_dict[element_name].keys())))
                max_element_versions.update(
                        {element_name: max_version, }
               )
        
        return max_element_versions
    
    def cache_version_folder(self):
        
        self.get_max_version()
        

class ElementsDB(JsonOPS):
    
    def __init__(self,
                 sub_task: str,
                 fx_publish_cache_dir: str,
                 element_names: list,
                 elements_source_paths: dict) -> None:
        
        super().__init__(sub_task, 
                         fx_publish_cache_dir, 
                         ELEMENTS_PATH_JSON)
        
        self.element_names = element_names
        self.elements_source_paths = elements_source_paths
        
    
    def write_data(self):
        
        version_path = super().get_json_path()
        file_empty_status = util_ops.is_file_empty(version_path)
        elements_path_db_dict = {}
        if file_empty_status:
            for elements in self.element_names:
                for element_names, elements_path in self.elements_source_paths.items():
                    if elements == element_names:
                        elements_path_db_dict.update(
                            {elements: [elements_path]}
                        )
            self.write_json(elements_path_db_dict)
        else:
            get_elements_path_dict = self.read_json()
            for elements in self.element_names:
                if elements in get_elements_path_dict:
                    if elements in self.elements_source_paths:
                        if self.elements_source_paths[elements] not in get_elements_path_dict[elements]:
                            get_elements_path_dict[elements].append(
                                    str(self.elements_source_paths[elements])
                            )
                        else:
                            err = f'Publishing Same path {self.elements_source_paths[elements]} '
                            err += f'for element \n\n{elements} \n\nnot allowed'
                            raise IdenticalPathError(err)
                else:
                    for element_names, elements_path in self.elements_source_paths.items():
                        if elements == element_names:
                            get_elements_path_dict.update(
                                {elements: [elements_path]}
                            )
            self.write_json(get_elements_path_dict)
        

class PublishVersions:
    
    def __init__(self,
                 versions_db: VersionsDB,
                 sub_task: str,
                 fx_publish_cache_dir: str,
                 element_names: list,
                 elements_cache_type: dict,
                 elements_publish_category: dict,
                 mov_path:str,
                 elements_publish_path: dict = {},
                 elements_last_publish_path: dict = {},
                 elements_frame_range: dict = {},
                 comments: str = '') -> None:
        
        self.versions_db = versions_db
        self.sub_task = sub_task
        self.fx_publish_cache_dir = fx_publish_cache_dir
        self.element_names = element_names
        self.elements_cache_type=elements_cache_type
        self.elements_publish_category = elements_publish_category
        self.user_name = os.environ['USERNAME']
        self.elements_publish_paths = elements_publish_path
        self.elements_last_publish_path = elements_last_publish_path
        self.elements_frame_range = elements_frame_range
        self.comments = comments
        self.mov_path = mov_path
        
        self.cache_name_dict = {}
        self.publish_versions = {}
        self.linked_element_dict = {}
        
        
        if self.elements_last_publish_path:
            self.elements_publish_paths.update(self.elements_last_publish_path)
          
        self.max_json_version = self.versions_db.get_max_version()

        self.json_version_file = \
                    self.versions_db.resolve_relative_json_path(self.max_json_version)
        
        # ({'fire': 2}, {'fire': 'v002.json'}) 
        self.element_max_versions = \
            self.versions_db.elements_latest_versions()
        
        self.version_json = JsonOPS(self.sub_task,
                                        self.fx_publish_cache_dir,
                                        self.json_version_file)
        

    def write_data(self):
        
        for (element_name, element_max_version), \
            (path_element_name, element_path), \
            (frame_range_element_name, frame_range), \
            (publish_category_element_name, publish_category), \
            (cache_type_element_name, cache_type) in \
                    zip(self.element_max_versions.items(), 
                        self.elements_publish_paths.items(), 
                        self.elements_frame_range.items(),
                        self.elements_publish_category.items(),
                        self.elements_cache_type.items()
                        ):

                        if  element_name == path_element_name and \
                            element_name == frame_range_element_name and \
                            element_name == publish_category_element_name and \
                            element_name == cache_type_element_name:
                            self.cache_name_dict.update(
                                {
                                    element_name : {
                                        "version": element_max_version,
                                        "cache_type": cache_type,
                                        "frame_range": frame_range,
                                        "publish_category": publish_category,
                                        "publish_path": element_path
                                    }
                                }
                            
                            )

        self.publish_versions.update(
            {
                "subtask": self.sub_task,
                "user": self.user_name,
                "subtask_version": self.max_json_version,
                "comments": self.comments,
                "mov_path": self.mov_path,
                "cache_names": self.cache_name_dict
            }
        )
        self.version_json.write_json(self.publish_versions)
 
 
class IdenticalPathError(Exception):
    pass

        
if __name__  == "__main__":
    
    elements_path_dict = ElementsDB(
                sub_task = "meteor1",
                fx_publish_cache_dir = r"R:\fx_db\aln\SC_1A\ALN_SC_1A_SH_070\FX1",
                element_names=["fire", "fire1"],
                elements_source_paths={'fire': "45", 'fire1': '1'} 
        )
    elements_path_dict.write_data()
        
    versions_tracker = VersionsDB(
        sub_task="meteor1",
        fx_publish_cache_dir=r"R:\fx_db\publishes\aln\SC_1A\ALN_SC_1A_SH_070\FX1",
        element_names=["fire", "fire1"],
    )  
    
    versions_tracker.write_data()
    # print(versions_tracker.get_max_version())


    # publish_version = PublishVersions(
    #     versions_db= versions_tracker,
    #     sub_task="meteor1",
    #     fx_publish_cache_dir=r"R:\fx_db\publishes\aln\SC_1A\ALN_SC_1A_SH_070\FX1",
    #     element_names=['fire1', 'fire'],
    #     elements_cache_type={'fire': "abc", 'fire1': 'vdb'}, 
    #     elements_publish_category={'fire': "WIP", 'fire1': 'TEMP'},
    #     elements_publish_path={'fire': "45", 'fire1': '1'},
    #     # elements_last_publish_path = {'fire': "45",},
    #     elements_frame_range= {'fire': "1001-1047", 'fire1': '1002-1004'},
    #     mov_path="",
    #     comments="test"
    # )
    # publish_version.write_data()
    
    publish_version = PublishVersions(
        versions_db= versions_tracker,
        sub_task="meteor1",
        fx_publish_cache_dir=r"R:\fx_db\publishes\aln\SC_1A\ALN_SC_1A_SH_070\FX1",
        element_names=['fire1', 'fire'],
        elements_cache_type={'fire': "abc", 'fire1': 'vdb'}, 
        elements_publish_category={'fire': "WIP", 'fire1': 'TEMP'},
        elements_publish_path={'fire1': '1'},
        elements_last_publish_path = {'fire': "45464dd",},
        elements_frame_range= {'fire': "1001-1047", 'fire1': '1002-1004'},
        mov_path="",
        comments="test"
    )
    publish_version.write_data()