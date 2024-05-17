import hou
from imp import reload
import os
import re
import sys
import time
import shutil
import threading
import hdefereval
from time import sleep
from copy import deepcopy
from .publish_api import (json_publish_entry_api,
                          make_publish_files)
from . import node_and_property_validation
from .publish_api import util_ops

reload(node_and_property_validation)
reload(json_publish_entry_api)
reload(make_publish_files)
reload(util_ops)


class Publish:
    
    def __init__(self,
                 publish_node: hou.Node,
                 task_publish_path: str,
                 publish_element_status_dict: dict,
                 publish_element_node: dict) -> None:
        
        self.publish_node = publish_node
        self.publish_element_status_dict = publish_element_status_dict
        self.publish_element_node = publish_element_node
        self.valid_ext_types = ['.abc', '.vdb', '.exr']
        
        self.sub_task = ''
        self.mov_path = ''
        self.task_publish_path = task_publish_path
        self.element_names= []
        self.elements_cache_type = {}
        self.publish_category = {}
        self.elements_path = {}
        self.elements_last_publish_path = {}
        self.elements_frame_range = {}
        self.elemnts_source_cache_drives = {}
        self.elements_destination_publish_path = {}
        self.pfx_publish_property_destination_dict = {}
        self.latest_element_path_status = {}
        self.comments = ''
        self.elements_db_lookup = {}
    
            
    def set_mov_path(self) -> None:
        
        if self.publish_node.parm("lbl_mov_path").eval():
            self.mov_path = self.publish_node.parm("mov_path").eval()
    
    def set_comments(self) -> None:
        
        self.comments = self.publish_node.parm("comments").evalAsString()

    def set_subtask(self) -> None:
        
        parm = self.publish_node.parm("sub_task")
        value = parm.eval()
        self.sub_task = parm.menuItems()[value]
    
        
    def set_element_entities(self):
        
        for publish_propety_node, fetch_node in self.publish_element_node.items():
            
            update_latest_path_mode = publish_propety_node.parm('update_to_latest_path').eval()
            update_latest_path = os.path.dirname(publish_propety_node.parm('path').eval())
            
            # publish_category Menu items name 
            publish_category = publish_propety_node.parm("publish_category")
            publish_category_index = publish_category.eval()
            current_publish_category = publish_category.menuItems()[publish_category_index]
            
            cache_file_type = publish_propety_node.parm("file_type")
            cache_file_type_index = cache_file_type.eval()
            current_file_type = cache_file_type.menuItems()[cache_file_type_index]
            
            start_frame = publish_propety_node.parm('f_rangex').eval()
            end_frame = publish_propety_node.parm('f_rangey').eval()
            
            fetch_node_validation = \
                    node_and_property_validation.FetchNodeValidation(fetch_node)
            source_node = fetch_node.parm('source').eval()
            
            # set elements names
            self.element_names.append(fetch_node.name())
                
            if fetch_node_validation.is_alembic_rop_sourced():
                
                cache_path = hou.node(source_node).parm('filename').eval()
                cache_path = cache_path.replace("\\", "/")
                
                if update_latest_path_mode:
                    self.elements_path.update(
                        {fetch_node.name(): update_latest_path}
                    )
                    self.latest_element_path_status.update(
                        {fetch_node.name(): False}
                    )
                    self.elements_db_lookup.update(
                        {fetch_node.name(): False}
                    )
                else:
                    self.elements_path.update(
                        {fetch_node.name(): cache_path}
                    )
                    self.latest_element_path_status.update(
                        {fetch_node.name(): True}
                    )
                    self.elements_db_lookup.update(
                        {fetch_node.name(): True}
                    )
                        
                self.elements_frame_range.update(
                    {fetch_node.name(): f"{start_frame}-{end_frame}"}
                )
                self.publish_category.update(
                    {fetch_node.name(): current_publish_category}
                )
                self.elements_cache_type.update(
                    {fetch_node.name(): current_file_type}
                )
                
            elif fetch_node_validation.is_filecache_sourced() or \
                fetch_node_validation.is_rop_geo_sourced():
                
                cache_path = hou.node(source_node).parm('sopoutput').eval()
                cache_dir = os.path.dirname(cache_path)
                cache_file = os.path.basename(cache_path).rsplit('.vdb')[0].rsplit('.',1)[0] + \
                            '.$F4.vdb'
                cache_path = os.path.join(cache_dir, cache_file)
                cache_path = cache_path.replace("\\", "/")

                if update_latest_path_mode:
                    self.elements_path.update(
                        {fetch_node.name(): update_latest_path}
                    )
                    self.latest_element_path_status.update(
                        {fetch_node.name(): False}
                    )
                    self.elements_db_lookup.update(
                        {fetch_node.name(): False}
                    )
                else:
                    self.elements_path.update(
                        {fetch_node.name(): cache_path}
                    )
                    self.latest_element_path_status.update(
                        {fetch_node.name(): True}
                    )
                    self.elements_db_lookup.update(
                        {fetch_node.name(): True}
                    )
                    
                self.elements_frame_range.update(
                    {fetch_node.name(): f"{start_frame}-{end_frame}"}
                )
                self.publish_category.update(
                    {fetch_node.name(): current_publish_category}
                )
                self.elements_cache_type.update(
                    {fetch_node.name(): current_file_type}
                )
                
            elif fetch_node_validation.is_mantra_sourced():
                
                cache_path = hou.node(source_node).parm('vm_picture').eval()
                cache_dir = os.path.dirname(cache_path)
                cache_file = os.path.basename(cache_path).rsplit('.exr')[0].rsplit('.',1)[0] + \
                            '.####.exr'
                cache_path = os.path.join(cache_dir, cache_file)
                cache_path = cache_path.replace("\\", "/")


                if update_latest_path_mode:
                    self.elements_path.update(
                        {fetch_node.name(): update_latest_path}
                    )
                    self.latest_element_path_status.update(
                        {fetch_node.name(): False}
                    )
                    self.elements_db_lookup.update(
                        {fetch_node.name(): False}
                    )
                else:
                    self.elements_path.update(
                        {fetch_node.name(): cache_path}
                    )
                    self.latest_element_path_status.update(
                        {fetch_node.name(): True}
                    )
                    self.elements_db_lookup.update(
                        {fetch_node.name(): True}
                    )
                    
                self.elements_frame_range.update(
                    {fetch_node.name(): f"{start_frame}-{end_frame}"}
                )
                self.publish_category.update(
                    {fetch_node.name(): current_publish_category}
                )
                self.elements_cache_type.update(
                    {fetch_node.name(): current_file_type}
                )

        pass
    
    def __is_validation_sucesss(self):
        
        txt = ''
        status = []

        if self.publish_element_status_dict:
            for element_name, element_attribs in self.publish_element_status_dict.items():
                txt += element_name +'\n'
                for element_attrib, element_status in element_attribs.items():
                    
                    if not isinstance(element_status, list):
                        status.append(element_status)
                    else:
                        status.append(element_status[0])
                        
                    if isinstance(element_status, list):
                        if not element_status[0]:
                            txt += f"   {element_attrib}:  {element_status[-1]}\n"
                txt += '\n'
        self.publish_node.parm('logs').set(txt)
        
        if all(status):
            return True
        else:
            msg = "Validation Failed!!\n\n"
            msg += "Check Errors in Error Logs Tab"
            hou.ui.displayMessage(msg)
            return False
    
    @staticmethod
    def generate_version_path(publish_dir,
                              version_number,
                              element_name = '',
                              create_folders = False):

        version_label = 'v' + f'{version_number}'.zfill(3)
        if element_name:
            data_cache_folder = os.path.join(publish_dir, 
                                            version_label,
                                            element_name)
            if create_folders:
                util_ops.create_folders(data_cache_folder)
            return data_cache_folder
        else:
            return os.path.join(publish_dir, 
                                version_label)
    
    def update_pfx_publish_property_path_parm(self,
                                            pfx_publish_property_destination_dict):
        
        for pfx_publish_property_node, destination_publish_path \
                in pfx_publish_property_destination_dict.items():
                    
            pfx_publish_property_node.parm('path').lock(0)
            pfx_publish_property_node.parm('path').set(destination_publish_path)
            pfx_publish_property_node.parm('path').lock(1)
    
    def create_file_node_in_fetch_node_source_path(self,
                                                    pfx_publish_property_destination_dict):
        
        def create_read_node_based_on_type(source_node_name,
                                           parent_node,
                                           destination_publish_path,
                                           node,
                                           file_parm,
                                           create_new_file_node = True):
            
            
            for nodes in hou.node(parent_node.path()).children():
                if source_node_name in nodes.userDataDict():
                    nodes.parm(file_parm).set(destination_publish_path)
                    nodes.setColor(hou.Color(0.145,0.667,0.557))
                    create_new_file_node = False
            
            if create_new_file_node:
                file_node =parent_node.createNode(node)
                file_node.setName(source_node_name + '_published',
                                unique_name=False)
                file_node.parm(file_parm).set(destination_publish_path)
                file_node.setColor(hou.Color(0.145,0.667,0.557))
                file_node.setUserData(source_node_name,'True')
                file_node.move([source_node_x_pose, source_node_y_pose - 2])
        
        create_new_file_node = True
        for pfx_publish_property_node, destination_publish_path \
                in pfx_publish_property_destination_dict.items():
            
            fetch_node = pfx_publish_property_node.inputs()[0]      
            source_of_fetch_node = fetch_node.parm('source').eval()
            source_node = hou.node(source_of_fetch_node)
            source_node_name = source_node.name()
            parent_node = source_node.parent()
            source_node_y_pose = source_node.position()[-1]
            source_node_x_pose = source_node.position()[0]
            
            fetch_node_validation = \
                    node_and_property_validation.FetchNodeValidation(fetch_node)
                    
            if fetch_node_validation.is_filecache_sourced() or \
                fetch_node_validation.is_rop_geo_sourced():

                create_read_node_based_on_type( source_node_name,
                                                parent_node,
                                                destination_publish_path,
                                                'file',
                                                'file',
                                                create_new_file_node=create_new_file_node)
                        
            elif fetch_node_validation.is_alembic_rop_sourced():
                
                create_read_node_based_on_type( source_node_name,
                                                parent_node,
                                                destination_publish_path,
                                                'alembic',
                                                'fileName',
                                                create_new_file_node=create_new_file_node)
    
    # Thread Based Execution Stats here
    
    def do_move(self,src_cache_path,
                des_cache_path,):
        
        sleep(0.01)
        print(f"\n\npfx publisher:: moving FROM: \n{src_cache_path}\n To: \n{des_cache_path}")
        shutil.move(src_cache_path,
                    des_cache_path)
            
    
    def register_publish_db(self):
        
        publish = json_publish_entry_api.PublishVersions(
            versions_db= self.versions_db,
            sub_task = self.sub_task,
            fx_publish_cache_dir = self.task_publish_path,
            element_names= self.element_names,
            elements_cache_type= self.elements_cache_type,
            elements_publish_category = self.publish_category,
            elements_publish_path=self.elements_path,
            elements_frame_range = self.elements_frame_range,
            comments = self.comments,
            mov_path = self.mov_path
        )
        publish.write_data()
        msg = "For Subtasks \n\n%s" %self.sub_task
        msg += "\n\n"
        msg += "Following Elements\n"
        msg += "-" * 70 +"\n"
        msg +="\n".join(self.element_names)
        msg +="\n\n"
        msg += 'publish version::  ' + 'v' + str(publish.versions_db.get_max_version()).zfill(3)
        msg += '\n\n Published Successully!!'
        hou.ui.displayMessage(msg)

    def hip_file_ops(self):
        
        hou.hipFile.save()
        hip_file_name = hou.hipFile.basename()
        version_label = re.findall(r'v\d{3}', hip_file_name)[0]
        version_no = int(re.findall('\d+', version_label)[0])
        extract_hip_name = hip_file_name.split("_" + version_label)[0]
        
        for _,elements_source_cache_drive in self.elemnts_source_cache_drives.items():
            
            hip_publish_path =self.generate_version_path(self.subtask_publish_folder,
                                                            self.max_version)
            
            hip_publish_path = os.path.join(elements_source_cache_drive,
                                            os.path.splitdrive(hip_publish_path)[-1])
            
        
            publish_hip_file_name =  extract_hip_name + '_' + version_label + '.hip'
            publish_hip_file_path = os.path.join(
                hip_publish_path, publish_hip_file_name
            )
            
            if not os.path.exists(publish_hip_file_path):
                shutil.copy2(hou.hipFile.path(), publish_hip_file_path )
        
        version_no = version_no + 1
        versionup_labal = 'v' + f'{version_no}'.zfill(3)
        version_up_hip_name = extract_hip_name + '_' + versionup_labal + '.hip'
        hou.ui.displayMessage(f"version uped!! Saved As \n\n {version_up_hip_name}")
        work_dir_path = os.path.dirname(hou.hipFile.path())
        version_up_hip_path = os.path.join(work_dir_path, version_up_hip_name)
        hou.hipFile.save(version_up_hip_path)
    
    def do_move_in_hou_main_thread(self,
                                    src_cache_path,
                                    des_cache_path):
        hdefereval.executeInMainThreadWithResult(self.do_move,
                                                 src_cache_path,
                                                 des_cache_path)
    
    def do_register_in_hou_main_thread(self):
        hdefereval.executeInMainThreadWithResult(self.register_publish_db)
    
    def do_hip_file_ops_in_hou_main_thread(self):
        hdefereval.executeInMainThreadWithResult(self.hip_file_ops)
        

    def do_publish(self) -> None:
        
        def is_update_exisiting_path_true():
            
            pfx_property_nodes_with_update_existing_path_true = set()
            for pfx_publish_property_nodes in self.publish_node.inputs():
                if pfx_publish_property_nodes.parm('update_to_latest_path').eval():
                    pfx_property_nodes_with_update_existing_path_true.add(
                        pfx_publish_property_nodes.name()
                    )
            
            if pfx_property_nodes_with_update_existing_path_true:       
                msg = "\"Use Existing Publish Path\" Enabled to Below Nodes. Proceed Publishing ??\n\n"
                msg += "\n".join(pfx_property_nodes_with_update_existing_path_true) 
                if hou.ui.displayMessage(msg,  
                                        title="Pfx Publish Validation Logs",
                                        buttons=('Yes', 'No')) == 0:
                    return True
                else:
                    return False
            else:
                if hou.ui.displayMessage("Proceed Publish??",  
                                        title="Pfx Publish",
                                        buttons=('Yes', 'No')) == 0:
                    return True
                else: 
                    return False
        
        def is_hip_save_by_pfx_save():
            
            import re
            hip_name = hou.hipFile.basename()
            show = os.environ['PFX_SHOW']
            shot = os.environ['PFX_SHOT']
            step = os.environ['PFX_TASK']
            version = re.findall(r'v\d{3}\b', hip_name)
            
            if show in hip_name and \
                shot in hip_name and \
                step in hip_name and \
                hip_name.endswith('.hip') and \
                '.' not in os.path.splitext(hip_name)[0] and \
                version:
                return True
            else:
               hou.ui.displayMessage("File Not saved using PFX Save\n Use File->PFX Save",  
                                        title="Pfx Publish",) 

        
        if is_hip_save_by_pfx_save():
            
            if is_update_exisiting_path_true() and \
                self.__is_validation_sucesss():
  
                self.set_subtask()
                self.set_mov_path()
                self.set_comments()
                self.set_element_entities()
                
                # Register the source path in the elemetns db 
                # Used to check whether the same source path 
                # published again. if so it throw wxception
                if any(self.elements_db_lookup.values()):
                    
                    elements_db_look_up_path_dict = deepcopy(self.elements_path)
                    del_elements = set()
                    for elem, db_status in self.elements_db_lookup.items():
                        for ele_name, _ in elements_db_look_up_path_dict.items():
                            if elem == ele_name:
                                if not db_status:
                                    del_elements.add(ele_name)

                    for del_element in del_elements:
                        if del_element in self.elements_path:
                            elements_db_look_up_path_dict.pop(del_element)
                        
                    self.elements_path_dict = json_publish_entry_api.ElementsDB(
                        sub_task = self.sub_task,
                        fx_publish_cache_dir =self.task_publish_path,
                        element_names=self.element_names,
                        elements_source_paths=elements_db_look_up_path_dict
                    )
                
                    try:
                        self.elements_path_dict.write_data()
                    except json_publish_entry_api.IdenticalPathError as e: 
                        hou.ui.displayMessage(str(e), title="Pfx Publish Validation Logs")
                        sys.exit(1)
                    
                self.versions_db = json_publish_entry_api.VersionsDB(
                    sub_task=self.sub_task,
                    fx_publish_cache_dir=self.task_publish_path,
                    element_names=self.element_names,
                )  
        
                self.versions_db.write_data()

                self.max_version = self.versions_db.get_max_version()
                
                publish_folder_obj = make_publish_files.PublishRegisterFolder(
                    sub_task=self.sub_task,
                    fx_publish_cache_dir=self.task_publish_path,
                )
                self.subtask_publish_folder = publish_folder_obj.get_publish_folder()
                
                for (element_name, elements_path), \
                    (status_element_name, element_new_publish_status) \
                            in zip(
                                self.elements_path.items(),
                                self.latest_element_path_status.items()
                            ):
                    if element_name == status_element_name:
                        if element_new_publish_status:
                            
                            publish_path = self.generate_version_path(self.subtask_publish_folder,
                                                                        self.max_version,
                                                                        element_name)

                            if os.path.splitdrive(elements_path)[0] in publish_path:
                                publish_path = self.generate_version_path(self.subtask_publish_folder,
                                                                        self.max_version,
                                                                        element_name,
                                                                        create_folders=True)

                            source_path_drive = os.path.splitdrive(elements_path)[0]

                            self.elemnts_source_cache_drives.update(
                                {element_name: source_path_drive}
                            )
                            
                            publish_path = os.path.join(source_path_drive,
                                                        os.path.splitdrive(publish_path)[-1])
                            
                            util_ops.create_folders(publish_path)
                            
                            for files in os.listdir(os.path.dirname(elements_path)):
                                if list(
                                    filter(files.endswith, self.valid_ext_types)
                                ):
                                    src_cache_path = os.path.join(
                                        os.path.dirname(elements_path), files
                                    )
                                    des_cache_path =  os.path.join(
                                        publish_path, 
                                        files
                                    )
                                    
                                    self.elements_destination_publish_path.update(
                                        {element_name: des_cache_path}
                                    )

                                    file_move_thread = threading.Thread(target=self.do_move_in_hou_main_thread, 
                                                                        args=(src_cache_path, 
                                                                                des_cache_path,))
                                    file_move_thread.start()
                    

                            # Remap the source path to publish path entry in the json db.
                            publish_path = publish_path.replace("\\", "/")
                            self.elements_path[element_name] = publish_path
                            
                for fetch_node_name, destination_publish_path \
                        in self.elements_destination_publish_path.items():
                    
                    if destination_publish_path.endswith('.vdb'):
                            destination_publish_path = \
                                        destination_publish_path.rsplit('.vdb')[0].rsplit('.',1)[0]
                            destination_publish_path = destination_publish_path.replace("\\", "/")
                            destination_publish_path = destination_publish_path + ".$F4.vdb"
                            
                    elif destination_publish_path.endswith('.exr'):
                            destination_publish_path = \
                                        destination_publish_path.rsplit('.exr')[0].rsplit('.',1)[0]
                            destination_publish_path = destination_publish_path.replace("\\", "/")
                            destination_publish_path = destination_publish_path + ".$F4.exr"
                    
                    elif destination_publish_path.endswith('.abc'):
                            destination_publish_path = \
                                        destination_publish_path.rsplit('.abc')[0]
                            destination_publish_path = destination_publish_path.replace("\\", "/")
                            destination_publish_path = destination_publish_path + '.abc'
                            
                            
                    child_pfx_publish_property_node = \
                        hou.node(f'/out/{fetch_node_name}').outputConnections()[0].outputNode()
                        
                    self.pfx_publish_property_destination_dict.update(
                            {child_pfx_publish_property_node: destination_publish_path}
                    )
                    
                self.update_pfx_publish_property_path_parm(
                            self.pfx_publish_property_destination_dict
                )
                self.create_file_node_in_fetch_node_source_path(
                            self.pfx_publish_property_destination_dict
                )
                
                publish_register_thread = threading.Thread(target=self.do_register_in_hou_main_thread,)
                publish_register_thread.start()

                hip_file_ops_thread = threading.Thread(target=self.do_hip_file_ops_in_hou_main_thread,)
                hip_file_ops_thread.start()
        