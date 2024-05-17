import hou
import re
import os 

class FetchNodeValidation:
    
    all_fetchnode_source_path = {}
    
    def __init__(self, fetch_node: hou.Node) -> None:
        
        self.fetch_node = fetch_node
        self.ext_types = ['.abc', '.vdb', '.exr']
        
    def is_source_empty(self) -> bool:
        
        return self.fetch_node.parm('source').eval()

    def is_naming_rule_passed(self) -> bool:
        
        fetch_node_name = self.fetch_node.name()
    
        if fetch_node_name.startswith('_'):
            return False 
        elif fetch_node_name.endswith('_'):
            return False 
        elif not re.match(r'^[a-z0-9_]*$', fetch_node_name):
            return False
        elif 'fetch' in fetch_node_name:
            return False
        else:
            return True
    
    def is_referenced_identical_paths(self) -> bool:
        
        source_path = self.is_source_empty()
        
        if source_path:
            self.all_fetchnode_source_path.update(
                {self.fetch_node.name(): source_path}
            )

        if set([x for x in list(self.all_fetchnode_source_path.values()) \
                if  list(self.all_fetchnode_source_path.values()).count(x) > 1]):
            
            return {x:y for (x,y) in self.all_fetchnode_source_path.items() 
                    if list(self.all_fetchnode_source_path.values()).count(y) > 1}
        else:
            return True    
    
    def is_sourced(self, 
                   node_type_name: str) -> bool:
        
        source_node = self.is_source_empty()
        
        if not source_node:
            return False
        else:
            if not hou.node(source_node):
                return False
            elif hou.node(source_node).type().name() == node_type_name:
                return True
    
    def is_alembic_rop_sourced(self) -> bool:
        
        if self.is_sourced('rop_alembic'):
            return True
        else:
            return False
    
    def is_filecache_sourced(self) -> bool:
        
        if self.is_sourced("filecache::2.0"):
            return True
        else:
            return False
    
    def is_mantra_sourced(self) -> bool:
        
        if self.is_sourced('ifd'):
            return True
        else:
            return False
    
    def is_rop_geo_sourced(self) -> bool:
        
        if self.is_sourced('rop_geometry'):
            return True
        else:
            return False
    
    def is_ext_type_valid(self) -> bool:
        
        source_node = self.is_source_empty()
        
        if self.is_alembic_rop_sourced():
            if hou.node(source_node).parm('filename').eval().endswith('.abc'):
                return True
            
        elif self.is_filecache_sourced() or \
            self.is_rop_geo_sourced():
            if hou.node(source_node).parm('sopoutput').eval().endswith('.vdb'):
                return True
            
        elif self.is_mantra_sourced():
            if hou.node(source_node).parm('vm_picture').eval().endswith('.exr'):
                return True
        
        else: return False
    
    def is_cache_file_exists(self) -> bool:
        
        source_node = self.is_source_empty()
        
        if self.is_alembic_rop_sourced():
            cache_path = os.path.dirname(hou.node(source_node).parm('filename').eval())
            
            if os.path.exists(cache_path):
                files_list =  os.listdir(cache_path)
                if not files_list:
                    return False
                elif not list(filter(lambda x: x.endswith('.abc'), files_list)):
                    return False
                else: 
                    return True
            else:
                return False

        elif self.is_filecache_sourced() or \
                    self.is_rop_geo_sourced():
            cache_path = os.path.dirname(hou.node(source_node).parm('sopoutput').eval())
            
            if os.path.exists(cache_path):
                files_list =  os.listdir(cache_path)
                if not files_list:
                    return False
                elif not list(filter(lambda x: x.endswith('.vdb'), files_list)):
                    return False
                else: 
                    return True
            else:
                return False
        
        elif self.is_mantra_sourced():
            cache_path = os.path.dirname(hou.node(source_node).parm('vm_picture').eval())
            
            if os.path.exists(cache_path):
                files_list =  os.listdir(cache_path)
                if not files_list:
                    return False
                elif not list(filter(lambda x: x.endswith('.exr'), files_list)):
                    return False
                else: 
                    return True
            else:
                return False
    
    def is_rop_current_render_frame_selected(self) -> bool:
        
        source_node = self.is_source_empty()
        if hou.node(source_node).parm('trange').eval() == 0:
            return False
        else:
            return True
        
        

class PFXPublishPropertyValidation:
    
    """Check the given publish node input connected to 
    only the to the pfx publish property node. Returns the 
    nodes list if connected nodes were publish property node"""
    
    def __init__(self, 
                 publish_node: hou.Node,) -> None:
        
        """Initialize node. Collect pfx_publish_properties node. 

        Args:
            publish_node (hou.Node): pfx_publish node fetched from 
                    the pfx_publish HDA 
        """
        self.publish_node = publish_node
        self.error_connectes_nodes = set()
        self.pfx_publish_property_nodes = set()
        
        for connected_nodes in self.publish_node.inputs():
            if not connected_nodes.type().name().startswith('pfx_publish_properties'):
                self.error_connectes_nodes.add(connected_nodes)
            else:
                self.pfx_publish_property_nodes.add(connected_nodes)
    
    @staticmethod
    def show_message(msg):
        hou.ui.displayMessage(msg,
                              title="Pfx Publish Validation Logs",) 
                
    def __is_pfx_publish_property_node_connected(self) -> bool:
        
        """Check the connection and return the status of True
        and False

        Returns:
            bool: True if the node connection matches else 
                    error gui and return false 
        """
                
        if self.error_connectes_nodes:
            err = "Please Connect only pfx_publish_properties node\n"
            err += "Below Connected nodes were not pfx_publish_property nodes \n\n"
            for error_connectes_node in self.error_connectes_nodes:
                err += f"{error_connectes_node}\n"
            err += "\naborted!!"
            self.show_message(err) 
        
            return False
        else:
            return True

    def __is_fetch_node_connected(self) -> bool:
        
        fetch_unconnected_nodes = set()
        if self.__is_pfx_publish_property_node_connected():
            for pfx_property_nodes in self.pfx_publish_property_nodes:
                if not pfx_property_nodes.inputs():
                    fetch_unconnected_nodes.add(pfx_property_nodes)
        
        if fetch_unconnected_nodes:
            err = f"Below\n\n"
            for fetch_unconnected_node in fetch_unconnected_nodes:
                err += f"{fetch_unconnected_node}\n"
            err += "\nnodes not connected with any fetch node. aborted!!"
            self.show_message(err)
            return False 
        else:
            return True
    
    
    def __is_fetch_node_refernce_node_connected(self) -> bool:
       
        fetch_parent_unconnected_nodes = set() 
        if self.__is_fetch_node_connected():
            for pfx_property_nodes in self.pfx_publish_property_nodes:
                for fetch_nodes in pfx_property_nodes.inputs():
                    source_node = hou.node(fetch_nodes.parm('source').eval())
                    
                    if source_node:
                        mantranode = FetchNodeValidation(fetch_nodes)
                        if mantranode.is_mantra_sourced():
                            continue
                        if not source_node.inputs():
                            fetch_parent_unconnected_nodes.add(source_node)
                        
        if fetch_parent_unconnected_nodes:
            err = f"Below\n\n"
            for fetch_parent_unconnected_node in fetch_parent_unconnected_nodes:
                err += f"{fetch_parent_unconnected_node.path()}\n"
            err += "\nnodes were not connected to any networks.\n"
            err += "attribute checks gonna fail. aborted!!\n"
            self.show_message(err)
            return False
        else:
            return True
    
    def __is_existing_publish_path_parm_empty(self) -> bool:
        
        for pfx_property_nodes in self.pfx_publish_property_nodes:
            if pfx_property_nodes.parm('update_to_latest_path').eval():
                if pfx_property_nodes.parm('path').eval():
                    return True
                else:
                    err = "Below Property Nodes Does Not Have "
                    err += "existing publish path!!\n\n"
                    err += f"{pfx_property_nodes}"
                    self.show_message(err)
                    return False 
            else:
                return True
                
    
    def __reference_node_connection_checks(self,
                                           file_type_mode: int) -> set:
        
        unsupported_nodes = {}
        for pfx_property_nodes in self.pfx_publish_property_nodes:
            if pfx_property_nodes.parm('file_type').eval() == file_type_mode:
                for fetch_nodes in pfx_property_nodes.inputs():
                    
                    fetchnode = FetchNodeValidation(fetch_nodes)
                    source_of_fetch_node = fetch_nodes.parm('source').eval()
                    
                    if file_type_mode == 0:
                        if not fetchnode.is_alembic_rop_sourced():
                            unsupported_nodes.update(
                                {fetch_nodes:source_of_fetch_node}
                            )
                    elif file_type_mode == 1:
                        if not any([fetchnode.is_filecache_sourced(), fetchnode.is_rop_geo_sourced()]):
                             unsupported_nodes.update(
                                {fetch_nodes:source_of_fetch_node}
                            )
                    elif file_type_mode == 2:
                        if not fetchnode.is_mantra_sourced():
                            unsupported_nodes.update(
                                {fetch_nodes:source_of_fetch_node}
                            )
                    
        return unsupported_nodes
    
    def __abctype_reference_node_connection_checks(self) -> bool:
        
        # '0' represent 'abc' in the pfx_publish_properties HDA file_type parm
        abc_unsupported_nodes = self.__reference_node_connection_checks(0)
                        
        if abc_unsupported_nodes:
            
            err = "Please Use \'Rop Alembic\' for caching alembic\n"
            err += "Below\n\n"
            for abc_unsupported_fetch_node, abc_unsupported_node_path \
                        in abc_unsupported_nodes.items():
                err += f"{abc_unsupported_fetch_node}: {abc_unsupported_node_path}\n"
            err += "\nnodes were not supported.aborted!!\n"
            self.show_message(err)
            return False       
        else:
            return True
    
    def __vdbtype_reference_node_connection_checks(self) -> bool:
               
        # '1' represent 'vdb' in the pfx_publish_properties HDA file_type parm
        vdb_unsupported_nodes = self.__reference_node_connection_checks(1)
                        
        if vdb_unsupported_nodes:
            
            err = "Please Use \'File Cache\' or \'Rop Geometry' for caching VDB\n"
            err += "Below\n\n"
            for vdb_unsupported_fetch_node, vdb_unsupported_node_path \
                        in vdb_unsupported_nodes.items():
                err += f"{vdb_unsupported_fetch_node}: {vdb_unsupported_node_path}\n"
            err += "\nnodes were not supported.aborted!!\n"
            self.show_message(err)
            return False       
        else:
            return True
    
    def __exrtype_reference_node_connection_checks(self) -> bool:
               
        # '2' represent 'exr' in the pfx_publish_properties HDA file_type parm
        exr_unsupported_nodes = self.__reference_node_connection_checks(2)
                        
        if exr_unsupported_nodes:
            
            err = "Mantra Rop Node Not detected\n"
            err += "Below\n\n"
            for exr_unsupported_fetch_node, exr_unsupported_node_path \
                        in exr_unsupported_nodes.items():
                err += f"{exr_unsupported_fetch_node}: {exr_unsupported_node_path}\n"
            err += "\nnodes were not supported.aborted!!\n"
            self.show_message(err)
            return False       
        else:
            return True
    
    
    def __is_fetch_validation_passed(self) -> bool:
        
        source_empty_nodes = set()
        naming_rule_failed_nodes = set()
        non_allowed_nodes = set()
        ext_type_mismatch_node = set()
        non_cache_path_exist_nodes = set()
        non_frame_range_selected_nodes = set()
        
        identical_nodes = []
        source_err = ''
        naming_err  = ''
        abc_err = ''
        identical_err = ''
        ext_type_mismatch_err = ''
        non_cache_path_err = ''
        non_frame_range_err = ''
        
        if self.__is_fetch_node_connected():
            for pfx_property_nodes in self.pfx_publish_property_nodes:
                for fetch_nodes in pfx_property_nodes.inputs():
                    
                    if not pfx_property_nodes.parm('update_to_latest_path').eval():
                        fetchnode = FetchNodeValidation(fetch_nodes)

                        if not fetchnode.is_source_empty():
                            source_empty_nodes.add(fetch_nodes)
                        
                        if not fetchnode.is_naming_rule_passed():
                            naming_rule_failed_nodes.add(fetch_nodes)
                            
                        if not any([fetchnode.is_alembic_rop_sourced(),
                            fetchnode.is_filecache_sourced(),
                            fetchnode.is_mantra_sourced(),
                            fetchnode.is_rop_geo_sourced()]):
                            non_allowed_nodes.add(fetch_nodes)
                        
                        if not fetchnode.is_ext_type_valid():
                            ext_type_mismatch_node.add(fetch_nodes)
                        
                        if not fetchnode.is_cache_file_exists():
                            non_cache_path_exist_nodes.add(fetch_nodes)
                        
                        if not fetchnode.is_rop_current_render_frame_selected():
                            non_frame_range_selected_nodes.add(fetch_nodes)
                            
                        fetch_node_identical_path = fetchnode.is_referenced_identical_paths()
                        if isinstance(fetch_node_identical_path, dict):
                            identical_nodes.append(fetch_node_identical_path)
                            
                            
        if source_empty_nodes:
            
            source_err = f"No source referenced for below nodes\n\n"
            for source_empty_node in source_empty_nodes:
                source_err += f"{source_empty_node}\n"
            source_err += "\n" + '-' * 100 + "\n"
            
        if naming_rule_failed_nodes:
            
            naming_err = "Re-Name the fetch node name with the element Name you are publishing.\n\n"
            naming_err += "Examples:-\nfire\nfire_1\nfire_bg_1\nmaster_waterfall\nsecondary_waterfall\nbig_smoke\ndust\n\n"
            naming_err += "Below Nodes not have valid element names\n\n"
            for naming_rule_failed_node in naming_rule_failed_nodes:
                naming_err += f"{naming_rule_failed_node}\n"
            naming_err += "\n" + '-' * 100 + "\n"
        
        if non_allowed_nodes:
            
            abc_err = "Referenced Nodes were not \'Rop_alembic\' or \'File Cache\' or \'Mantra\' Nodes\n"
            abc_err += "OR not exist\n\n"
            for non_allowed_node in non_allowed_nodes:
                abc_err += f"{non_allowed_node}:    {non_allowed_node.parm('source').eval()}\n"
            abc_err += "\n" + '-' * 100 + "\n"
        
        if identical_nodes:
            
            identical_err = "Same Source Path Referenced More than one time in Fetch Node\n\n"
            for identical_node, identical_node_path in identical_nodes[0].items():
                identical_err += f"{identical_node}:  {identical_node_path}\n"
            identical_err += "\n" + '-' * 100 + "\n"
        
        if ext_type_mismatch_node:
            
            ext_type_mismatch_err = "\'abc\', \'vdb\' or \'exr\' extension not found \n\n"
            for ext_type_mismatch_nodes in ext_type_mismatch_node:
                ext_type_mismatch_err += f"{ext_type_mismatch_nodes.parm('source').eval()}\n"
            ext_type_mismatch_err += "\n" + '-' * 100 + "\n"
        
        if non_cache_path_exist_nodes:
            
            non_cache_path_err = "1 . Cache path not exist in the directory\n\n"
            non_cache_path_err += "(OR)\n\n"
            non_cache_path_err += "2. The pfx publish property file type extension not matching with the cached files"
            non_cache_path_err += " for below nodes\n\n"
            for non_cache_path_exist_node in non_cache_path_exist_nodes:
                non_cache_path_err += f"{non_cache_path_exist_node.parm('source').eval()}\n"
            non_cache_path_err += "\n" + '-' * 100 + "\n"
        
        if non_frame_range_selected_nodes:
            
            non_frame_range_err = "Single Frame Publishing Detected \n\n"
            non_frame_range_err +="\'Render Current Frame\' or \'Single Frame\' "
            non_frame_range_err += "option selected to below nodes.\n"
            non_frame_range_err += "Use \'Render Frame Range\' option to cache out \n\n\n"
            for non_frame_range_selected_node in non_frame_range_selected_nodes:
                non_frame_range_err += f"{non_frame_range_selected_node.parm('source').eval()}\n"
            non_frame_range_err += "\n" + '-' * 100 + "\n"
            
        if source_empty_nodes or \
                naming_rule_failed_nodes or \
                non_allowed_nodes or \
                identical_nodes or \
                ext_type_mismatch_node or \
                non_cache_path_exist_nodes or \
                non_frame_range_selected_nodes:    

            errmsg = source_err + \
                naming_err   + \
                abc_err  + \
                identical_err + \
                ext_type_mismatch_err + \
                non_cache_path_err + \
                non_frame_range_err + \
                'Aborted!!\n'
                
            self.show_message(errmsg)
            return False
            
        return True
                             
    
    def get_nodes(self) -> [hou.Node]:
        
        """The method returns the list of pfx_publish_propety nodes
       if no other type nodes connected with the pfx_publish nodes

        Returns:
            hou.Node : Return list the property nodes 
        """
        
        if self.__is_pfx_publish_property_node_connected() and \
            self.__is_fetch_node_connected() and \
            self.__is_fetch_node_refernce_node_connected() and \
            self.__abctype_reference_node_connection_checks() and \
            self.__vdbtype_reference_node_connection_checks() and \
            self.__exrtype_reference_node_connection_checks() and \
            self.__is_existing_publish_path_parm_empty() and \
            self.__is_fetch_validation_passed():
                
            if self.pfx_publish_property_nodes:
                return self.pfx_publish_property_nodes
        
        