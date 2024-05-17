import hou
from imp import reload
import os
from . import node_and_property_validation
reload(node_and_property_validation)


class GeneralChecks:
    
    def __init__(self, publish_node) -> None:
        
        self.publish_node = publish_node
        self.fetch_nodes_status = {}
        
        frame_range = hou.hscriptExpression('$PFX_FRAME_RANGE')
        if frame_range.split('-')[0] and frame_range.split('-')[-1]:
            self.scene_start_frame = int(frame_range.split('-')[0])
            self.scene_end_frame = int(frame_range.split('-')[-1])
        else:
            self.scene_start_frame = hou.playbar.frameRange()[0]
            self.scene_end_frame = hou.playbar.frameRange()[-1]
              
        self.pfx_publish_property_nodes = \
                    node_and_property_validation.PFXPublishPropertyValidation(self.publish_node)
        self.publish_property_nodes = self.pfx_publish_property_nodes.get_nodes()
        
        self.property_fetch_node_dict = {} 
        
        if self.publish_property_nodes:
            for property_nodes in self.publish_property_nodes:
                fetch_node = property_nodes.inputs()[0]
                
                self.property_fetch_node_dict.update(
                    {property_nodes: fetch_node}
                )
                
                fetch_node_name = fetch_node.name()
                
                self.fetch_nodes_status.update(
                    {fetch_node_name: {}}
                )
                
            self.fetch_nodes_status.update(
                {'global_settings': {}}
            )
            
        
    def update_dict(self,
                    fetch_node_name: str,
                    attrib: str,
                    status: bool) -> None:
        
        self.fetch_nodes_status[fetch_node_name].update(
                        {attrib: status}    
        )
        
    def inspect_frame_range(self) -> None:
        
        if self.publish_node.parm('frame_range').eval():
            for property_node, fetch_node in self.property_fetch_node_dict.items():
                
                frame_range_x = property_node.parm('f_rangex').evalAsInt()
                frame_range_y = property_node.parm('f_rangey').evalAsInt()
                fetch_node_name = fetch_node.name()
                
                
                if frame_range_x == self.scene_start_frame:
                    self.update_dict(fetch_node_name, 'start_frame', True)
                    
                else:
                    err = 'pfx_property start frame not matching with scene start frame. '
                    err += "Check on \'Shot info HDA\' in /obj level"
                    self.update_dict(fetch_node_name, 'start_frame', [False, 
                                                                      err])
                    
                if frame_range_y == self.scene_end_frame:
                    self.update_dict(fetch_node_name, 'end_frame', True)
                    
                else:
                    err = 'pfx_property end frame not matching with scene end frame. '
                    err += "Check on \'Shot info HDA\' in /obj level"
                    self.update_dict(fetch_node_name, 'end_frame', [False, 
                                                                      err])

    def inspect_file_path(self) -> None:
        
        if self.publish_node.parm('path_exist').eval():
            for property_node, fetch_node in self.property_fetch_node_dict.items():
                
                sourcenode_path = fetch_node.parm("source").eval()
                sourcenode = hou.node(sourcenode_path)
                fetch_node_name = fetch_node.name()
                
                if sourcenode.type().name() == "filecache::2.0" :
                    
                    #Check directory exist
                    cache_dir = os.path.dirname(sourcenode.parm('sopoutput').eval())
                
                elif sourcenode.type().name() == 'rop_alembic':
                    
                    cache_dir = os.path.dirname(sourcenode.parm('filename').eval())
                
                elif sourcenode.type().name() == 'rop_geometry':
                    
                    cache_dir = os.path.dirname(sourcenode.parm('sopoutput').eval())
                
                elif sourcenode.type().name() == 'ifd':
                    
                    cache_dir = os.path.dirname(sourcenode.parm('vm_picture').eval())
                    
                if os.path.exists(cache_dir) or len(os.listdir(cache_dir)) > 0:
                    self.update_dict(fetch_node_name, 'file_path_exists', True)
                    
                else:
                    self.update_dict(fetch_node_name, 'file_path_exists', [False,
                                                                           "path not exist"])
                pass
    
    def inspect_scene_scale(self) -> None:
        
        scene_scale = hou.hscriptExpression('$PFX_SCENE_SCALE')
        master_null_node = set()
        
        if self.publish_node.parm('scene_scale').eval():
            
            for nodes in hou.node('/obj').children():
                if 'master_null' in nodes.userDataDict():
                    master_null_node.add(nodes)
                    
                
            if master_null_node:
                for masternull_node in master_null_node:
                    scale = masternull_node.parm('scale').evalAsFloat()
                    if scale == float(scene_scale):
                        self.update_dict('global_settings', 'scene_scale', True)
                    else:
                        errmsg = "Master null Scene Scale not matching with the project configuration scene scale. "
                        errmsg += "Check on \'Shot info HDA\' in /obj level"
                        self.update_dict('global_settings', 'scene_scale', [False,
                                                                            errmsg])
                self.update_dict('global_settings', 'master_null', True)
            else:
                err = "Master Null node not created from Shot Info HDA from /obj level. "
                err += " Cannot Perform Scene Scale Checks. "
                self.update_dict('global_settings', 'master_null', [False,
                                                                    err])
                
                    
        
    def do_general_checks(self) -> None:
        
        
        if self.publish_property_nodes:
            self.inspect_frame_range()
            self.inspect_file_path()
            self.inspect_scene_scale()
        

class AttribChecks(GeneralChecks):
    
    def __init__(self,
                 publish_node: hou.Node) -> None:
        
        super().__init__(publish_node)
        self.do_general_checks()

    def update_attrib_to_dict(self,
                    attrib: str, 
                    attrib_list: set,
                    fetch_node: str,
                    err_msg: str = '') -> None:
        
        if attrib not in attrib_list: 
            self.fetch_nodes_status[fetch_node].update(
                {attrib: [False, err_msg]}
            )
        else:
            self.fetch_nodes_status[fetch_node].update(
                {attrib: True}
            )
            
    def frame_range_checks(self,
                           property_node_frame_no: int,
                           cache_node: hou.Node,
                           node_name: str,
                           parm: str,
                           frame_labal: str) -> None:
        
        if property_node_frame_no == int(cache_node.parm(parm).eval()):
            self.update_dict(node_name, 
                            f"{cache_node.name()}_{frame_labal}",
                            True)
        else:
            err_msg = "The entered start or end frame not matching with "
            err_msg += "shot start or end frame. "
            err_msg += "Check on \'Shot info HDA\' in /obj level"
            self.update_dict(node_name, 
                            f"{cache_node.name()}_{frame_labal}",
                            [False, err_msg])
                    
    def alembic_checks(self) -> None:
        
        alembic_property_nodes = set()
        point_attribs = set()
        vertex_attribs = set()
        prim_attribs = set()
        

        for property_node, fetch_node in self.property_fetch_node_dict.items():
            
            # '0' represent 'abc' in the pfx_publish_properties1 HDA file_type parm
            if property_node.parm('file_type').eval() == 0:
                
                frame_range_x = property_node.parm('f_rangex').evalAsInt()
                frame_range_y = property_node.parm('f_rangey').evalAsInt()
                
                alembic_property_nodes.add(fetch_node)
                
        for fetch_node in alembic_property_nodes:
            fetch_node_name = fetch_node.name()
            if self.publish_node.parm('abc_checks').eval():

                rop_alembic_node = hou.node(
                    fetch_node.parm('source').eval()
                )
                
                # get point attribs for P,v
                point_attrib_of_parent_rop_alembic = \
                        rop_alembic_node.inputs()[0].geometry().pointAttribs()
                # get vertex attrib to check 'uvs'
                vertex_attrib_of_parent_rop_alembic = \
                        rop_alembic_node.inputs()[0].geometry().vertexAttribs()
                # get prim attribs to check 'path' 
                prim_attrib_of_parent_rop_alembic = \
                        rop_alembic_node.inputs()[0].geometry().primAttribs()

                for point_attrib in point_attrib_of_parent_rop_alembic:
                    point_attribs.add(point_attrib.name())
                for vertex_attrib in vertex_attrib_of_parent_rop_alembic:
                    vertex_attribs.add(vertex_attrib.name())
                for print_attrib in prim_attrib_of_parent_rop_alembic:
                    prim_attribs.add(print_attrib.name())
                
                    
                if self.publish_node.parm('geo_v').eval():
                    err_msg = "Geometry velocity point attribute not found"
                    self.update_attrib_to_dict('v', point_attribs, fetch_node_name, err_msg=err_msg)
                        
                if self.publish_node.parm('uvs').eval():
                    err_msg = "Geometry texture vertex attribute not found"
                    self.update_attrib_to_dict('uv', vertex_attribs, fetch_node_name, err_msg=err_msg)
                        
                if self.publish_node.parm('path').eval():
                    err_msg = "Geometry path primitive attribute not found"
                    self.update_attrib_to_dict('path', prim_attribs, fetch_node_name, err_msg=err_msg)
                    
                    # Rop Alembic has 'Build Hierarchy From Attribute' check
                    # if this is on or off and update the status  
                    if rop_alembic_node.parm('build_from_path').eval():
                        self.update_dict(fetch_node_name, 
                                            'build_hierarchy_from_attribute',
                                            True)
                    else:
                        err_msg = "Rop Alembic output node 'Build Hierarchy From Attribute' option not switched on"
                        self.update_dict(fetch_node_name, 
                                            'build_hierarchy_from_attribute',
                                            [False,
                                             err_msg])
                        
                if self.publish_node.parm('frame_range').eval():
                    self.frame_range_checks(frame_range_x,
                                            rop_alembic_node,
                                            fetch_node_name,
                                            'f1',
                                            'start_frame')
                    self.frame_range_checks(frame_range_y,
                                            rop_alembic_node,
                                            fetch_node_name,
                                            'f2',
                                            'end_frame')
                    
    
    def vdb_checks(self) -> None:
        
        for property_node, fetch_node in self.property_fetch_node_dict.items():
            
            frame_range_x = property_node.parm('f_rangex').evalAsInt()
            frame_range_y = property_node.parm('f_rangey').evalAsInt()
            
            
            # '1' represent 'vdb' in the pfx_publish_properties1 HDA file_type parm
            if property_node.parm('file_type').eval() == 1:
                fetch_node_name = fetch_node.name()
                if self.publish_node.parm('vdb_checks').eval():
                    
                    vdb_source_node = hou.node(
                        fetch_node.parm('source').eval()
                    )
                    
                    if self.publish_node.parm('frame_range').eval():
                        self.frame_range_checks(frame_range_x,
                                            vdb_source_node,
                                            fetch_node_name,
                                            'f1',
                                            'start_frame')
                        self.frame_range_checks(frame_range_y,
                                                vdb_source_node,
                                                fetch_node_name,
                                                'f2',
                                                'end_frame')
                     
                    if vdb_source_node.type().name() == "rop_geometry":
                        vdb_source_node = vdb_source_node.inputs()[0]

                    name_attrib_exist = False
                    for vdb_prim_attribs in vdb_source_node.geometry().primAttribs():
                        if vdb_prim_attribs.name() == 'name':
                            vdb_attribs = vdb_prim_attribs.strings()
                            name_attrib_exist = True
                           
                    
                    if name_attrib_exist:
                        # The playbar frame moved to the pfx property startframe 
                        # consider if vdb cache start from 1000. but playbar current
                        # frame is 996. in this frame no density, vel attrib found.
                        hou.setFrame(frame_range_x)
                        if self.publish_node.parm('volume_density').eval():
                            err_msg = "Volume density primitive attribute not found"
                            self.update_attrib_to_dict('density', 
                                                       vdb_attribs, 
                                                       fetch_node_name,
                                                       err_msg=err_msg)
                            
                        if self.publish_node.parm('volume_vel').eval():
                            err_msg = "Volume velocity vel.x primitive attribute not found"
                            self.update_attrib_to_dict('vel',
                                                       vdb_attribs,
                                                       fetch_node_name,
                                                       err_msg=err_msg)

                    
    def exr_checks(self) -> None:
        
        #SCene resolution grabbed
        resolution = hou.hscriptExpression('$PFX_RESOLUTION')
        character = ''
        if chr(215) in resolution:
            character = chr(215)
        elif 'x' in resolution:   
            character = 'x'
        elif 'X' in resolution:   
            character = 'X'
        elif '*' in resolution: 
            character = '*'
        res_x = int(resolution.split(character)[0])
        res_y = int(resolution.split(character)[-1])
        
        for property_node, fetch_node in self.property_fetch_node_dict.items():
            
            frame_range_x = property_node.parm('f_rangex').evalAsInt()
            frame_range_y = property_node.parm('f_rangey').evalAsInt()
            
            # '2' represent 'exr' in the pfx_publish_properties1 HDA file_type parm
            if property_node.parm('file_type').eval() == 2:
                fetch_node_name = fetch_node.name()
                
                if self.publish_node.parm('exr_checks').eval():
                    
                    exr_source_node = hou.node(
                        fetch_node.parm('source').eval()
                    )
                    
                    camera = exr_source_node.parm('camera').eval()
                    camera_node = hou.node(camera)
                    if camera_node:
                        self.update_dict(fetch_node_name, 
                                        camera,
                                        True)
                    else:
                        err = "Referenced camera not found. Cannot perform Resolution Checks"
                        self.update_dict(fetch_node_name, 
                                        camera,
                                        [False, err])
                    
                    if camera_node:
                        
                        cam_res_x = camera_node.parm('resx').evalAsInt()
                        cam_res_y = camera_node.parm('resy').evalAsInt()
                        
                        if cam_res_x == res_x:
                            self.update_dict(fetch_node_name, 
                                        "camera_res_x",
                                        True)
                        else:
                            self.update_dict(fetch_node_name, 
                                        "camera_res_x",
                                        [False, "Given Camera Resolution X not matching with project resolution X"])
                            
                        if cam_res_y == res_y:
                            self.update_dict(fetch_node_name, 
                                        "camera_res_y",
                                        True)
                        else:
                            self.update_dict(fetch_node_name, 
                                        "camera_res_y",
                                        [False, "Given Camera Resolution Y not matching with project resolution Y"])
                            
                    if self.publish_node.parm('frame_range').eval():
                        self.frame_range_checks(frame_range_x,
                                            exr_source_node,
                                            fetch_node_name,
                                            'f1',
                                            'start_frame')
                        
                        self.frame_range_checks(frame_range_y,
                                                exr_source_node,
                                                fetch_node_name,
                                                'f2',
                                                'end_frame')

        
    def do_checks(self) -> dict:
        
        self.alembic_checks()
        self.vdb_checks()
        self.exr_checks()

        return self.fetch_nodes_status, self.property_fetch_node_dict

