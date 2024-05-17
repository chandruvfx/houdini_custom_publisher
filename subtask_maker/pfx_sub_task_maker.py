import os 
import sys
import re
import json
from PySide2.QtUiTools import QUiLoader
from PySide2 import QtWidgets, QtGui
from PySide2.QtCore import Qt
from thadam_base import thadam_api

ROOT_SUB_TASK_DIR = os.environ["SUB_TASK_DIR"]


class PfxSubTaskMaker(QtWidgets.QMainWindow):
    
    """GUI for creating subtasks for a task. 
    Subtask were saved in sub task global dir, inside the 
    show/seq/shot/task hierarchiel folder. The sub tasks
    were entered as list in the json file. 
    
    Lead or Fx Sups allowed to choose mandotary fields 
    for task and allowed to enter mutiple subtask names for a 
    choosen task. Able add, remove task in the existing task list.
    """
    
    def __init__(self) -> None:
        
        """Qt Widget file loaded and all widget referenced here
        
        Thadam api server accessed to get all the project and task 
        level entities
        """
        
        super().__init__()
        
        dirname = os.path.dirname(__file__)
        ui_file = os.path.join(dirname, 
                               "ui\pfx_fx_task_maker.ui"
        )
        ui_loader = QUiLoader()
        self.subtask_window = ui_loader.load(ui_file)

        self.show_combo_box = self.subtask_window.findChild(
            QtWidgets.QComboBox,
            "show_cbx"
        )
        self.sequence_combo_box = self.subtask_window.findChild(
            QtWidgets.QComboBox,
            "seq_cbx"
        )
        self.shot_combo_box = self.subtask_window.findChild(
            QtWidgets.QComboBox,
            "shot_cbx"
        )
        self.task_combo_box = self.subtask_window.findChild(
            QtWidgets.QComboBox,
            "task_cbx"
        )
        self.sub_task_list = self.subtask_window.findChild(
            QtWidgets.QListView,
            "subtask_list"
        )
        self.model = QtGui.QStandardItemModel()
        
        self.add_task = self.subtask_window.findChild(
            QtWidgets.QPushButton,
            "add"
        )
        self.remove_task = self.subtask_window.findChild(
            QtWidgets.QPushButton,
            "remove"
        )
        self.publish_subtask = self.subtask_window.findChild(
            QtWidgets.QPushButton,
            "make"
        )
        self.master_icon = self.subtask_window.findChild(
            QtWidgets.QLabel,
            "icon"
        )
        
        self.thadam_api_server = thadam_api.ThadamParser()
        self.set_project()
        
        self.show_combo_box.activated[str].connect(self.set_sequence)
        
        seq_args = lambda: self.set_shot(self.show_combo_box.currentText(), 
                                          self.sequence_combo_box.currentText()
        )
        self.sequence_combo_box.activated[str].connect(seq_args)
        
        self.add_task.clicked.connect(self.add_subtask)
        self.remove_task.clicked.connect(self.remove_subtask)
        self.publish_subtask.clicked.connect(self.make_subtask)
        
        tool_icon = os.path.join(dirname, "icons/green_grow.png")
        tool_pixmap = QtGui.QPixmap(tool_icon)
        self.master_icon.setPixmap(tool_pixmap.scaled(50,50, Qt.KeepAspectRatio))
        
    
    
    def set_project(self) -> None:
        
        """Get All the Projects from thadam server and 
        append to the project combo box
        """
        
        self.show_combo_box.clear()
        self.sequence_combo_box.clear()
        self.shot_combo_box.clear()
        self.task_combo_box.clear()
        
        self.projects = self.thadam_api_server.get_projects()
        
        self.projects = sorted(self.projects, key=lambda d: d['proj_code'])
        
        for project in self.projects:
            self.show_combo_box.addItem(project['proj_code'])
            
        self.show_combo_box.setCurrentIndex(-1)
        
    
    def set_sequence(self, 
                     project_name:str
        )-> None:
        
        """ append all the seq for the given project
        clear all child combo box relative to that
        
        Args:
            project_name(str): project name passed from the 
                        selected project name from the combobox
        """
        
        self.sequence_combo_box.clear()
        self.shot_combo_box.clear()
        self.task_combo_box.clear()
        
        sequences = set()
        self.get_sequences = self.thadam_api_server.get_sequences(project_name)
        for sequence in self.get_sequences:
            sequences.add(sequence['seq_name']) 
        
        for sequence in sorted(sequences):
            self.sequence_combo_box.addItem(sequence)
        
        self.sequence_combo_box.setCurrentIndex(-1)
    
    def set_shot(self, 
                 project_name: str,
                 seq_name: str) -> None:
        
        """Append shots for the given project name and seq name
        
        Args:
            project_name(str): User selected project in project combobox
            seq_name(str): user selected seq in seq combobox
        """
        
        self.shot_combo_box.clear()
        self.task_combo_box.clear()
        
        self.shots = self.thadam_api_server.get_shots(project_name,
                                                    seq_name
        )
        
        self.shots = sorted(self.shots, key=lambda d: d['shot_name'])
        for shot in self.shots:
            self.shot_combo_box.addItem(shot['shot_name'])
        
        self.shot_combo_box.setCurrentIndex(-1)
        self.shot_combo_box.activated[str].connect(self.set_task)
    
    def set_task(self) -> None:
        
        
        """Append Task names and allow user to select from the 
        combobox of task
        """
        self.task_combo_box.clear()
        
        tasks = set()
        get_selected_project_name = self.show_combo_box.currentText()
        get_selected_shot = self.shot_combo_box.currentText()
        
        for project in self.projects:
            if project['proj_code'] == get_selected_project_name:
                get_selected_show_id = project['proj_id']
            
        for shots in self.shots:
            if shots['shot_name'] == get_selected_shot:
                get_selected_shot_id = shots['scope_id']

        self.task_types = self.thadam_api_server.get_tasks(
                                                    get_selected_project_name,
                                                    get_selected_show_id,
                                                    get_selected_shot_id
        )
            
        for task_types in self.task_types:
            tasks.add(task_types['type_name'])
        for task_types in sorted(tasks):
            self.task_combo_box.addItem(task_types)
        self.task_combo_box.setCurrentIndex(-1)
        self.task_combo_box.activated[str].connect(self.gather_subtask)
    
    def add_subtask(self) -> None:
        
        """
        Pop ups a Qinput dialog for user to enter the task name 
        Task name added to the task list view once all the naming
        rules were satisfied. if name entered by the user is wrong 
        show warning and re open input dialog for user. 
        """
        def input_sub_task():
            
            """ Recursive function to show the input dialog to the 
            user to enter sub task name"""
            
            sub_task_name, status = QtWidgets.QInputDialog.getText(
                self, 'Subtasker', 'Enter SubTask name:')

            if sub_task_name and status:
                
                if self.name_restriction(sub_task_name):
                    item = QtGui.QStandardItem(sub_task_name)
                    self.model.appendRow(item)
                    self.sub_task_list.setModel(self.model)
                else:
                    msg = "Naming Rule OverRide!!\n\n"
                    
                    msg += "Naming Rules\n"
                    msg += "------------\n\n"
                    msg += "1. Name Should Not Starts with number or any special character\n"
                    msg += "2. Capital Letters are not allowed\n"
                    msg += "3. Under score allowed\n\n"
                    
                    msg += "Allowed Naming Convention\n"
                    msg += "Examples\n"
                    msg += "-------\n"
                    msg += "fire1\n"
                    msg += "smoke_1\n"
                    msg += "bg_fire1_slow_1"
     
                    QtWidgets.QMessageBox.information(self,
                                'Information',
                                msg)
                    input_sub_task()
        input_sub_task()
    
    def remove_subtask(self) -> None:
        
        """Remove Selected sub task from the list"""
        
        row = self.sub_task_list.selectionModel().selectedIndexes()[0].row()
        self.sub_task_list.model().removeRow(row)
    
    def is_all_selected(self) -> bool:
        
        """Return if all the combobox value selected by user
        
        Return:
            boolean true 
        """
        project = self.show_combo_box.currentText()
        seq = self.sequence_combo_box.currentText()
        shot = self.shot_combo_box.currentText()
        task = self.task_combo_box.currentText()
        
        if all([project,
               seq,
               shot,
               task]):
            return True
    
    def gen_subtask_file_path(self) -> str:
        
        """ Return the path of the subtask file path"""
        
        return os.path.join(
            ROOT_SUB_TASK_DIR,
            self.show_combo_box.currentText(),
            self.sequence_combo_box.currentText(),
            self.shot_combo_box.currentText(),
            self.task_combo_box.currentText(),
            "subtasks.json"
        )
            
        
    def make_subtask(self) -> None:
        
        """
        COllect user entered sub tasks and register in to 
        the json file in the shot subtask path and close
        the qt master window.
        """
        model = self.sub_task_list.model()
        
        if self.is_all_selected() and \
                    self.sub_task_list.model().rowCount() > 0:
            tasks = set()
            for index in range(model.rowCount()):
                item = model.item(index)
                tasks.add(item.text())
            
            subtask_file = self.gen_subtask_file_path()
            if not os.path.exists(os.path.dirname(subtask_file)):
                os.makedirs(os.path.dirname(subtask_file))
            
            with open(subtask_file, "w") as subtaskfile:
                json.dump(list(tasks),subtaskfile, indent=4)
            
            info = '\n'.join(list(tasks))
            info += "\n\n Sub Tasks Were Successfully Registered!!" 
            
            QtWidgets.QMessageBox.information(self,
                            'Information',
                            info)
        else:
             QtWidgets.QMessageBox.information(self,
                            'Information',
                            "All Fields Were Mandatory to set!!")
    
    def gather_subtask(self) -> None:
        
        """ Read all the subtask from the giver shot and task 
        load it into the list"""
        
        self.model.clear()
        subtask_file = self.gen_subtask_file_path()
        if os.path.exists(subtask_file):
            with open(subtask_file, "r") as subtaskfile:
                subtasks = json.load(subtaskfile)
            
            for subtask in subtasks:
                item = QtGui.QStandardItem(subtask)
                self.model.appendRow(item)
                self.sub_task_list.setModel(self.model)
    
    @staticmethod
    def name_restriction(text) -> bool:
        
        """ Implement Naming Rule Checks"""
        
        if text[0].isdigit():
            return False
        elif text.startswith('_'):
            return False 
        elif text.endswith('_'):
            return False 
        elif not re.match(r'^[a-z0-9_]*$', text):
            return False
        else:
            return True
        
             
        
if __name__ == "__main__":
    
    app = QtWidgets.QApplication(sys.argv)
    pfx_sub_task_maker = PfxSubTaskMaker()
    pfx_sub_task_maker.subtask_window.show()
    app.exec_()