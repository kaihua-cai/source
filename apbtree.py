import pandas as pd
import numpy as np

class BinaryTree(object):
    def __init__(self, parent):
        self.parent = parent

class APBTree(BinaryTree):
    def __init__(self, parent, feature_name, action, value_list):
        #super(APBTree, self).__init__(parent)
        BinaryTree.__init__(self, parent)
        self.feature_name = feature_name
        self.action = action
        self.value_list = value_list

        self.imps = None
        self.events = None
        self.true_child = None
        self.false_child = None
    
    @property
    def height(self):
        return 1 + max(self.true_child.height, self.false_child.height)
    
    def __str__(self):
        return ( '(' +  
                self.feature_name + 
                self.action + 
                str(self.value_list) + 
                self.true_child.__str__() + 
                self.false_child.__str__() +  
                ')' )
    
class APBLeaf(BinaryTree):
    def __init__(self, parent, cpm_value):
        BinaryTree.__init__(self, parent)
        self.cpm_value = cpm_value
    
    @property
    def height(self):
        return 1
    def __str__(self):
        return str(self.cpm_value)



