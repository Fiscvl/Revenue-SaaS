
from datetime import *
from msilib.schema import Class
from dateutil.relativedelta import *
#from utilities import *
import warnings

import pandas as pd
from Constants import *
from Formats import *
from Collections import *

class CProducts():
    
    def __init__(self, products_list):

    	self.products_list = products_list
    	self.Product_index = self.getProductIndexes

    def get_product_account(self, product_name):

    	product_account = self.products_list.index(product_name)
    	return product_account

    def getProductIndexes(self):

            Product_index = []
            for product_row in self.products_list:
                    Product_index.append(product_row[kProductNameIndex])
            return Product_index