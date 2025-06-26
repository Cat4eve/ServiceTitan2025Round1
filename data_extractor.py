import pickle
import pandas as pd
import numpy as np
import string
from datetime import datetime

class DataExtractor:
    
    # Initialization of the class
    def __init__(self) -> None:
        
        # Attribute for storing in data from pickle file
        self.extracted_data = None

        # Attribute for storing expired invoices
        self.list_of_vip = None

        # Conversion for invoice item types
        self.type_conversation = {1: 'Electronics', 2: 'Apparel', 3: 'Books', 4: 'Home Goods'}
        


    # Loading of data from pickle file from given URL location.
    def _load_dataset(self, url) -> list:

        #find file and open it
        file = open(url, 'rb')

        # load data using pickle load method
        data = pickle.load(file)

        # save the last extracted data as a property named 'extracted_data'
        self.extracted_data = data

        # return the given data of type list
        return data
    
    
    def _load_vip_customers(self, url) -> list:
        # open file and read it
        file = open(url, 'r')

        # define list containing vip customer's id
        list_of_vip = []

        # get id from multiple lines
        for line in file.readlines():
            if len(line.strip()) > 0:
                list_of_vip.append(int(line.strip()))

        

        # save as 'expired_list' attribute
        self.list_of_vip = list_of_vip

        # return value
        return list_of_vip
        
        

    # Transformation of data from s
    def _transform_to_flat_data(self) -> None:
        
        # Creation of dictionary which will be converted to dataframe
        dictionary_to_be_convereted_to_dataframe = {'customer_id': None,'customer_name': None,'registration_date': None,
                                                    'is_vip': None,'order_id': None,'order_date': None,'product_id': None,
                                                    'product_name': None,'category': None, 'unit_price': None,
                                                    'item_quantity': None, 'total_item_price': None,
                                                    'total_order_value_percentage': None}
        

        # Creation of respective arrays for computaions
        customer_id_arr = []
        customer_name_arr = []
        registration_date_arr = []
        is_vip_arr = []
        order_id_arr = []
        order_date_arr = []
        product_id_arr = []
        product_name_arr = []
        category_arr = []
        unit_price_arr = []
        item_quantity_arr = []
        total_item_price_arr = []
        total_order_value_percentage_arr = []
        

        # Iteration over all invoices
        for item in self.extracted_data:
            if item.get('registration_date') == None:
                continue

            if item.get('orders') == None or len(item.get('orders')) == 0:
                continue

            if self._is_not_valid_date(item.get('registration_date')):
                continue

            orders = item.get('orders')

            for order in orders:
                
                total_order_value = 0
                order_percantage_push_counter = 0

                if order.get('order_id') == None:
                    continue

                if order.get('order_date') == None or self._is_not_valid_date(order.get('order_date')):
                    continue
                
                if order.get('items') == None or len(order.get('items')) == 0:
                    continue

                items = order.get('items')
                for product in items:
                    
                    if product.get('item_id') == None:
                        continue

                    if product.get('price') == None or (isinstance(product.get('price'),
                                                                   str) and product.get('price')[0] != '$'):
                        continue

                    if isinstance(product.get('quantity'), str):
                        continue


                    category = product.get('category')

                    customer_id_arr.append(item.get('id'))
                    customer_name_arr.append(item.get('name'))

                    registration_date = item.get('registration_date')
                    if "/" in registration_date:
                        registration_date = datetime.strptime(registration_date, "%Y/%m/%d")
                    elif "-" in registration_date:
                        registration_date = datetime.strptime(registration_date, "%Y-%m-%d %H:%M:%S")
                    registration_date_arr.append(registration_date)
                    is_vip_arr.append(self._is_vip(item.get('customer_id')))

                    order_id = order.get('order_id')

                    if (isinstance(order_id, str)):
                        if order_id.startswith("ORD"):
                            order_id = int(order_id[3:])
                    order_id_arr.append(order_id)

                    order_date_arr.append(order.get('order_date'))

                    product_id_arr.append(product.get('product_id'))
                    product_name_arr.append(product.get('product_name'))

                    

                    if isinstance(category, int):
                        if 1 <= category and 4 >= category:
                            category_arr.append(self.type_conversation[category])
                        else:
                            category_arr.append('Misc')
                    else:
                        category_arr.append(string.capwords(category))

                    price = product.get('price')
                    if isinstance(price, str):
                        if price[0] == "$":
                            price = float(price[1:])
                    else:
                        price = float(price)

                    quanity = int(product.get('quantity'))

                    unit_price_arr.append(price)
                    item_quantity_arr.append(quanity)
                    total_item_price_arr.append(price*quanity)
                    total_order_value += (price*quanity)
                    order_percantage_push_counter += 1

                for _ in range(order_percantage_push_counter):
                    total_order_value_percentage_arr.append(total_order_value)

        for i in range(len(customer_id_arr)):
            total_order_value_percentage_arr[i] = (total_item_price_arr[i] / 
                                                   total_order_value_percentage_arr[i]) * 100

                
        dictionary_to_be_convereted_to_dataframe['customer_id'] = customer_id_arr
        dictionary_to_be_convereted_to_dataframe['customer_name'] = customer_name_arr
        dictionary_to_be_convereted_to_dataframe['registration_date'] = registration_date_arr
        dictionary_to_be_convereted_to_dataframe['is_vip'] = is_vip_arr
        dictionary_to_be_convereted_to_dataframe['order_id'] = order_id_arr
        dictionary_to_be_convereted_to_dataframe['order_date'] = order_date_arr
        dictionary_to_be_convereted_to_dataframe['product_id'] = product_id_arr
        dictionary_to_be_convereted_to_dataframe['product_name'] = product_name_arr
        dictionary_to_be_convereted_to_dataframe['category'] = category_arr
        dictionary_to_be_convereted_to_dataframe['unit_price'] = unit_price_arr
        dictionary_to_be_convereted_to_dataframe['item_quantity'] = item_quantity_arr
        dictionary_to_be_convereted_to_dataframe['total_item_price'] = total_item_price_arr
        dictionary_to_be_convereted_to_dataframe['total_order_value_percentage'] = total_order_value_percentage_arr

        # conversion
        data_frame = pd.DataFrame(data=dictionary_to_be_convereted_to_dataframe)

        # soring
        data_frame = data_frame.sort_values(["customer_id", "order_id", "product_id"],
                                            ascending=[True, True, True])

        # extraction 
        data_frame.to_csv('Data_Extraction_Results.csv')
        return data_frame


    # helper methods
    def _is_vip(self, customer_id: int):
        return customer_id in self.list_of_vip

    def _is_not_valid_date(self, date: str):
        try:
            pd.to_datetime(date)
            return False
        except ValueError:
            return True