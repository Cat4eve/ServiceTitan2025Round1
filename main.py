from data_extractor import DataExtractor

if __name__ == '__main__':
    D = DataExtractor()
    D._load_dataset('customer_orders.pkl')
    D._load_vip_customers('vip_customers.txt')
    D._transform_to_flat_data()