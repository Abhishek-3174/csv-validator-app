# -*- coding: utf-8 -*-
import pandas as pd

#csv_file = "batch_balances_2025-05-27_214100.csv"
#csv_file = "campaigns_2025057_123440 (1).csv"
#csv_file = "end_customers_2025-05-27_213300.csv"
#csv_file = "locations_2025_05-27-211700.csv"
#csv_file = "product_groups_2025-05-27_212400.csv"
#csv_file = "product_locations_2025-05-27_212900.csv"
#csv_file = "products_2025_05-27_211900.csv"
csv_file = "suppliers_2025_05-27_213500.csv"
#csv_file = "historical_sales2023_2025-05-28_120300 (1).csv"
#unique_fields = ['system_date', 'branch_plant', 'item_number']
#unique_fields = ['adNum']
#unique_fields = ['address_number']
#unique_fields = ['address_number']
#unique_fields = ['product_group_1','product_group_2','product_group_3']
#unique_fields = ['item_number','branch_plant']
#unique_fields = ['item_number']
#unique_fields = ['address_number']
#mandatory_fields = ['system_date', 'branch_plant', 'item_number','relex_transaction_type','quantity_on_hand','expire_date'] 
#mandatory_fields = ['adNum', 'adtype', 'FirstEffectiveDate','LastEffectiveDate','Class'] 
#mandatory_fields = ['address_number', 'address_line3', 'address_line4','customer_group'] 
#mandatory_fields = ['address_number', 'alpha_name', 'relex_location_type'] 
#mandatory_fields = ['product_group_1','product_group_1_desc']
#mandatory_fields = ['item_number','branch_plant']
#mandatory_fields = ['item_number','item_description','product_group_3','net_weight','weight_unit_of_measure']
mandatory_fields = ['address_number','alpha_name']
#mandatory_fields = ['actual_ship_date','branch_plant','item_number','requested_date','quantity_shipped','shipto_number','relex_transaction_type']


def validate_and_report_missing(file_path, mandatory_columns):
    try:
        df = pd.read_csv(file_path,encoding="ISO-8859-1")
        missing_columns = [col for col in mandatory_columns if col not in df.columns]
        if missing_columns:
            print(f"❌ Missing mandatory columns: {missing_columns}")
            return

        # Count missing values per field
        null_counts = df[mandatory_columns].isnull().sum()
        null_counts = null_counts[null_counts > 0]

        if not null_counts.empty:
            print("❌ Missing field metrics:")
            for field, count in null_counts.items():
                print(f" - {field}: {count} missing")
            
            # Show affected rows
            missing_rows = df[df[mandatory_columns].isnull().any(axis=1)]
            print(f"\n❌ Found {len(missing_rows)} rows with at least one missing mandatory field:")
            print(missing_rows)
        else:
            print("✅ Good")

    except Exception as e:
        print(f"Error: {e}")

validate_and_report_missing(csv_file, mandatory_fields)

def find_duplicates(file_path, key_columns):
    try:
        df = pd.read_csv(file_path)
        missing_cols = [col for col in key_columns if col not in df.columns]
        if missing_cols:
            print(f"❌ Missing columns: {missing_cols}")
            return

        dup_values = df[key_columns].duplicated(keep=False)
        unique_duplicates = df.loc[dup_values, key_columns].drop_duplicates()

        print(f"❗ Found {len(unique_duplicates)} unique duplicated key combinations:")
        #print(unique_duplicates.to_string(index=False))

    except Exception as e:
        print(f"Error: {e}")


#find_duplicates(csv_file, unique_fields)