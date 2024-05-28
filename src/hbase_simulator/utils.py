import pandas as pd
from collections import OrderedDict

def scan_to_dataframe(scan_data, only_latest_version=False):
    rows = []
    
    for row_key, row_data in scan_data:
        for column_family, columns in row_data.items():
            for column, values in columns.items():
                if only_latest_version:
                    # Add only the latest version
                    latest_value = values[-1]
                    rows.append({
                        "Row Key": row_key,
                        "Column_Family:Column": f"{column_family}:{column}",
                        "Timestamp": latest_value['timestamp'],
                        "Value": latest_value['value']
                    })
                else:
                    # Add all versions
                    for value_entry in values:
                        rows.append({
                            "Row Key": row_key,
                            "Column_Family:Column": f"{column_family}:{column}",
                            "Timestamp": value_entry['timestamp'],
                            "Value": value_entry['value']
                        })
                    
    df = pd.DataFrame(rows, columns=["Row Key", "Column_Family:Column", "Timestamp", "Value"])
    return df
