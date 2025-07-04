import pandas as pd
import json

# === Input file paths ===
descr_path = r'C:\git\crossview_processing\micro_fields_descr.xlsx'
codes_path = r'C:\git\crossview_processing\micro_codes.xlsx'

# === Load field descriptions ===
descr_df = pd.read_excel(descr_path)

# === Load the multi-sheet Excel with code-label mappings ===
xls = pd.ExcelFile(codes_path)

# === Initialize final dictionary ===
indicators_dict = {}

# === Process each sheet ===
for sheet_name in xls.sheet_names:
    codes_df = xls.parse(sheet_name)

    # Make sure the sheet has 'code' and 'label' columns
    if 'code' in codes_df.columns and 'label' in codes_df.columns:
        code_label_dict = pd.Series(codes_df['label'].values, index=codes_df['code']).dropna().to_dict()
    else:
        code_label_dict = {}

    # Get the description from the first Excel file
    desc_row = descr_df[descr_df.iloc[:, 0] == sheet_name]
    description = desc_row.iloc[0, 1] if not desc_row.empty else None

    # Add the structured entry with 4 keys
    indicators_dict[sheet_name] = {
        'title': sheet_name,
        'description': description,
        'active': True,
        'codes': code_label_dict
    }

# === Optional: preview sample
for k, v in list(indicators_dict.items())[:3]:
    print(f"{k}:\n  Title: {v['title']}\n  Description: {v['description']}\n  Active: {v['active']}\n  Codes: {list(v['codes'].items())[:5]}\n")

# === Export as a JS file using window namespace ===
js_variable_name = "indicatorData"
js_output_path = r'C:\git\crossview_processing\indicators_metadata.js'

with open(js_output_path, 'w', encoding='utf-8') as f:
    f.write(f"window.{js_variable_name} = ")
    json.dump(indicators_dict, f, ensure_ascii=False, indent=2)
    f.write(";")


