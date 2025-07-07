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

# === Add binary Yes/No domain to missing binary indicators ===
binary_indicators = [
    "need", "need_food", "need_cash", "need_vouchers_fair", "need_crop_inputs", "need_crop_infrastructure",
    "need_crop_knowledge", "need_ls_feed", "need_ls_vet_service", "need_ls_infrastructure", "need_ls_knowledge",
    "need_fish_inputs", "need_fish_infrastructure", "need_fish_knowledge", "need_env_infra_rehab",
    "need_cold_storage", "need_marketing_supp", "need_other", "need_dk", "need_ref",
    "need_received_food", "need_received_cash", "need_received_vouchers_fair", "need_received_crop_assist",
    "need_received_ls_assist", "need_received_fish_assist", "need_received_rehabilitation",
    "need_received_sales_support", "need_received_other", "need_received_none", "need_received_dk",
    "need_received_ref", "assistance_quality", "assistance_fao", "assistance_wfp", "assistance_otherun",
    "assistance_gov", "assistance_ngo", "assistance_dk", "assistance_ref"
]

yes_no_dict = {"1": "Yes", "0": "No"}

for field in binary_indicators:
    if field not in indicators_dict:
        # Try to get description from descr_df
        desc_row = descr_df[descr_df.iloc[:, 0] == field]
        description = desc_row.iloc[0, 1] if not desc_row.empty else None

        indicators_dict[field] = {
            'title': field,
            'description': description,
            'active': True,
            'codes': yes_no_dict
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
