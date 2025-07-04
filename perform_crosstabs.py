import pandas as pd
import json
import re


# Show all columns and full width when printing DataFrames
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)  # Optional: don't truncate long text


# === PARAMETERS ===
adm0_iso3 = "COD"
round_num = 9
indicator_name = "hh_agricactivity"
export_csv = True  # Set to False to disable export

# === LOAD INDICATOR METADATA ===
with open("indicators_metadata.js", "r", encoding="utf-8") as f:
    js_text = f.read()

js_clean = re.sub(r"^window\.indicatorData\s*=\s*", "", js_text.strip())
if js_clean.endswith(";"):
    js_clean = js_clean[:-1]
indicator_data = json.loads(js_clean)

if indicator_name not in indicator_data:
    raise ValueError(f"Indicator '{indicator_name}' not found in metadata")

indicator_title = indicator_data[indicator_name].get("title", indicator_name)

indicator_codes = indicator_data[indicator_name]["codes"]
print(f"Loaded metadata for '{indicator_name}' with {len(indicator_codes)} codes.")



# === SHOCK and NEED FIELDS ===
shock_fields = [
    "shock_noshock", "shock_sicknessordeathofhh", "shock_lostemplorwork",
    "shock_otherintrahhshock", "shock_higherfoodprices", "shock_higherfuelprices",
    "shock_mvtrestrict", "shock_othereconomicshock", "shock_pestoutbreak",
    "shock_plantdisease", "shock_animaldisease", "shock_napasture",
    "shock_othercropandlivests", "shock_coldtemporhail", "shock_flood",
    "shock_hurricane", "shock_drought", "shock_earthquake", "shock_landslides",
    "shock_firenatural", "shock_othernathazard", "shock_violenceinsecconf",
    "shock_theftofprodassets", "shock_firemanmade", "shock_othermanmadehazard",
    "shock_dk", "shock_ref"
]

need_fields = [
    "need_food", "need_cash", "need_vouchers_fair", "need_crop_inputs",
    "need_crop_infrastructure", "need_crop_knowledge", "need_ls_feed",
    "need_ls_vet_service", "need_ls_infrastructure", "need_ls_knowledge",
    "need_fish_inputs", "need_fish_infrastructure", "need_fish_knowledge",
    "need_env_infra_rehab", "need_cold_storage", "need_marketing_supp",
    "need_other", "need_dk", "need_ref"
]

# === Assistance Provided Fields ===
assistance_provided_fields = [
    "need_received_food", "need_received_cash", "need_received_vouchers_fair",
    "need_received_crop_assist", "need_received_ls_assist", "need_received_fish_assist",
    "need_received_rehabilitation", "need_received_sales_support",
    "need_received_other", "need_received_none", "need_received_dk", "need_received_ref"
]


# === Load CSV and filter ===
csv_path = r"C:\git\crossview_processing\DIEM_micro20250703_CODR9.csv"
df = pd.read_csv(csv_path)
df = df[(df["adm0_iso3"] == adm0_iso3) & (df["round"] == round_num)]
print(f"Fetched {len(df)} records.")


# === Weighted percentage function ===
def compute_weighted_percentages(df: pd.DataFrame, fields: list) -> dict:
    result = {}
    for field in fields:
        sum_w = 0.0
        total_w = 0.0
        for _, row in df.iterrows():
            val = row.get(field)
            try:
                w = float(row.get("weight_final", 0))
                if not w > 0 or pd.isnull(val) or val == "":
                    continue
                if val == 1 or val == "1" or val is True:
                    sum_w += w
                total_w += w
            except:
                continue
        result[field] = (sum_w / total_w * 100) if total_w > 0 else 0.0
    return result


# === Initialize container for all analyses ===
result_dfs = []

# === SHOCKS STATS ===
shock_results = {}

for code, label in indicator_codes.items():
    group_df = df[df[indicator_name].astype(str) == code]
    if len(group_df) == 0:
        continue
    print(f"Processing SHOCKS for group '{label}' ({len(group_df)} records)...")
    shock_results[label] = compute_weighted_percentages(group_df, shock_fields)

shock_df = pd.DataFrame(shock_results).T.round(1)
shock_total = compute_weighted_percentages(df, shock_fields)
shock_df.loc["TOTAL"] = pd.Series(shock_total).round(1)

shock_df.index.name = indicator_title
print("\n=== Weighted SHOCKS (% of HH) ===\n")
print(shock_df)

# Append to list
result_dfs.append({
    "title": f"Weighted SHOCKS (% of HH)",
    "df": shock_df
})


# === NEEDS STATS ===
need_results = {}

for code, label in indicator_codes.items():
    group_df = df[df[indicator_name].astype(str) == code]
    if len(group_df) == 0:
        continue
    print(f"Processing NEEDS for group '{label}' ({len(group_df)} records)...")
    need_results[label] = compute_weighted_percentages(group_df, need_fields)

need_df = pd.DataFrame(need_results).T.round(1)
need_total = compute_weighted_percentages(df, need_fields)
need_df.loc["TOTAL"] = pd.Series(need_total).round(1)

need_df.index.name = indicator_title
print("\n=== Weighted NEEDS (% of HH) ===\n")
print(need_df)

# Append to list
result_dfs.append({
    "title": f"Weighted NEEDS (% of HH)",
    "df": need_df
})




# === INCOME COMP STATS ===
income_categories = {
    1: "A lot more",
    2: "Slightly more",
    3: "Same",
    4: "Slightly less",
    5: "A lot less",
    888: "Don't know",
    999: "Refused"
}
income_keys = list(income_categories.keys())

income_rows = []

for code, label in indicator_codes.items():
    group_df = df[df[indicator_name].astype(str) == code]
    if len(group_df) == 0:
        continue

    print(f"Processing INCOME for group '{label}' ({len(group_df)} records)...")

    weighted_counts = {k: 0.0 for k in income_keys}
    total_weight = 0.0

    for _, row in group_df.iterrows():
        w = row.get("weight_final")
        val = row.get("income_main_comp")

        try:
            w = float(w)
            val = int(val)
        except:
            continue

        if not (w > 0) or val not in income_keys:
            continue

        weighted_counts[val] += w
        total_weight += w

    row_data = {income_categories[k]: (weighted_counts[k] / total_weight * 100 if total_weight > 0 else 0.0) for k in income_keys}
    row_data[indicator_title] = label
    income_rows.append(row_data)

income_df = pd.DataFrame(income_rows).set_index(indicator_title).round(1)

# Compute total weighted percentages for income categories
weighted_counts = {k: 0.0 for k in income_keys}
total_weight = 0.0

for _, row in df.iterrows():
    try:
        w = float(row.get("weight_final"))
        val = int(row.get("income_main_comp"))
    except:
        continue
    if not (w > 0) or val not in income_keys:
        continue
    weighted_counts[val] += w
    total_weight += w

total_row = {income_categories[k]: (weighted_counts[k] / total_weight * 100 if total_weight > 0 else 0.0) for k in income_keys}
income_df.loc["TOTAL"] = pd.Series(total_row).round(1)


print("\n=== Weighted INCOME CHANGES (% of HH) ===\n")
print(income_df)

result_dfs.append({
    "title": "Income change from main source (% of HH)",
    "df": income_df
})



# === FIES STATS: p_mod and p_sev ===
fies_fields = {
    "p_mod": "% moderate/severe (p_mod)",
    "p_sev": "% severe only (p_sev)"
}

fies_rows = []

for code, label in indicator_codes.items():
    group_df = df[df[indicator_name].astype(str) == code]
    if len(group_df) == 0:
        continue

    print(f"Processing FIES for group '{label}' ({len(group_df)} records)...")

    row_data = {}
    for field, field_label in fies_fields.items():
        weighted_sum = 0.0
        total_weight = 0.0

        for _, row in group_df.iterrows():
            try:
                w = float(row.get("weight_final", 0))
                val = float(row.get(field, 0))
            except:
                continue
            if not (w > 0) or not pd.notna(val):
                continue
            weighted_sum += w * val
            total_weight += w

        percentage = (weighted_sum / total_weight * 100) if total_weight > 0 else 0.0
        row_data[field_label] = round(percentage, 1)

    row_data[indicator_title] = label
    fies_rows.append(row_data)

fies_df = pd.DataFrame(fies_rows).set_index(indicator_title)
total_row = {}
for field, field_label in fies_fields.items():
    weighted_sum = 0.0
    total_weight = 0.0

    for _, row in df.iterrows():
        try:
            w = float(row.get("weight_final", 0))
            val = float(row.get(field, 0))
        except:
            continue
        if not (w > 0) or not pd.notna(val):
            continue
        weighted_sum += w * val
        total_weight += w

    percentage = (weighted_sum / total_weight * 100) if total_weight > 0 else 0.0
    total_row[field_label] = round(percentage, 1)

fies_df.loc["TOTAL"] = pd.Series(total_row)


print("\n=== FIES (Food Insecurity Experience Scale) ===\n")
print(fies_df)

result_dfs.append({
    "title": "FIES: Prevalence of food insecurity (% of HH)",
    "df": fies_df
})





# === AGRICULTURAL DEPENDENCY STATS ===
agriculture_categories = {
    1: "Yes - crop production",
    2: "Yes - livestock production",
    3: "Yes - both crop and livestock production",
    4: "No",
    888: "Don't know",
    999: "Refused"
}
agriculture_keys = list(agriculture_categories.keys())

agriculture_rows = []

for code, label in indicator_codes.items():
    group_df = df[df[indicator_name].astype(str) == code]
    if len(group_df) == 0:
        continue

    print(f"Processing AGRICULTURE for group '{label}' ({len(group_df)} records)...")

    weighted_counts = {k: 0.0 for k in agriculture_keys}
    total_weight = 0.0

    for _, row in group_df.iterrows():
        try:
            w = float(row.get("weight_final"))
            val = int(row.get("hh_agricactivity"))
        except:
            continue
        if not (w > 0) or val not in agriculture_keys:
            continue

        weighted_counts[val] += w
        total_weight += w

    row_data = {agriculture_categories[k]: (weighted_counts[k] / total_weight * 100 if total_weight > 0 else 0.0) for k in agriculture_keys}
    row_data[indicator_title] = label
    agriculture_rows.append(row_data)

# Build the DataFrame
agriculture_df = pd.DataFrame(agriculture_rows).set_index(indicator_title).round(1)

# === TOTAL (entire population)
weighted_counts = {k: 0.0 for k in agriculture_keys}
total_weight = 0.0

for _, row in df.iterrows():
    try:
        w = float(row.get("weight_final"))
        val = int(row.get("hh_agricactivity"))
    except:
        continue
    if not (w > 0) or val not in agriculture_keys:
        continue
    weighted_counts[val] += w
    total_weight += w

total_row = {agriculture_categories[k]: (weighted_counts[k] / total_weight * 100 if total_weight > 0 else 0.0) for k in agriculture_keys}
agriculture_df.loc["TOTAL"] = pd.Series(total_row).round(1)

print("\n=== Weighted AGRICULTURAL DEPENDENCY (% of HH) ===\n")
print(agriculture_df)

result_dfs.append({
    "title": "Agricultural dependency (% of HH)",
    "df": agriculture_df
})



# === SATISFACTION WITH ASSISTANCE RECEIVED ===
assistance_categories = {
    1: "Yes",
    2: "No - not received on time",
    3: "No - did not meet my needs",
    4: "No - quantity was not sufficient",
    5: "No - problem with provider",
    6: "No - malfunction of in-kind assistance",
    888: "Don't know",
    999: "Refused"
}
assistance_keys = list(assistance_categories.keys())

assistance_rows = []

for code, label in indicator_codes.items():
    group_df = df[df[indicator_name].astype(str) == code]
    if len(group_df) == 0:
        continue

    print(f"Processing ASSISTANCE for group '{label}' ({len(group_df)} records)...")

    weighted_counts = {k: 0.0 for k in assistance_keys}
    total_weight = 0.0

    for _, row in group_df.iterrows():
        try:
            w = float(row.get("weight_final"))
            val = int(row.get("assistance_quality"))
        except:
            continue
        if not (w > 0) or val not in assistance_keys:
            continue
        if val in assistance_keys:
            weighted_counts[val] += w
            total_weight += w

    row_data = {assistance_categories[k]: (weighted_counts[k] / total_weight * 100 if total_weight > 0 else 0.0) for k in assistance_keys}
    row_data[indicator_title] = label
    assistance_rows.append(row_data)

# Build the DataFrame
assistance_df = pd.DataFrame(assistance_rows).set_index(indicator_title).round(1)

# === TOTAL row
weighted_counts = {k: 0.0 for k in assistance_keys}
total_weight = 0.0

for _, row in df.iterrows():
    try:
        w = float(row.get("weight_final"))
        val = int(row.get("assistance_quality"))
    except:
        continue
    if not (w > 0) or val not in assistance_keys:
        continue
    if val in assistance_keys:
        weighted_counts[val] += w
        total_weight += w

total_row = {assistance_categories[k]: (weighted_counts[k] / total_weight * 100 if total_weight > 0 else 0.0) for k in assistance_keys}
assistance_df.loc["TOTAL"] = pd.Series(total_row).round(1)

print("\n=== Satisfaction with assistance received (% of HH) ===\n")
print(assistance_df)

result_dfs.append({
    "title": "Satisfaction with assistance received (% of HH)",
    "df": assistance_df
})




# === ASSISTANCE PROVIDED STATS ===
assistance_provided_results = {}

for code, label in indicator_codes.items():
    group_df = df[df[indicator_name].astype(str) == code]
    if len(group_df) == 0:
        continue

    print(f"Processing ASSISTANCE PROVIDED for group '{label}' ({len(group_df)} records)...")

    sums = {field: 0.0 for field in assistance_provided_fields}
    totals = {field: 0.0 for field in assistance_provided_fields}

    for _, row in group_df.iterrows():
        try:
            w = float(row.get("weight_final", 0))
        except:
            continue
        if not w > 0:
            continue

        for field in assistance_provided_fields:
            val = row.get(field)
            if val == 1 or val == "1" or val is True:
                sums[field] += w
            if val is not None and val != "":
                totals[field] += w

    row_data = {
        field.replace("need_received_", "").replace("_", " "):
            (sums[field] / totals[field] * 100 if totals[field] > 0 else 0.0)
        for field in assistance_provided_fields
    }

    assistance_provided_results[label] = row_data

# Build DataFrame
assist_provided_df = pd.DataFrame(assistance_provided_results).T.round(1)
assist_provided_df.index.name = indicator_title

# === TOTAL (entire population)
sums = {field: 0.0 for field in assistance_provided_fields}
totals = {field: 0.0 for field in assistance_provided_fields}

for _, row in df.iterrows():
    try:
        w = float(row.get("weight_final", 0))
    except:
        continue
    if not w > 0:
        continue

    for field in assistance_provided_fields:
        val = row.get(field)

        try:
            v = int(val)
        except:
            continue  # skip if val is None, "", or not convertible to int

        if v == 1:
            sums[field] += w
        if v in [0, 1]:
            totals[field] += w

total_row = {
    field.replace("need_received_", "").replace("_", " "):
        (sums[field] / totals[field] * 100 if totals[field] > 0 else 0.0)
    for field in assistance_provided_fields
}

assist_provided_df.loc["TOTAL"] = pd.Series(total_row).round(1)

print("\n=== Assistance Provided (% of HH) ===\n")
print(assist_provided_df)

result_dfs.append({
    "title": "Types of assistance received (% of HH)",
    "df": assist_provided_df
})





# Optional: export
# === Export all analyses to Excel ===
if export_csv:
    from openpyxl import Workbook
    from openpyxl.utils.dataframe import dataframe_to_rows

    wb = Workbook()
    ws = wb.active
    ws.title = "Grouped Analysis"

    # Write all results
    for i, result in enumerate(result_dfs):
        if i > 0:
            ws.append([])  # Spacer row
        ws.append([f"Indicator used for grouping: {indicator_title}"])
        ws.append([result["title"]])
        for r in dataframe_to_rows(result["df"].reset_index(), index=False, header=True):
            ws.append(r)

    # Save
    output_path = csv_path.replace(".csv", "_grouped_analysis.xlsx")
    wb.save(output_path)
    print(f"\nExported grouped analysis to: {output_path}")


