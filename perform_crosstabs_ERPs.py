import pandas as pd
import json, os
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
import pandas as pd
import re


######### get updated list of surveys to process ###########
# Filter to keep only selected countries
selected_countries = [
    "AFG",  # Afghanistan
    "BGD",  # Bangladesh
    "BFA",  # Burkina Faso
    "CMR",  # Cameroon
    "CAF",  # Central African Republic (CAR)
    "TCD",  # Chad
    "COL",  # Colombia
    "COD",  # Democratic Republic of the Congo (DRC)
    "SLV",  # El Salvador
    "GTM",  # Guatemala
    "HTI",  # Haiti
    "HND",  # Honduras
    "LBN",  # Lebanon
    "MLI",  # Mali
    "MOZ",  # Mozambique
    "MMR",  # Myanmar
    "NER",  # Niger
    "NGA",  # Nigeria
    "PSE",  # West Bank (Palestine)
    "YEM",  # Yemen
    "IRQ",  # Iraq
    "PAK",  # Pakistan
    "MWI",  # Malawi
    "ZWE"   # Zimbabwe
]

# selected_countries = [
#    'BGD']

get_updated_list_of_surveys_from_AGOL = True

if get_updated_list_of_surveys_from_AGOL == False:
    # survey_list =[{ 'adm0_iso3': 'AFG', 'adm0_name': 'Afghanistan', 'round_num': 10, 'coll_end_date': Timestamp('2024-07-10 00:00:00')},
    #               { 'adm0_iso3': 'HTI', 'adm0_name': 'Haiti', 'round_num': 6, 'coll_end_date': Timestamp('2024-07-10 00:00:00')}]

    survey_list =[{
        'adm0_iso3': 'BGD',
        'adm0_name': 'Bangladesh',
        'coll_end_date': pd.Timestamp('2025-01-14 00:00:00'),
        'round_num': 12
    },
    {
        'adm0_iso3': 'MMR',
        'adm0_name': 'Myanmar',
        'coll_end_date': pd.Timestamp('2025-01-14 00:00:00'),
        'round_num': 11
    },{
        'adm0_iso3': 'PAK',
        'adm0_name': 'Pakistan',
        'coll_end_date': pd.Timestamp('2025-01-14 00:00:00'),
        'round_num': 6
    },{
        'adm0_iso3': 'AFG',
        'adm0_name': 'Afghanistan',
        'coll_end_date': pd.Timestamp('2025-01-14 00:00:00'),
        'round_num': 10
    }]

    survey_list = [item for item in survey_list if item['adm0_iso3'] in selected_countries]

else:
    # Feature layer URL (Layer 0)
    layer_url = "https://services5.arcgis.com/sjP4Ugu5s0dZWLjd/arcgis/rest/services/OER_Monitoring_System_View/FeatureServer/0"
    layer = FeatureLayer(layer_url)
    # Query all features
    features = layer.query(where="round_validated='Yes'", out_fields="admin0_isocode, round, admin0_name_en, coll_end_date", return_geometry=False)
    # Convert to DataFrame
    df = features.sdf
    # Drop duplicates to get unique combinations
    unique_combinations = df.drop_duplicates(subset=["admin0_isocode", "round"]).reset_index(drop=True)
    # Clean and prepare round column
    df_clean = df.dropna(subset=["admin0_isocode", "round"]).copy()
    # Extract numeric part from "Round 01", "Round 12", etc.
    df_clean["round_num"] = df_clean["round"].str.extract(r"Round\s+(\d+)", expand=False).astype(int)
    # Get the highest round per country
    latest_rounds = df_clean.sort_values("round_num", ascending=False).drop_duplicates(subset=["admin0_isocode"])
    # Sort by country code
    latest_rounds = latest_rounds.sort_values("admin0_isocode").reset_index(drop=True)

    latest_rounds = latest_rounds[latest_rounds["admin0_isocode"].isin(selected_countries)].reset_index(drop=True)
    latest_rounds = latest_rounds.rename(columns={"admin0_isocode": "adm0_iso3", "admin0_name_en": "adm0_name"})
    survey_list = latest_rounds.to_dict(orient="records")
    # Print the result
    # for survey in survey_list:
    #     print(survey["adm0_iso3"], survey["round_num"], survey.get("coll_end_date", "N/A"))

# Show all columns and full width when printing DataFrames
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

# === PARAMETERS ===
group_indicators = {
    "fies_resid": "hh_residencetype",
    "fies_hhtype": "hh_agricactivity",
    "agriculture": "hh_residencetype"
}
export_csv = True

# === LOAD INDICATOR METADATA ===
with open("indicators_metadata.js", "r", encoding="utf-8") as f:
    js_text = f.read()
js_clean = re.sub(r"^window\.indicatorData\s*=\s*", "", js_text.strip())
if js_clean.endswith(";"):
    js_clean = js_clean[:-1]
indicator_data = json.loads(js_clean)

def get_indicator_info(indicator_name):
    if indicator_name not in indicator_data:
        raise ValueError(f"Indicator '{indicator_name}' not found in metadata")
    return {
        "title": indicator_data[indicator_name].get("title", indicator_name),
        "codes": indicator_data[indicator_name]["codes"]
    }


# === DEFINE FUNCTION  ===

def fies_by_indicator(indicator_key, df, universe_label=None):
    fies_fields = {
        "p_mod": "% moderate/severe (p_mod)",
        "p_sev": "% severe only (p_sev)"
    }
    fies_rows = []
    indicator = group_indicators[indicator_key]
    info = get_indicator_info(indicator)
    title = info["title"]
    codes = info["codes"]


    # Generate the descriptive title
    if universe_label is None:
        universe_label = "all households"
    subtitle = f"FIES weighted average by {title} — Universe: {universe_label}"

    # print(subtitle)

    for code, label in codes.items():
        if label in ["Don't know", "Refused"]:
            continue  # Skip unwanted categories
        try:
            group_df = df[df[indicator].astype("Int64") == int(code)]
        except ValueError:
            group_df = df[df[indicator].astype(str) == str(code)]

        if len(group_df) == 0:
            continue
        #print(f"Processing FIES for group '{label}' ({len(group_df)} records)...")

        row_data = {}
        for field, label_field in fies_fields.items():
            weighted_sum = 0.0
            total_weight = 0.0
            for _, row in group_df.iterrows():
                try:
                    w = float(row.get("weight_final", 0))
                    val = float(row.get(field, 0))
                    if w > 0 and pd.notna(val):
                        weighted_sum += w * val
                        total_weight += w
                except:
                    continue
            row_data[label_field] = round((weighted_sum / total_weight) * 100, 1) if total_weight > 0 else 0.0
        row_data[title] = label
        fies_rows.append(row_data)

    fies_df = pd.DataFrame(fies_rows).set_index(title)

    # TOTAL row calculation
    total_row = {}
    for field, label_field in fies_fields.items():
        weighted_sum = 0.0
        total_weight = 0.0
        for _, row in df.iterrows():
            try:
                w = float(row.get("weight_final", 0))
                val = float(row.get(field, 0))
                if w > 0 and pd.notna(val):
                    weighted_sum += w * val
                    total_weight += w
            except:
                continue
        total_row[label_field] = round((weighted_sum / total_weight) * 100, 1) if total_weight > 0 else 0.0
    fies_df.loc["TOTAL"] = pd.Series(total_row)
    if indicator_key == 'fies_hhtype':
        title = f"FIES: Prevalence of recent food insecurity (% of HH) by detailed agricultural dependency group (% of HH)"
    elif indicator_key == 'fies_resid':
        title = f"FIES: Prevalence of recent food insecurity (% of HH) by residency status (% of HH)"

    return {
        "title": title,
        "subtitle": subtitle,
        "df": fies_df
    }

def fies_by_simplified_agriculture(df):
    df = df.copy()
    df.loc[:, "agriculture_group"] = df["hh_agricactivity"].apply(lambda x: (
        "Agricultural HH" if int(x) in [1, 2, 3] else
        "Non-agricultural HH" if int(x) == 4 else
        "Don't know" if int(x) == 888 else
        "Refused" if int(x) == 999 else None
    ) if pd.notna(x) else None)

    fies_fields = {
        "p_mod": "% moderate/severe (p_mod)",
        "p_sev": "% severe only (p_sev)"
    }
    rows = []
    for group in df["agriculture_group"].dropna().unique():
        if group in ["Don't know", "Refused"]:
            continue  # Exclude these categories from the output table
        group_df = df[df["agriculture_group"] == group]
        if len(group_df) == 0:
            continue
        row_data = {}
        for field, label_field in fies_fields.items():
            weighted_sum = 0.0
            total_weight = 0.0
            for _, row in group_df.iterrows():
                try:
                    w = float(row.get("weight_final", 0))
                    val = float(row.get(field, 0))
                    if w > 0 and pd.notna(val):
                        weighted_sum += w * val
                        total_weight += w
                except:
                    continue
            row_data[label_field] = round((weighted_sum / total_weight) * 100, 1) if total_weight > 0 else 0.0
        row_data["Agriculture group"] = group
        rows.append(row_data)

    fies_df = pd.DataFrame(rows).set_index("Agriculture group")

    total_row = {}
    for field, label_field in fies_fields.items():
        weighted_sum = 0.0
        total_weight = 0.0
        for _, row in df.iterrows():
            try:
                w = float(row.get("weight_final", 0))
                val = float(row.get(field, 0))
                if w > 0 and pd.notna(val):
                    weighted_sum += w * val
                    total_weight += w
            except:
                continue
        total_row[label_field] = round((weighted_sum / total_weight) * 100, 1) if total_weight > 0 else 0.0
    fies_df.loc["TOTAL"] = pd.Series(total_row)
    return {
        "title": "FIES: Prevalence of recent food insecurity by simplified agricultural dependency group (% of HH)",
        "df": fies_df
    }


def agricultural_dependency(df):
    # Full category labels
    agriculture_categories = {
        1: "Yes - crop production",
        2: "Yes - livestock production",
        3: "Yes - both crop and livestock production",
        4: "Non-agricultural household",
        888: "Don't know",
        999: "Refused"
    }

    # Keep only valid analytical codes (exclude DK and REF)
    agriculture_keys = [1, 2, 3, 4]

    agri_indicator = group_indicators["agriculture"]
    agri_info = get_indicator_info(agri_indicator)
    agri_title = agri_info["title"]
    agri_codes = agri_info["codes"]
    rows = []

    for code, label in agri_codes.items():
        if label in ["Don't know", "Refused"]:
            continue
        try:
            group_df = df[df[agri_indicator].astype("Int64") == int(code)]
        except ValueError:
            group_df = df[df[agri_indicator].astype(str) == str(code)]

        if len(group_df) == 0:
            continue
        #print(f"Processing AGRICULTURE for group '{label}' ({len(group_df)} records)...")

        weighted_counts = {k: 0.0 for k in agriculture_keys}
        total_weight = 0.0
        for _, row in group_df.iterrows():
            try:
                w = float(row.get("weight_final"))
                val = int(row.get("hh_agricactivity"))
                if w > 0 and val in agriculture_keys:
                    weighted_counts[val] += w
                    total_weight += w
            except:
                continue

        row_data = {
            agriculture_categories[k]: (weighted_counts[k] / total_weight * 100 if total_weight > 0 else 0.0)
            for k in agriculture_keys
        }
        row_data[agri_title] = label
        rows.append(row_data)

    df_result = pd.DataFrame(rows).set_index(agri_title).round(1)

    # TOTAL
    total_counts = {k: 0.0 for k in agriculture_keys}
    total_weight = 0.0
    for _, row in df.iterrows():
        try:
            w = float(row.get("weight_final"))
            val = int(row.get("hh_agricactivity"))
            if w > 0 and val in agriculture_keys:
                total_counts[val] += w
                total_weight += w
        except:
            continue

    total_row = {
        agriculture_categories[k]: (total_counts[k] / total_weight * 100 if total_weight > 0 else 0.0)
        for k in agriculture_keys
    }
    df_result.loc["TOTAL"] = pd.Series(total_row).round(1)

    return {
        "title": "Detailed agricultural dependency by residency status (% of HH)",
        "df": df_result
    }

def simplified_dependency_by_residency(df):
    df = df.copy()
    df.loc[:, "agri_simple"] = df["hh_agricactivity"].apply(lambda x: (

        "Agricultural HH" if int(x) in [1, 2, 3] else
        "Non-agricultural HH" if int(x) == 4 else None
    ) if pd.notna(x) else None)

    agri_indicator = group_indicators["fies_resid"]
    resid_info = get_indicator_info(agri_indicator)
    resid_title = resid_info["title"]
    resid_codes = resid_info["codes"]
    rows = []

    for code, label in resid_codes.items():
        if label in ["Don't know", "Refused"]:
            continue
        try:
            group_df = df[df[agri_indicator].astype("Int64") == int(code)]
        except ValueError:
            group_df = df[df[agri_indicator].astype(str) == str(code)]
        #group_df = df[df[agri_indicator].astype(str) == code]
        if len(group_df) == 0:
            continue
        #print(f"Processing SIMPLIFIED AGRIC DEPENDENCY for residency group '{label}' ({len(group_df)} records)...")
        weighted_counts = {"Agricultural HH": 0.0, "Non-agricultural HH": 0.0}
        total_weight = 0.0
        for _, row in group_df.iterrows():
            group = row.get("agri_simple")
            try:
                w = float(row.get("weight_final", 0))
                if w > 0 and group in weighted_counts:
                    weighted_counts[group] += w
                    total_weight += w
            except:
                continue
        row_data = {
            "Agricultural HH": (weighted_counts["Agricultural HH"] / total_weight * 100 if total_weight > 0 else 0.0),
            "Non-agricultural HH": (weighted_counts["Non-agricultural HH"] / total_weight * 100 if total_weight > 0 else 0.0),
            resid_title: label
        }
        rows.append(row_data)

    df_result = pd.DataFrame(rows).set_index(resid_title).round(1)

    # TOTAL
    total_counts = {"Agricultural HH": 0.0, "Non-agricultural HH": 0.0}
    total_weight = 0.0
    for _, row in df.iterrows():
        group = row.get("agri_simple")
        try:
            w = float(row.get("weight_final", 0))
            if w > 0 and group in total_counts:
                total_counts[group] += w
                total_weight += w
        except:
            continue
    total_row = {
        "Agricultural HH": (total_counts["Agricultural HH"] / total_weight * 100 if total_weight > 0 else 0.0),
        "Non-agricultural HH": (total_counts["Non-agricultural HH"] / total_weight * 100 if total_weight > 0 else 0.0)
    }
    df_result.loc["TOTAL"] = pd.Series(total_row).round(1)

    return {
        "title": "Simplified agricultural dependency by residency status (% of HH)",
        "df": df_result
    }

def needs_summary_grouped(df_all, adm0_iso3, round_num, use_grouping=True, use_previous_round=True, universe_filter=[1]):
    import collections

    # Choose round (current or previous)
    selected_round = round_num - 1 if use_previous_round else round_num

    # Filter to selected round, country, and universe
    df_universe = df_all[
        (df_all["adm0_iso3"] == adm0_iso3) &
        (df_all["round"] == selected_round) &
        (df_all["need"].isin(universe_filter))
    ].copy()

    universe_count = len(df_universe)

    # Total weight
    total_weight = df_universe["weight_final"].apply(pd.to_numeric, errors="coerce").dropna()
    total_weight = total_weight[total_weight > 0].sum()

    # Reclassification
    need_fields_dict = {
        "need_food": "need_food",
        "need_cash": "need_cash",
        "need_other": "agricultural livelihoods",
        "need_vouchers_fair": "agricultural livelihoods",
        "need_crop_inputs": "agricultural livelihoods",
        "need_crop_infrastructure": "agricultural livelihoods",
        "need_crop_knowledge": "agricultural livelihoods",
        "need_ls_feed": "agricultural livelihoods",
        "need_ls_vet_service": "agricultural livelihoods",
        "need_ls_infrastructure": "agricultural livelihoods",
        "need_ls_knowledge": "agricultural livelihoods",
        "need_fish_inputs": "agricultural livelihoods",
        "need_fish_infrastructure": "agricultural livelihoods",
        "need_fish_knowledge": "agricultural livelihoods",
        "need_env_infra_rehab": "agricultural livelihoods",
        "need_cold_storage": "agricultural livelihoods",
        "need_marketing_supp": "agricultural livelihoods"
    }

    # Grouping logic
    if use_grouping:
        from collections import defaultdict
        group_fields = defaultdict(list)
        for field, group in need_fields_dict.items():
            group_fields[group].append(field)
    else:
        all_fields = list(need_fields_dict.keys())
        group_fields = {field: [field] for field in all_fields}

    # Initialize group sums
    group_weight_sums = {group: 0.0 for group in group_fields}

    for _, row in df_universe.iterrows():
        try:
            w = float(row.get("weight_final", 0))
            if not w > 0:
                continue
        except:
            continue

        for group, fields in group_fields.items():
            for field in fields:
                val = row.get(field)
                if val in [1, "1", True]:
                    group_weight_sums[group] += w
                    break  # Only once per group

    # Format output
    results = []
    for group, w_sum in group_weight_sums.items():
        pct = (w_sum / total_weight * 100) if total_weight > 0 else 0.0
        label = group.replace("need_", "").replace("_", " ").capitalize()
        results.append({
            "Need group": label,
            "Weighted % of HH": round(pct, 1)
        })

    df_result = pd.DataFrame(results).set_index("Need group")

    # Dynamic title
    title = (
        f"Needs reported in previous round\n"
        f"Round: {selected_round}, Country: {adm0_iso3.upper()}, Universe: need in {universe_filter}"
    )

    return {
        "title": "Needs reported in previous round",
        "metadata": f"Round: {selected_round}, Country: {adm0_iso3.upper()}, Universe: need in {universe_filter}",
        "df": df_result
    }

def assistance_summary(df, use_grouping=True, universe_filter=[1], round_num=None, adm0_iso3=None):
    import collections

    df_universe = df[df["need"].isin(universe_filter)].copy()
    universe_count = len(df_universe)
    total_weight = df_universe["weight_final"].apply(pd.to_numeric, errors="coerce").dropna()
    total_weight = total_weight[total_weight > 0].sum()

    assistance_provided_dict = {
        "need_received_food": "need_received_food",
        "need_received_cash": "need_received_cash",
        "need_received_vouchers_fair": "agricultural livelihoods",
        "need_received_crop_assist": "agricultural livelihoods",
        "need_received_ls_assist": "agricultural livelihoods",
        "need_received_fish_assist": "agricultural livelihoods",
        "need_received_rehabilitation": "agricultural livelihoods",
        "need_received_sales_support": "agricultural livelihoods",
        "need_received_other": "agricultural livelihoods",
        "need_received_none": "need_received_none"
    }

    if use_grouping:
        group_fields = collections.defaultdict(list)
        for field, group in assistance_provided_dict.items():
            group_fields[group].append(field)
    else:
        all_fields = list(assistance_provided_dict.keys())
        group_fields = {field: [field] for field in all_fields}

    group_weight_sums = {group: 0.0 for group in group_fields.keys()}
    for _, row in df_universe.iterrows():
        try:
            w = float(row.get("weight_final", 0))
            if not w > 0:
                continue
        except:
            continue
        for group, fields in group_fields.items():
            for field in fields:
                val = row.get(field)
                if val in [1, "1", True]:
                    group_weight_sums[group] += w
                    break

    results = []
    for group, w_sum in group_weight_sums.items():
        pct = (w_sum / total_weight * 100) if total_weight > 0 else 0.0
        if group == "need_received_food":
            label = "Food"
        elif group == "need_received_cash":
            label = "Cash"
        else:
            label = group.replace("need_received_", "").replace("_", " ").capitalize()

        results.append({
            "Assistance type": label,
            "Weighted % of HH": round(pct, 1)
        })

    df_result = pd.DataFrame(results).set_index("Assistance type")

    # Compose detailed title
    title = (
        f"Assistance received in round {round_num}\n"
        f"Country: {adm0_iso3.upper() if adm0_iso3 else 'N/A'}, Universe: need in {universe_filter}"
    )

    return {
        "title": f"Assistance received in round {round_num}",
        "metadata": f"Country: {adm0_iso3.upper() if adm0_iso3 else 'N/A'}, Universe: need in {universe_filter}",
        "df": df_result
    }

def compare_needs_vs_assistance(needs_result, assistance_result):
    # Extract and align dataframes
    df_needs = needs_result["df"].copy()
    df_assistance = assistance_result["df"].copy()

    # Rename value columns and drop universe columns
    df_needs = df_needs.rename(columns={
        "Weighted % of HH": "Need in previous round (%)"
    }).drop(columns=["Universe (n)"], errors="ignore")

    df_assistance = df_assistance.rename(columns={
        "Weighted % of HH": "Assistance received (%)"
    }).drop(columns=["Universe (n)"], errors="ignore")

    # Align indices
    df_needs.index.name = "Type"
    df_assistance.index.name = "Type"

    # Join both tables
    merged_df = df_needs.join(df_assistance, how="outer").fillna(0).round(1)

    return {
        "title": "Comparison: Needs in previous round vs Assistance received",
        "metadata": f"Needs → Round {round_num - 1}, Assistance → Round {round_num}, Country: {adm0_iso3.upper()}, Universe: need in [0, 1, 888]",
        "df": merged_df
    }

def assistance_quality_summary(df, group_by="need_received"):
    from collections import defaultdict

    # === Satisfaction categories (full, but 888 and 999 will be excluded later) ===
    assistance_categories = {
        1: "Yes - satisfied",
        2: "No - not received on time",
        3: "No - did not meet my needs",
        4: "No - quantity was not sufficient",
        5: "No - problem with provider",
        6: "No - malfunction of in-kind assistance",
        888: "Don't know",
        999: "Refused"
    }

    # === Grouping logic ===
    if group_by == "need_received":
        field_dict = {
            "need_received_food": "need_received_food",
            "need_received_cash": "need_received_cash",
            "need_received_vouchers_fair": "agricultural livelihoods",
            "need_received_crop_assist": "agricultural livelihoods",
            "need_received_ls_assist": "agricultural livelihoods",
            "need_received_fish_assist": "agricultural livelihoods",
            "need_received_rehabilitation": "agricultural livelihoods",
            "need_received_sales_support": "agricultural livelihoods",
            "need_received_other": "need_received_other",
            "need_received_none": "need_received_none",
            "need_received_dk": "need_received_dk",
            "need_received_ref": "need_received_ref"
        }
        group_label = "Received"
    elif group_by == "need":
        field_dict = {
            "need_food": "need_food",
            "need_cash": "need_cash",
            "need_other": "agricultural livelihoods",
            "need_dk": "need_dk",
            "need_ref": "need_ref",
            "need_vouchers_fair": "agricultural livelihoods",
            "need_crop_inputs": "agricultural livelihoods",
            "need_crop_infrastructure": "agricultural livelihoods",
            "need_crop_knowledge": "agricultural livelihoods",
            "need_ls_feed": "agricultural livelihoods",
            "need_ls_vet_service": "agricultural livelihoods",
            "need_ls_infrastructure": "agricultural livelihoods",
            "need_ls_knowledge": "agricultural livelihoods",
            "need_fish_inputs": "agricultural livelihoods",
            "need_fish_infrastructure": "agricultural livelihoods",
            "need_fish_knowledge": "agricultural livelihoods",
            "need_env_infra_rehab": "agricultural livelihoods",
            "need_cold_storage": "agricultural livelihoods",
            "need_marketing_supp": "agricultural livelihoods"
        }
        group_label = "Need"
    else:
        raise ValueError("group_by must be either 'need_received' or 'need'")

    group_fields = defaultdict(list)
    for field, group in field_dict.items():
        group_fields[group].append(field)

    # Only use the valid categories for output
    valid_categories = [1, 2, 3, 4, 5, 6]

    result_rows = []

    for group, fields in group_fields.items():
        group_df = df[df[fields].isin([1, "1", True]).any(axis=1)].copy()
        if group_df.empty:
            continue

        #print(f"Processing ASSISTANCE QUALITY for group '{group}' ({len(group_df)} records)...")

        weighted_counts = {cat: 0.0 for cat in valid_categories}
        total_weight = 0.0

        for _, row in group_df.iterrows():
            try:
                w = float(row.get("weight_final", 0))
                val = int(row.get("assistance_quality"))
                if w > 0 and val in weighted_counts:
                    weighted_counts[val] += w
                    total_weight += w
            except:
                continue

        if total_weight == 0:
            continue

        row = {
            f"{group_label} group": group.replace("need_received_", "").replace("need_", "").replace("_", " ").capitalize()
        }
        for cat in valid_categories:
            label = assistance_categories[cat]
            row[label] = round((weighted_counts[cat] / total_weight) * 100, 1)

        result_rows.append(row)

    index_name = f"{group_label} group"
    df_result = pd.DataFrame(result_rows).set_index(index_name).round(1)

    sample_size = df["assistance_quality"].dropna().shape[0]
    warning = " — Warning: small sample size" if sample_size < 100 else ""
    title = f"Satisfaction with assistance by group of assistance received (sample for question on satisfaction = {sample_size}){warning}"

    return {
        "title": title,
        "df": df_result
    }


def extract_top10_by_cropland(country_iso3, file_name="IPC_multicountry_20250707.xlsx"):
    """
    Loads the IPC Excel file, extracts the sheet for the given country (ISO3),
    and returns a dictionary with a title and the top 10 adm2 units
    with the highest cropland exposed to floods (rounded to 1 decimal).
    Unnecessary fields are removed and adm2 fields are listed before adm1.
    If the sheet is missing, empty, or an error occurs, 'SKIPPED' is included in the title.
    """
    base_title = "List of adm2 units in IPC3+ with the highest amount of cropland exposed to floods"

    columns_to_keep = [
        "adm0_name", "adm0_ISO3", "adm1_name", "adm1_pcode", "adm2_name", "adm2_pcode",
        "cropland_exp_sqkm", "cropland_tot_sqkm", "crop_exp_perc",
        "pop_exposed", "pop_total", "pop_exp_perc",
        "area_phase_current", "analysis_period_current",
        "area_phase_proj1", "analysis_period_proj1",
        "area_phase_proj2", "analysis_period_proj2"  # <-- ADD THESE
    ]

    rename_dict = {
        "cropland_exp_sqkm": "Cropland exposed (Km2)",
        "cropland_tot_sqkm": "Total cropland (Km2)",
        "crop_exp_perc": "% of cropland exposed",
        "pop_exposed": "Population exposed",
        "pop_total": "Total population",
        "pop_exp_perc": "% of population exposed",
        "area_phase_current": "IPC/CH area current",
        "analysis_period_current": "Period of current IPC analysis",
        "area_phase_proj1": "IPC/CH area projected",
        "analysis_period_proj1": "Period of projected IPC analysis",
        "area_phase_proj2": "IPC/CH area projected 2nd",  # <-- ADD
        "analysis_period_proj2": "Period of 2nd projected IPC analysis"  # <-- ADD
    }

    try:
        file_path = os.path.join(os.path.dirname(__file__), file_name)
        df = pd.read_excel(file_path, sheet_name=country_iso3)

        if df.empty:
            print(f"Sheet '{country_iso3}' exists but is empty.")
            return {
                "title": f"{base_title} — SKIPPED: Sheet '{country_iso3}' is empty",
                "df": pd.DataFrame()
            }

        # Keep only the columns that exist in the sheet
        available_columns = [col for col in columns_to_keep if col in df.columns]
        df_filtered = df[available_columns].rename(columns=rename_dict)

        # Drop area_phase_proj2 and analysis_period_proj2 if they are entirely empty
        for optional_field in ["IPC/CH area projected 2nd", "Period of 2nd projected IPC analysis"]:
            if optional_field in df_filtered.columns and df_filtered[optional_field].isna().all():
                df_filtered = df_filtered.drop(columns=optional_field)

        df_filtered = df_filtered.round(1)

        # Apply filtering only if any IPC area field is available and not all null
        filter_applied = False
        for col in [ "IPC/CH area projected 2nd", "IPC/CH area projected", "IPC/CH area current"]:
            if col in df_filtered.columns and df_filtered[col].notna().any():
                df_filtered = df_filtered[df_filtered[col] >= 3]
                filter_applied = True
                break  # Stop after applying the first valid filter


        # Drop unnecessary fields
        df_filtered = df_filtered.drop(columns=["adm0_name", "adm0_ISO3"], errors="ignore")

        # Reorder adm2 fields before adm1
        desired_order = [
            "adm2_name", "adm2_pcode",
            "adm1_name"
        ] + [col for col in df_filtered.columns if col not in ["adm1_name", "adm1_pcode", "adm2_name", "adm2_pcode"]]
        df_filtered = df_filtered[desired_order]

        df_top10 = df_filtered.sort_values(by="Cropland exposed (Km2)", ascending=False).head(10)

        return {
            "title": base_title,
            "df": df_top10
        }

    except FileNotFoundError:
        msg = f"{base_title} — SKIPPED: {adm0_iso3} File '{file_name}' not found"
        print(msg)
    except ValueError:
        msg = f"{base_title} — SKIPPED: {adm0_iso3} Sheet '{country_iso3}' not found"
        print(msg)
    except Exception as e:
        msg = f"{base_title} — SKIPPED: {adm0_iso3} Error processing sheet '{country_iso3}': {e}"
        failed_icp_floods.append(msg)
        print(msg)

    return {
        "title": msg,
        "df": pd.DataFrame()
    }

def residency_sample_size_summary(df):
    """
    Returns a clean 2-column DataFrame showing the sample size by residency group.
    """
    # Load official residency labels from metadata
    residency_labels = get_indicator_info('hh_residencetype').get("codes", {})

    # Ensure hh_residencetype is numeric
    df = df.copy()
    df["hh_residencetype"] = pd.to_numeric(df["hh_residencetype"], errors="coerce").astype("Int64")

    rows = []
    total = 0

    for code_str, label in residency_labels.items():
        if label in ["Don't know", "Refused"]:
            continue
        try:
            code = int(code_str)
        except ValueError:
            continue

        count = df[df["hh_residencetype"] == code].shape[0]
        rows.append({
            "Residency group": label,
            "Sample size": count
        })
        total += count

    # Print summary in console
    # print("Sample size by residency status – Total:", total)
    # for row in rows:
    #     print(f"{row['Residency group']}: {row['Sample size']}")

    return {
        "title": "Sample size by residency status. The next three tables and charts will be based on the residency status of the household. In some cases, specific groups may be underrepresented; therefore, the analysis outcomes should be interpreted with caution.",
        "metadata": f"Total sample size: {total}",
        "df": pd.DataFrame(rows)
    }

import pandas as pd
import requests

def get_top7_shocks_by_country(adm0_iso3: str) -> pd.DataFrame:
    indicators = [
        "shock_noshock", "shock_sicknessordeathofhh", "shock_lostemplorwork", "shock_otherintrahhshock",
        "shock_higherfoodprices", "shock_higherfuelprices", "shock_mvtrestrict", "shock_othereconomicshock",
        "shock_pestoutbreak", "shock_plantdisease", "shock_animaldisease", "shock_napasture",
        "shock_othercropandlivests", "shock_coldtemporhail", "shock_flood", "shock_hurricane",
        "shock_drought", "shock_earthquake", "shock_landslides", "shock_firenatural",
        "shock_othernathazard", "shock_violenceinsecconf", "shock_theftofprodassets",
        "shock_firemanmade", "shock_othermanmadehazard", "shock_dk", "shock_ref",
        "fies_mod_sev", "fies_sev"
    ]

    service_url = "https://services5.arcgis.com/sjP4Ugu5s0dZWLjd/ArcGIS/rest/services/diem_trend_adm0/FeatureServer/66/query"

    where_clause = f"(adm0_iso3 = '{adm0_iso3}') AND indicator IN ('" + "','".join(indicators) + "')"
    params = {
        "where": where_clause,
        "outFields": "adm0_iso3,indicator,value,round,coll_end_date",
        "returnGeometry": "false",
        "outSR": "4326",
        "f": "json",
        "resultRecordCount": "5000"
    }

    response = requests.get(service_url, params=params)
    response.raise_for_status()
    data = response.json()
    records = [f["attributes"] for f in data.get("features", [])]

    df = pd.DataFrame(records)
    df["coll_end_date"] = pd.to_datetime(df["coll_end_date"], unit="ms", errors="coerce")

    shock_df = df[~df["indicator"].isin(["fies_mod_sev", "fies_sev"])]
    top_shocks = (
        shock_df.groupby("indicator")
        .apply(lambda g: g.loc[g["value"].idxmax()])
        .sort_values("value", ascending=False)
        .head(7)
        .indicator
        .tolist()
    )

    top_indicators = top_shocks + ["fies_mod_sev", "fies_sev"]
    result_df = df[df["indicator"].isin(top_indicators)].copy()
    result_df.sort_values(["indicator", "coll_end_date"], inplace=True)
    return result_df


# === LOAD CSV ===

# csv_path = r"C:\git\crossview_processing\DIEM_micro20250703_CODR89.csv"
csv_path = r"C:\git\crossview_processing\DIEM_micro20250703.csv"

df_all = pd.read_csv(csv_path)
failed_icp_floods = []
# === Main loop ===
for survey in survey_list:
    adm0_iso3 = survey["adm0_iso3"]
    adm0_name = survey["adm0_name"]
    round_num = survey["round_num"]
    coll_end_date = survey["coll_end_date"]
    print("Processing %s R%s" % (adm0_iso3, round_num))
# for survey in survey_list:
#     adm0_iso3 = survey["adm0_iso3"]
#     round_num = survey["round_num"]
    df = df_all[(df_all["adm0_iso3"] == adm0_iso3) & (df_all["round"] == round_num)]
    # print(f"Fetched {len(df)} records for {adm0_iso3} round {round_num}.")



    # === MAIN EXECUTION ===
    result_dfs = []

    # #FIES by agricultural dependancy simplified
    print('FIES by agricultural dependancy simplified')
    result_dfs.append(fies_by_simplified_agriculture(df))
    # #FIES by agricultural dependancy
    print('FIES by agricultural dependancy')
    result_dfs.append(fies_by_indicator("fies_hhtype", df))
    if "hh_residencetype" in df.columns and df["hh_residencetype"].dropna().any():
        result_dfs.append(residency_sample_size_summary(df))
        ##FIES by residency status
        print('FIES by residency status')
        result_dfs.append(fies_by_indicator("fies_resid", df))
        # #agricultural dependancy simplified by residency status
        print('agricultural dependancy simplified by residency status')
        result_dfs.append(simplified_dependency_by_residency(df))
        # #agricultural dependancy by residency status
        print('agricultural dependancy by residency status')
        result_dfs.append(agricultural_dependency(df))
    else:
        msg = "This survey did not contain a question on residency status (hh_residencetype), so residency-based analysis was skipped."
        #print(msg)
        result_dfs.append({
            "title": "Residency-based analysis",
            "metadata": None,
            "df": pd.DataFrame({"Message": [msg]})
        })
    # # Needs summary grouped, using previous round, and custom universe
    if adm0_iso3 in ['PSE']:
        print ("WARNING: Skipping Needs Analysis for PSE since a non standard questionnaire was used")
    else:
        print('Needs summary grouped, using previous round')
        needs_res = needs_summary_grouped(df_all, adm0_iso3, round_num,use_grouping=True, use_previous_round=True,universe_filter=[0, 1, 888])
        # # Assistance summary (grouped)
        print('Assistance summary (grouped)')
        assistance_res = assistance_summary(    df, use_grouping=True,universe_filter=[0, 1, 888],round_num=round_num,adm0_iso3=adm0_iso3)
        result_dfs.append(needs_res)
        result_dfs.append(assistance_res)
        # Append comparison
        print('Comparison previous needs VS assistance')
        result_dfs.append(compare_needs_vs_assistance(needs_res, assistance_res))
        # Grouped by received assistance type
        if "assistance_quality" in df.columns and df["assistance_quality"].dropna().any():
            print('Assistance quality')
            result_dfs.append(assistance_quality_summary(df, group_by="need_received"))
        else:
            msg = "This survey did not contain a question on quality of assistance, so analysis on assistance satisfaction was skipped."
            # print(msg)
            result_dfs.append({
                "title": "Satisfaction with assistance by group of assistance received",
                "metadata": None,
                "df": pd.DataFrame({"Message": [msg]})
            })


    try:
        print('IPC and flood exposure')
        result_dfs.append(extract_top10_by_cropland(adm0_iso3))
    except:
        print ('FAILED IPC TABLE INSERTION')
        failed_icp_floods.append(adm0_iso3)


    # === Add top shocks + FIES trends (time series)
    try:
        print("Top 7 shock trends and food insecurity (FIES)")
        top_shocks_df = get_top7_shocks_by_country(adm0_iso3)
        result_dfs.append({
            "title": "Top 7 shocks and food insecurity trends (national level)",
            "metadata": None,
            "df": top_shocks_df
        })
    except Exception as e:
        print(f"❌ Failed to extract shock trends: {e}")

    # Print results
    # for res in result_dfs:
    #     # print(f"\n=== {res['title']} ===")
    #     if "metadata" in res:
    #         print(res["metadata"])
    #     print(res["df"])

    if export_csv:
        from openpyxl import Workbook
        from openpyxl.utils.dataframe import dataframe_to_rows
        from openpyxl.chart import BarChart, Reference
        from openpyxl.styles import Font, Border, Side
        from openpyxl.utils import get_column_letter

        wb = Workbook()
        ws = wb.active
        ws.title = "DIEM Surveys analysis"

        # Set wider column widths for A–E
        for col in ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J","K","L","M",'N']:
            ws.column_dimensions[col].width = 24

        current_row = 1
        label_max_len = 30   # Truncate chart labels beyond this length

        thin_border = Border(
            left=Side(style="thin"),
            right=Side(style="thin"),
            top=Side(style="thin"),
            bottom=Side(style="thin")
        )

        def truncate_label(label, max_len):
            return label if len(label) <= max_len else label[:max_len - 3] + "..."

        # Add main title
        coll_end_date_raw = survey.get("coll_end_date", "")
        try:
            coll_end_date = pd.to_datetime(coll_end_date_raw).strftime("%Y-%m-%d")
        except:
            coll_end_date = coll_end_date_raw  # fallback in case parsing fails

        title_string = f"DIEM surveys analysis for ERPs – {adm0_name} – Round {round_num} – Data collection ended on {coll_end_date}"
        ws.cell(row=current_row, column=1, value=title_string)
        ws.cell(row=current_row, column=1).font = Font(size=14, bold=True)
        current_row += 2  # leave a blank row after main title

        for i, result in enumerate(result_dfs):
            if i > 0:
                current_row += 15  # space between tables
            #deals with a special case of a missing flood exposure
            if 'is empty' in result["title"].lower():
                continue #we are dealing with a country with no flood exposure data
            # Title and metadata
            if "title" in result:
                ws.cell(row=current_row, column=1, value=result["title"])
                ws.cell(row=current_row, column=1).font = Font(bold=True)
                current_row += 1
            if "metadata" in result:
                ws.cell(row=current_row, column=1, value=result["metadata"])
                current_row += 1

            df = result["df"].reset_index()

            # === Special case: shock + FIES trends (time series) line chart
            if result["title"].lower().startswith("top 7 shocks and food insecurity"):
                # Create a pivot: rows = date, columns = indicator, values = value
                df_pivot = df.pivot_table(index="coll_end_date", columns="indicator", values="value",
                                          aggfunc="mean").reset_index()
                df_pivot = df_pivot.sort_values("coll_end_date")

                # Remove 'fies_sev' from both table and chart
                if "fies_sev" in df_pivot.columns:
                    df_pivot = df_pivot.drop(columns=["fies_sev"])

                # Skip writing the table to the Excel sheet (only chart)

                # Define reference range for line chart
                min_col = 2  # first indicator (after date)
                max_col = 1 + len(df_pivot.columns) - 1
                num_data_rows = df_pivot.shape[0]
                date_col = 1

                # Write the pivot to sheet temporarily, only for chart data (in a hidden area if needed)
                temp_row_start = current_row
                for row in dataframe_to_rows(df_pivot, index=False, header=True):
                    for col_idx, val in enumerate(row, 1):
                        ws.cell(row=current_row, column=col_idx, value=val)
                    current_row += 1

                from openpyxl.chart import LineChart

                data = Reference(ws, min_col=min_col, min_row=temp_row_start,
                                 max_col=max_col, max_row=current_row - 1)

                categories = Reference(ws, min_col=date_col, min_row=temp_row_start + 2,
                                       max_row=current_row - 1)

                line_chart = LineChart()
                line_chart.title = "Most frequent 7 Shock and food insecurity trends (p_mod)"
                line_chart.y_axis.title = "Value"
                line_chart.x_axis.title = "Collection date"
                line_chart.x_axis.number_format = "mmm yyyy"
                line_chart.width = 24  # larger width
                line_chart.height = 14  # larger height
                line_chart.add_data(data, titles_from_data=True)
                line_chart.set_categories(categories)
                line_chart.legend.position = "b"


                # Match series by their column order (excluding 'coll_end_date')
                for idx, series in enumerate(line_chart.series):
                    series.smooth = True
                    col_name = df_pivot.columns[idx + 1]  # +1 to skip 'coll_end_date'
                    if col_name == "fies_mod_sev":
                        series.graphicalProperties.line.dashStyle = "sysDot"

                chart_row = current_row + 2
                chart_position = f"B{chart_row}"
                ws.add_chart(line_chart, chart_position)
                current_row = chart_row + 20
                continue  # skip default logic for this result

            # === Handle special cases:
            # skip chart and optionally skip full table rendering
            contains_chart = True
            if (
                    df.shape[1] == 3 and df.columns.tolist() == ['index', 'Residency group', 'Sample size']
                    and "sample size by residency status" in result["title"].lower()
            ):
                # Write the table as normal
                table_cols = list(df.columns)
                for row_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True)):
                    for col_idx, val in enumerate(row, 1):
                        cell = ws.cell(row=current_row, column=col_idx, value=val)
                        cell.border = thin_border
                    current_row += 1

                # Do NOT generate chart for this table
                contains_chart = False
                continue
            # === Legacy case: single-row skipped messages
            if df.shape == (1, 2) and (
                    "skipped" in str(df.iloc[0, 1]).lower()
            ):
                message_text = str(df.iloc[0, 1])
                ws.cell(row=current_row, column=1, value=message_text)
                current_row += 2
                contains_chart = False
                continue

            # Create truncated label column for chart X-axis
            chart_labels_col = df.columns[0] + "_short"
            df[chart_labels_col] = df.iloc[:, 0].apply(lambda x: truncate_label(str(x), label_max_len))

            start_row = current_row

            # Write table (full-length labels)
            table_cols = list(df.columns[:-1])  # exclude chart_labels_col
            for row_idx, row in enumerate(dataframe_to_rows(df[table_cols], index=False, header=True)):
                for col_idx, val in enumerate(row, 1):
                    cell = ws.cell(row=current_row, column=col_idx, value=val)
                    cell.border = thin_border
                current_row += 1

            # Write truncated labels far right in column AZ (col 52)
            chart_label_col_idx = 52
            ws.cell(row=start_row, column=chart_label_col_idx, value="Chart labels")
            for r in range(df.shape[0]):
                label_val = df.iloc[r, df.columns.get_loc(chart_labels_col)]
                ws.cell(row=start_row + 1 + r, column=chart_label_col_idx, value=label_val)

            # Create chart
            num_cols = len(table_cols)
            num_rows = df.shape[0]
            if num_cols >= 2:

                # === Custom chart for flood exposure table (cropland + population)
                # === Custom chart for flood exposure table (cropland only, sorted)
                if result["title"].startswith(
                        "List of adm2 units in IPC3+ with the highest amount of cropland exposed to floods"):
                    try:
                        crop_col_idx = table_cols.index("Cropland exposed (Km2)")
                        adm2_name_idx = table_cols.index("adm2_name")
                    except ValueError as e:
                        print(f"Missing expected column for flood chart: {e}")
                        continue

                    # Extract data from existing table
                    data_rows = []
                    for i in range(num_rows):
                        row_idx = start_row + 1 + i  # Skip header
                        adm2 = ws.cell(row=row_idx, column=adm2_name_idx + 1).value
                        crop_val = ws.cell(row=row_idx, column=crop_col_idx + 1).value
                        data_rows.append((row_idx, adm2, crop_val))

                    # Sort rows by cropland exposed descending
                    sorted_rows = sorted(data_rows, key=lambda x: (x[2] if x[2] is not None else 0), reverse=True)

                    # Prepare references to the sorted row indexes
                    sorted_row_indices = [r[0] for r in sorted_rows]

                    # Create chart data and category references using sorted order
                    categories = Reference(ws, min_col=adm2_name_idx + 1,
                                           min_row=sorted_row_indices[0],
                                           max_row=sorted_row_indices[-1])
                    crop_data = Reference(ws, min_col=crop_col_idx + 1,
                                          min_row=sorted_row_indices[0] - 1,  # include header
                                          max_row=sorted_row_indices[-1])

                    # Create chart
                    chart_crop = BarChart()
                    chart_crop.type = "col"
                    chart_crop.grouping = "clustered"
                    chart_crop.title = "Flood exposure: cropland only (top 10 adm2 in IPC3+)"
                    chart_crop.x_axis.title = "Admin2 unit"
                    chart_crop.y_axis.title = "Cropland exposed (Km²)"
                    chart_crop.y_axis.majorGridlines = None
                    chart_crop.width = 18
                    chart_crop.height = 9

                    chart_crop.add_data(crop_data, titles_from_data=True)
                    chart_crop.set_categories(categories)

                    # Insert into sheet
                    chart_row = current_row + 2
                    chart_position = f"B{chart_row}"
                    ws.add_chart(chart_crop, chart_position)

                    # Advance row counter
                    current_row = chart_row + 20



                # === Default chart for all other tables
                else:
                    chart = BarChart()
                    chart.type = "col"
                    chart.title = "Chart: " + result["title"]
                    chart.y_axis.title = "Percentage"
                    chart.x_axis.title = df.columns[0]
                    chart.width = 18
                    chart.height = 9

                    data = Reference(ws, min_col=2, min_row=start_row,
                                     max_col=num_cols, max_row=start_row + num_rows)
                    categories = Reference(ws, min_col=chart_label_col_idx,
                                           min_row=start_row + 1, max_row=start_row + num_rows)

                    chart.add_data(data, titles_from_data=True)
                    chart.set_categories(categories)

                    chart_row = current_row + 2
                    chart_position = f"B{chart_row}"

                    ws.add_chart(chart, chart_position)
                    if contains_chart:
                        current_row = chart_row + 7
                    else:
                        current_row = chart_row - 10

        # Create the output directory if it does not exist
        output_dir = "outputs_for_erps"
        os.makedirs(output_dir, exist_ok=True)

        # Define path inside the subdirectory
        output_path = os.path.join(output_dir, f"DIEM_survey_analysis_ERPs_202507_{adm0_iso3}_{round_num}.xlsx")
        wb.save(output_path)
        print(f"\nExported grouped analysis with adaptive chart layout to: {output_path}")

print('Failed IPC and flood exposure for %s' % failed_icp_floods)