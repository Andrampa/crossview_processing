from arcgis.gis import GIS
from arcgis.features import FeatureLayer
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import shape
import pandas as pd
import os
from matplotlib.patches import Patch
from datetime import datetime


# === Functions to check coverage if not national ===
def is_meaningfully_covered(ipc_geom, diem_geoms):
    for diem_geom in diem_geoms:
        if ipc_geom.intersects(diem_geom):
            try:
                intersection_area = ipc_geom.intersection(diem_geom).area
                if intersection_area / ipc_geom.area >= 0.15:
                    return True
            except:
                try:
                    fixed_ipc = ipc_geom.buffer(0)
                    fixed_diem = diem_geom.buffer(0)
                    if fixed_ipc.intersects(fixed_diem):
                        intersection_area = fixed_ipc.intersection(fixed_diem).area
                        if intersection_area / fixed_ipc.area >= 0.15:
                            return True
                except:
                    continue
    return False

def count_covered_geoms(gdf, diem_geoms):
    ipc3plus = gdf[gdf["phase"] >= 3]
    n_total = ipc3plus.shape[0]
    n_covered = ipc3plus.geometry.apply(lambda g: is_meaningfully_covered(g, diem_geoms)).sum()
    return n_total, n_covered

def count_covered(phase_col):
    df = ipc_gdf[[phase_col, "geometry"]].dropna(subset=[phase_col])
    df = df.rename(columns={phase_col: "phase"}).to_crs(main_gdf.crs)
    return count_covered_geoms(df, main_gdf.geometry)

# Define output directory
output_dir = os.path.join(os.getcwd(), "outputs_for_erps", "coverage_maps")
os.makedirs(output_dir, exist_ok=True)  # create directory if it does not exist

# Connect to ArcGIS Online
gis = GIS()

##dict below is done by using variable survey_list in line 93 of script perform_crosstabs_ERPs
# and then applying this command in console survey_pairs = [[s['adm0_iso3'], s['round_num'], s['diem_survey_coverage'], s['adm0_name'], s['diem_target_pop']] for s in survey_list]
survey_details = [['AFG',  10,  'All 34 Province are covered',  'Afghanistan',  'Target: Rural population'], ['BFA',  3,  '29 Provinces covered out of 45',  'Burkina Faso',
  'Target: Rural population'], ['BGD',  12,  'All 64 District are covered',  'Bangladesh',  'Target: Entire population'], ['CAF',  6,  '23 Sous-Préfecture covered out of 72',  'Central African Republic',
  'Target: Entire population'], ['CMR',  7,  'All 10 Adamawa, Centre, East, Far North, Littoral, North, North-West, South, South-West, West are covered',
  'Cameroon',  'Target: Entire population'], ['COD',  9,  '5 Provinces covered out of 26',  'Democratic Republic of the Congo',  'Target: Entire population'],
 ['COL',  6,  '13 Department covered out of 32',  'Colombia',  'Target: Rural population'], ['GTM',  4,  'All 22 provinces are covered',  'Guatemala',  'Target: under review'],
 ['HND',  4,  'All 18 Department are covered',  'Honduras',  'Target: Entire population'], ['HTI',  6,  'All 10 Departement are covered',  'Haiti',  'Target: Entire population'],
 ['IRQ',  13,  'All 18 Governorate are covered',  'Iraq',  'Target: Entire population'], ['LBN',  8,  '25 Districts covered out of 26',  'Lebanon',  'Target: Agricultural population'],
 ['MLI', 9, '8 Region covered out of 10', 'Mali', 'Target: Rural population'], ['MMR',  11,  '13 State/Region covered out of 15',  'Myanmar',  'Target: Entire population'],
 ['MOZ',  7,  '9 Provinces covered out of 11',  'Mozambique',  'Target: Rural population'], ['MWI',  1,  '28 Districts covered out of 32',  'Malawi',  'Target: Agricultural population'],
['NER',  10,  '7 Regions covered out of 8',  'Niger',  'Target: Rural population'], ['NGA',  8,  '3 States covered out of 37',  'Nigeria',  'Target: Rural population'],
 ['PAK',  6,  '25 District covered out of 161',  'Pakistan',  'Target: Rural population'], ['PSE',  1,  '9 Governorates covered out of 14',  'Palestine',  'Target: Agricultural population'],
 ['TCD',  8,  '19 Departement covered out of 70',  'Chad',  'Target: Rural population'], ['YEM',  28,  'All 22 Governorates are covered',  'Yemen',  'Target: Entire population'],
 ['ZWE',  10,  '8 Province covered out of 10',  'Zimbabwe',  'Target: Rural population']]

countries_ipc_json = ["AFG", "BGD", "CAF", "COD", "SLV", "HTI", "LBN", "MOZ", "YEM", "PAK", "MWI", "ZWE"]
countries_excel_only = ["CMR", "TCD", "MLI", "NER", "GTM", "NGA", "HND", "SLV"]
countries_ipc_adm1 = ["GTM", "HND", "SLV"]
countries_none = ["COL", "MMR", "PSE", "IRQ", "BFA"]

for survey_detail in survey_details:
    adm0_iso3, round, diem_survey_coverage, adm0_name, diem_target_pop = survey_detail
    if adm0_iso3 in ["COL", "MMR", "PSE", "IRQ", "BFA"]:
        continue
    print('Creating IPC/CH maps for %s R%s' % (adm0_iso3, round))
    if adm0_iso3 in countries_ipc_json:
        try:
            main_layer = FeatureLayer("https://services5.arcgis.com/sjP4Ugu5s0dZWLjd/arcgis/rest/services/diem_adm_repr_1_mview/FeatureServer/29")
            main_result = main_layer.query(where=f"adm0_iso3 = '{adm0_iso3}' AND round = {round} AND surveys > 2",
                                           out_fields='*', return_geometry=True)

            main_records, coll_end_date = [], ""
            for feat in main_result.features:
                geom = feat.geometry
                if geom and "rings" in geom:
                    geojson_geom = {"type": "Polygon", "coordinates": geom["rings"]}
                    attr = feat.attributes
                    attr["geometry"] = shape(geojson_geom)
                    if not coll_end_date and "coll_end_date" in attr and attr["coll_end_date"]:
                        coll_end_date = datetime.utcfromtimestamp(attr["coll_end_date"] / 1000).strftime("%b %Y")
                    main_records.append(attr)
            if not main_records:
                raise ValueError("No valid features in main layer.")
            main_gdf = gpd.GeoDataFrame(main_records, geometry="geometry", crs="EPSG:4326")

            bkg_layer = FeatureLayer("https://services5.arcgis.com/sjP4Ugu5s0dZWLjd/arcgis/rest/services/Administrative_Boundaries_Reference_(view_layer)/FeatureServer/2")
            bkg_result = bkg_layer.query(where=f"adm0_iso3 = '{adm0_iso3}'", out_fields='*', return_geometry=True)
            bkg_records = []
            for feat in bkg_result.features:
                geom = feat.geometry
                if geom and "rings" in geom:
                    geojson_geom = {"type": "Polygon", "coordinates": geom["rings"]}
                    attr = feat.attributes
                    attr["geometry"] = shape(geojson_geom)
                    bkg_records.append(attr)
            if not bkg_records:
                raise ValueError("No valid background features.")
            bkg_gdf = gpd.GeoDataFrame(bkg_records, geometry="geometry", crs="EPSG:4326")

            ipc_path = os.path.join(
                r"C:\Users\Amparore\OneDrive - Food and Agriculture Organization\Needs Assessments\01. DIEM Monitoring\IPC_files",
                next(f for f in os.listdir(r"C:\Users\Amparore\OneDrive - Food and Agriculture Organization\Needs Assessments\01. DIEM Monitoring\IPC_files")
                     if f.startswith(adm0_iso3) and f.endswith(".json")))
            ipc_gdf = gpd.read_file(ipc_path)
            ipc_gdf = ipc_gdf[ipc_gdf.geometry.type.isin(["Polygon", "MultiPolygon"])]
            if ipc_gdf.empty:
                raise ValueError("Empty IPC GeoJSON.")


            has_current = "overall_phase_C" in ipc_gdf.columns and ipc_gdf["overall_phase_C"].notna().any()
            has_projection = "overall_phase_P" in ipc_gdf.columns and ipc_gdf["overall_phase_P"].notna().any()
            has_second_projection = "overall_phase_A" in ipc_gdf.columns and ipc_gdf["overall_phase_A"].notna().any()
            ncols = int(has_current) + int(has_projection) + int(has_second_projection)
            if ncols == 0:
                raise ValueError("No phase info available.")
            fig, axes = plt.subplots(1, ncols, figsize=(10 * ncols, 10))
            if ncols == 1:
                axes = [axes]

            # Add overall title on top
            #fig.suptitle(f"{adm0_name}: DIEM – IPC coverages", fontsize=18, fontweight='bold', ha='center', y=0.98)
            fig.suptitle(f"{adm0_name} – DIEM survey and IPC/CH phase overlap", fontsize=17, fontweight='bold',
                         ha='center', y=0.98)

            ax_idx = 0

            ipc_phase_styles = {
                1: {"color": (205/255, 250/255, 205/255), "label": "Phase 1 (Minimal/None)"},
                2: {"color": (250/255, 230/255, 30/255), "label": "Phase 2 (Alert/Stressed)"},
                3: {"color": (230/255, 120/255, 0/255), "label": "Phase 3 (Serious/Crisis)"},
                4: {"color": (200/255, 0/255, 0/255), "label": "Phase 4 (Critical/Emergency)"},
                5: {"color": (100/255, 0/255, 0/255), "label": "Phase 5 (Extremely Critical/Famine)"}
            }

            def plot_phase(ax, column, title):
                bkg_gdf.plot(ax=ax, edgecolor='black', facecolor='none', linewidth=0.5)
                for phase_value, style in ipc_phase_styles.items():
                    subset = ipc_gdf[ipc_gdf[column] == phase_value]
                    if not subset.empty:
                        subset.plot(ax=ax, facecolor=style["color"], edgecolor='black',
                                    linewidth=0.5, alpha=0.6, label=style["label"])
                main_gdf.plot(ax=ax, facecolor='none', edgecolor='red', hatch='//', linewidth=0, alpha=0.9)
                ax.set_title(title, fontsize=13)
                ax.set_axis_off()


            if has_current:
                start = ipc_gdf['current_from_date'].dropna().iloc[0] if 'current_from_date' in ipc_gdf else '?'
                end = ipc_gdf['current_thru_date'].dropna().iloc[0] if 'current_thru_date' in ipc_gdf else '?'
                period = f"{pd.to_datetime(start).strftime('%b %Y')} to {pd.to_datetime(end).strftime('%b %Y')}" if start != '?' and end != '?' else '?'
                subtitle = f"DIEM coverage in Round {round} ({coll_end_date}) \nand IPC acute food insecurity phases – Current situation ({period})"
                plot_phase(axes[ax_idx], "overall_phase_C", subtitle)
                ax_idx += 1

            if has_projection:
                start = ipc_gdf['projected_from_date'].dropna().iloc[0] if 'projected_from_date' in ipc_gdf else '?'
                end = ipc_gdf['projected_thru_date'].dropna().iloc[0] if 'projected_thru_date' in ipc_gdf else '?'
                period = f"{pd.to_datetime(start).strftime('%b %Y')} to {pd.to_datetime(end).strftime('%b %Y')}" if start != '?' and end != '?' else '?'
                subtitle = f"DIEM coverage in Round {round} ({coll_end_date}) \nand IPC acute food insecurity phases – First projection ({period})"
                plot_phase(axes[ax_idx], "overall_phase_P", subtitle)
                ax_idx += 1

            if has_second_projection:
                start = ipc_gdf['second_projected_from_date'].dropna().iloc[
                    0] if 'second_projected_from_date' in ipc_gdf else '?'
                end = ipc_gdf['second_projected_thru_date'].dropna().iloc[
                    0] if 'second_projected_thru_date' in ipc_gdf else '?'
                period = f"{pd.to_datetime(start).strftime('%b %Y')} to {pd.to_datetime(end).strftime('%b %Y')}" if start != '?' and end != '?' else '?'
                subtitle = f"DIEM coverage in Round {round} ({coll_end_date}) \nand IPC acute food insecurity phases – Second projection ({period})"
                plot_phase(axes[ax_idx], "overall_phase_A", subtitle)


            annotation_lines = [f"DIEM Survey R{round} coverage: {diem_survey_coverage}"]
            annotation_lines.append(f"DIEM Survey R{round} target population: {diem_target_pop}")

            if "all" not in diem_survey_coverage.lower():

                if has_current:
                    n_current, n_cov_current = count_covered("overall_phase_C")
                    annotation_lines.append(f"Current: Number of admin units in IPC3+: {n_current} (of which covered by DIEM: {n_cov_current})")

                if has_projection:
                    n_proj, n_cov_proj = count_covered("overall_phase_P")
                    annotation_lines.append(f"IPC first proj.: Number in IPC3+: {n_proj} (of which covered by DIEM: {n_cov_proj})")

                if has_second_projection:
                    n_2nd, n_cov_2nd = count_covered("overall_phase_A")
                    annotation_lines.append(f"Number in IPC3+ (2nd proj.): {n_2nd} (of which covered by DIEM: {n_cov_2nd})")

            annotation_lines.append("")
            annotation_lines.append("Note: figures are indicative, as DIEM and IPC/CH may use different administrative references or levels.")
            annotation_lines.append("Maps and figures above automatically derived. For authoritative information, please refer to official DIEM and IPC/CH documentation.")

            annotation_text = "\n".join(annotation_lines)

            legend_elements = [Patch(facecolor=style["color"], edgecolor='black', label=style["label"], alpha=0.6)
                               for style in ipc_phase_styles.values()]
            legend_elements.append(Patch(facecolor='none', edgecolor='crimson',
                                         label=f'DIEM Monitoring Survey Round {round} coverage',
                                         hatch='//', linewidth=1))

            fig.subplots_adjust(bottom=0.25)
            bottom_ax = fig.add_axes([0.1, 0.01, 0.8, 0.12])
            bottom_ax.axis("off")
            bottom_ax.legend(handles=legend_elements, loc='upper center', ncol=3, fontsize='medium', frameon=False,
                             bbox_to_anchor=(0.5, 1.0), handlelength=2.5, handleheight=1.5, borderpad=1.0, labelspacing=0.8)
            bottom_ax.text(0.0, 0.25, annotation_text, ha='left', va='top', fontsize=12, transform=bottom_ax.transAxes)

            output_file = os.path.join(output_dir, f"map_{adm0_iso3.lower()}_round{round}_diem_ipc.png")
            plt.savefig(output_file, bbox_inches='tight')
            plt.close()
            print(f"✅ Map saved to: {output_file}")

        except Exception as e:
            import traceback
            tb = traceback.extract_tb(e.__traceback__)[-1]
            print(f"❌ Failed for {adm0_iso3} at line {tb.lineno} in your script")
            print(f"   ➤ Code: {tb.line}")
            print(f"   ➤ Error: {type(e).__name__} - {e}")
    elif adm0_iso3 in countries_excel_only:
        try:
            print(f"Processing IPC data from Excel for {adm0_iso3} R{round}")

            # === Load IPC data from Excel ===
            ipc_excel_file = "IPC_multicountry_20250707.xlsx"
            ipc_excel_path = os.path.join(os.path.dirname(__file__), ipc_excel_file)
            ipc_df = pd.read_excel(ipc_excel_path, sheet_name=adm0_iso3)

            phase_columns = [
                "area_phase_proj2",
                "area_phase_proj1",
                "area_phase_current"
            ]

            period_fields = {
                "area_phase_proj2": "analysis_period_proj2",
                "area_phase_proj1": "analysis_period_proj1",
                "area_phase_current": "analysis_period_current"
            }

            ipc_phase_styles = {
                1: {"color": (205/255, 250/255, 205/255), "label": "Phase 1 (Minimal/None)"},
                2: {"color": (250/255, 230/255, 30/255), "label": "Phase 2 (Alert/Stressed)"},
                3: {"color": (230/255, 120/255, 0/255), "label": "Phase 3 (Serious/Crisis)"},
                4: {"color": (200/255, 0/255, 0/255), "label": "Phase 4 (Critical/Emergency)"},
                5: {"color": (100/255, 0/255, 0/255), "label": "Phase 5 (Extremely Critical/Famine)"}
            }

            # === Load background admin2 layer ===
            if adm0_iso3 in countries_ipc_adm1:
                bkg_layer = FeatureLayer("https://services5.arcgis.com/sjP4Ugu5s0dZWLjd/arcgis/rest/services/Administrative_Boundaries_Reference_(view_layer)/FeatureServer/1")
                bkg_result = bkg_layer.query(where=f"adm0_iso3 = '{adm0_iso3}' AND validity = 'yes'", out_fields='*', return_geometry=True)
            else:
                bkg_layer = FeatureLayer("https://services5.arcgis.com/sjP4Ugu5s0dZWLjd/arcgis/rest/services/Administrative_Boundaries_Reference_(view_layer)/FeatureServer/0")
                bkg_result = bkg_layer.query(where=f"adm0_iso3 = '{adm0_iso3}' AND validity = 'yes'", out_fields='*', return_geometry=True)

            bkg_records = []
            for feat in bkg_result.features:
                geom = feat.geometry
                if geom and "rings" in geom:
                    geojson_geom = {"type": "Polygon", "coordinates": geom["rings"]}
                    attr = feat.attributes
                    attr["geometry"] = shape(geojson_geom)
                    bkg_records.append(attr)

            if not bkg_records:
                raise ValueError("No valid background features for admin1.")

            bkg_gdf = gpd.GeoDataFrame(bkg_records, geometry="geometry", crs="EPSG:4326")

            # === Load DIEM survey coverage layer ===
            main_layer = FeatureLayer("https://services5.arcgis.com/sjP4Ugu5s0dZWLjd/arcgis/rest/services/diem_adm_repr_1_mview/FeatureServer/29")
            main_result = main_layer.query(
                where=f"adm0_iso3 = '{adm0_iso3}' AND round = {round} AND surveys > 2",
                out_fields='*',
                return_geometry=True
            )

            main_records, coll_end_date = [], ""
            for feat in main_result.features:
                geom = feat.geometry
                if geom and "rings" in geom:
                    geojson_geom = {"type": "Polygon", "coordinates": geom["rings"]}
                    attr = feat.attributes
                    attr["geometry"] = shape(geojson_geom)
                    if not coll_end_date and "coll_end_date" in attr and attr["coll_end_date"]:
                        coll_end_date = datetime.utcfromtimestamp(attr["coll_end_date"] / 1000).strftime("%b %Y")
                    main_records.append(attr)

            main_gdf = gpd.GeoDataFrame(main_records, geometry="geometry", crs="EPSG:4326") if main_records else None

            # === Identify which IPC phases are available ===
            available_phases = []
            for col in reversed(phase_columns):  # current, proj1, proj2
                if col in ipc_df.columns and ipc_df[col].notna().any():
                    available_phases.append(col)

            if not available_phases:
                raise ValueError("No usable IPC phase column found in Excel.")

            # === Create subplots based on available phases ===

            ncols = len(available_phases)
            fig, axes = plt.subplots(1, ncols, figsize=(10 * ncols, 10))
            if ncols == 1:
                axes = [axes]
            # Add overall title on top
            #fig.suptitle(f"{adm0_name}: DIEM – IPC/CH coverages", fontsize=18, fontweight='bold', ha='center', y=0.98)
            fig.suptitle(f"{adm0_name} – DIEM survey and IPC/CH phase overlap", fontsize=17, fontweight='bold',
                         ha='center', y=0.98)

            join_field = "adm2_pcode"
            if adm0_iso3 in countries_ipc_adm1:
                join_field = "adm1_pcode"

            for idx, col in enumerate(available_phases):
                ax = axes[idx]
                df_phase = ipc_df[[join_field, col, period_fields[col]]].dropna()
                df_phase = df_phase.rename(columns={col: "phase", period_fields[col]: "reference_period"})
                df_phase["phase"] = df_phase["phase"].astype(int)

                merged_gdf = bkg_gdf.merge(df_phase, on=join_field)

                bkg_gdf.plot(ax=ax, edgecolor="black", facecolor="none", linewidth=0.5)

                for phase_value, style in ipc_phase_styles.items():
                    subset = merged_gdf[merged_gdf["phase"] == phase_value]
                    if not subset.empty:
                        #subset.plot(ax=ax, facecolor=style["color"], edgecolor="black", linewidth=0.5, alpha=0.6, label=style["label"])
                        subset.plot(ax=ax, facecolor=style["color"], edgecolor="black", linewidth=0.5, alpha=1.0, label=style["label"])

                if main_gdf is not None:
                    main_gdf.plot(ax=ax, facecolor='none', edgecolor='red', hatch='//', linewidth=0, alpha=0.9)

                label_map = {
                    "area_phase_current": "Current situation",
                    "area_phase_proj1": "First projection",
                    "area_phase_proj2": "Second projection"
                }
                period_text = df_phase["reference_period"].dropna().iloc[0]
                subtitle = f"DIEM coverage in Round {round} ({coll_end_date}) \nand CH/IPC acute food insecurity phases – {label_map[col]} ({period_text})"
                ax.set_title(subtitle, fontsize=13)
                ax.set_axis_off()


            # === Annotation block ===
            annotation_lines = [f"DIEM Survey R{round} coverage: {diem_survey_coverage}"]
            annotation_lines.append(f"DIEM Survey R{round} target population: {diem_target_pop}")
            if "all" not in diem_survey_coverage.lower():
                for col in available_phases:
                    df_phase = ipc_df[[join_field, col, period_fields[col]]].dropna()
                    df_phase = df_phase.rename(columns={col: "phase", period_fields[col]: "reference_period"})
                    df_phase["phase"] = df_phase["phase"].astype(int)

                    merged_gdf = bkg_gdf.merge(df_phase, on=join_field)
                    merged_gdf = merged_gdf.to_crs(main_gdf.crs)

                    n_total, n_covered = count_covered_geoms(merged_gdf, main_gdf.geometry)

                    phase_label = {
                        "area_phase_current": "Current",
                        "area_phase_proj1": "First proj.",
                        "area_phase_proj2": "Second proj."
                    }.get(col, col)

                    annotation_lines.append(
                        f"{phase_label}: Number of admin units in IPC3+: {n_total} (of which covered by DIEM: {n_covered})")
            annotation_lines.append("")
            annotation_lines.append(
                "Note: figures are indicative, as DIEM and IPC/CH may use different administrative references or levels.")
            annotation_lines.append(
                "All values are automatically derived. For authoritative information, please refer to official DIEM and IPC/CH documentation.")

            annotation_text = "\n".join(annotation_lines)

            # === Legend and export ===
            legend_elements = [Patch(facecolor=style["color"], edgecolor='black', label=style["label"], alpha=0.6)
                               for style in ipc_phase_styles.values()]
            legend_elements.append(Patch(facecolor='none', edgecolor='crimson',
                                         label=f'DIEM Monitoring Survey Round {round} coverage',
                                         hatch='//', linewidth=1))

            fig.subplots_adjust(bottom=0.25)
            bottom_ax = fig.add_axes([0.1, 0.01, 0.8, 0.12])
            bottom_ax.axis("off")
            bottom_ax.legend(handles=legend_elements, loc='upper center', ncol=3, fontsize='medium', frameon=False,
                             bbox_to_anchor=(0.5, 1.0), handlelength=2.5, handleheight=1.5, borderpad=1.0, labelspacing=0.8)

            bottom_ax.text(0.0, 0.25, annotation_text, ha='left', va='top', fontsize=12, transform=bottom_ax.transAxes)
            ipc_ch_source = "ch"
            if adm0_iso3 in countries_ipc_adm1:
                ipc_ch_source = "ipc"
            output_file = os.path.join(output_dir, f"map_{adm0_iso3.lower()}_round{round}_diem_{ipc_ch_source}.png")

            plt.savefig(output_file, bbox_inches='tight')
            plt.close()
            print(f"✅ Excel-based IPC multi-phase map saved to: {output_file}")

        except Exception as e:
            import traceback
            tb = traceback.extract_tb(e.__traceback__)[-1]
            print(f"❌ Failed for {adm0_iso3} at line {tb.lineno} in Excel-based IPC process")
            print(f"   ➤ Code: {tb.line}")
            print(f"   ➤ Error: {type(e).__name__} - {e}")


    else:
        print("No IPC/CH data available since it's in this list", countries_none)
