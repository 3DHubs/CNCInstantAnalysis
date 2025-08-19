import os
import json
import pandas as pd
import numpy as np
import snowflake.connector

SNOWFLAKE_CONFIG = {
    'account': 'DWNFBOS-FACTORY',
    'user': 'nick.pemsingh@protolabs.com',
    'authenticator': 'externalbrowser',
    'role': 'DATA_ENGINEER_FR',
    'warehouse': 'ADHOC_WH',
    'database': 'SANDBOX',
    'schema': 'CNC_INSTANT',
}

DATA_DIR = r"C:\Users\nick.pemsingh\Desktop\CNC_INSTANT Sample Data"
JSON_FILES = ["analysis_output_lom.json", "analysis_output_thin.json", "analysis_output_thread.json"]

def clean_dataframe_for_sql(df):
    """Replace NaN values with None for SQL compatibility"""
    return df.replace({np.nan: None})

def parse_json_to_dataframes(json_data_list):
    """Parse all JSON files into pandas DataFrames"""
    
    # Initialize lists for each table
    analysis_data = []
    applications_data = []
    toolsets_data = []
    materials_data = []
    block_fits_data = []
    threaded_features_data = []
    thread_options_data = []
    advisory_infos_data = []
    availability_failures_data = []
    
    for data in json_data_list:
        analysis_id = data['sourceDetails']['modelId']
        part_metrics = data.get('partMetrics', {})
        
        # Analysis data
        analysis_data.append({
            'analysis_id': analysis_id,
            'model_id': analysis_id,
            'surface_area': part_metrics.get('surfaceArea'),
            'x_extent': part_metrics.get('xExtent'),
            'y_extent': part_metrics.get('yExtent'),
            'z_extent': part_metrics.get('zExtent'),
            'volume': part_metrics.get('volume')
        })
        
        # Applications data
        for app in data.get('applications', []):
            applications_data.append({
                'analysis_id': analysis_id,
                'application_name': app.get('name'),
                'application_version': app.get('version')
            })
        
        # Process toolsets and nested data
        toolsets = data.get('toolsets', [])
        for toolset in toolsets:
            toolset_id = toolset.get('toolsetId')
            
            # Toolsets data
            threaded_features = toolset.get('threadedFeatures')
            viewer_files = []
            scene = None
            
            if threaded_features and threaded_features.get('displayInfo'):
                viewer_files = threaded_features.get('displayInfo', {}).get('viewerFiles', [])
                scene = threaded_features.get('displayInfo', {}).get('scene')
            
            toolsets_data.append({
                'analysis_id': analysis_id,
                'toolset_id': toolset_id,
                'is_5_axis': toolset.get('is5Axis'),
                'is_minimal_milling': toolset.get('isMinimalMilling'),
                'machining_minutes_part': toolset.get('machiningMinutesPart'),
                'machining_minutes_bushing': toolset.get('machiningMinutesBushing'),
                'leftover_material_volume': toolset.get('leftoverMaterialVolume'),
                'viewer_files_json': json.dumps(viewer_files),  # Keep as string for now
                'scene': scene
            })
            
            # Materials and block fits data
            for material in toolset.get('materials', []):
                material_id = material.get('materialId')
                
                materials_data.append({
                    'analysis_id': analysis_id,
                    'toolset_id': toolset_id,
                    'material_id': material_id,
                    'available': material.get('available')
                })
                
                # Block fits for this material
                for block in material.get('estimatedBlockFits', []):
                    block_fits_data.append({
                        'analysis_id': analysis_id,
                        'toolset_id': toolset_id,
                        'material_id': material_id,
                        'block_id': block.get('blockId'),
                        'max_parts': block.get('maxParts'),
                        'is_safe': block.get('isSafe')
                    })
            
            # Threaded features and thread options
            if threaded_features and threaded_features.get('features'):
                for feature in threaded_features.get('features', []):
                    display_info = feature.get('displayInfo', {})
                    identifiable_location = feature.get('identifiableLocation', {})
                    feature_id = feature.get('featureId')
                    
                    threaded_features_data.append({
                        'analysis_id': analysis_id,
                        'toolset_id': toolset_id,
                        'feature_id': feature_id,
                        'feature_type': feature.get('featureType'),
                        'hole_diameter': identifiable_location.get('holeDiameter'),
                        'hole_point_x': identifiable_location.get('holePointX'),
                        'hole_point_y': identifiable_location.get('holePointY'),
                        'hole_point_z': identifiable_location.get('holePointZ'),
                        'depth': display_info.get('depth'),
                        'is_through': display_info.get('through'),
                        'axis_x': display_info.get('axisX'),
                        'axis_y': display_info.get('axisY'),
                        'axis_z': display_info.get('axisZ'),
                        'top_x': display_info.get('topX'),
                        'top_y': display_info.get('topY'),
                        'top_z': display_info.get('topZ')
                    })
                    
                    # Thread options for this feature
                    for option in feature.get('threadOptions', []):
                        display_info = option.get('displayInfo', {})
                        
                        thread_options_data.append({
                            'analysis_id': analysis_id,
                            'toolset_id': toolset_id,
                            'feature_id': feature_id,
                            'thread_id': option.get('threadId'),
                            'major_diameter': display_info.get('majorDiameter'),
                            'minor_diameter': display_info.get('minorDiameter'),
                            'thread_depth': display_info.get('threadDepth'),
                            'taper_angle_radians': display_info.get('taperAngleRadians'),
                            'top_display_offset': display_info.get('topDisplayOffset'),
                            'top_offset': display_info.get('topOffset')
                        })
        
        # Advisory infos
        for advisory in data.get('advisoryInfos', []):
            # Extract toolset value
            toolset_value = None
            discriminators = advisory.get('discriminators', [])
            for disc in discriminators:
                if disc.get('name') == 'TOOLSET':
                    toolset_value = disc.get('value')
                    break
            
            metadata = advisory.get('metadata', {})
            
            advisory_infos_data.append({
                'analysis_id': analysis_id,
                'toolset_value': toolset_value,
                'scene': advisory.get('scene'),
                'type': advisory.get('type'),
                'application': metadata.get('application'),
                'viewer_files_json': json.dumps(advisory.get('viewerFiles', [])),
                'properties_json': json.dumps(advisory.get('properties')) if advisory.get('properties') is not None else None
            })
        
        # Availability failures
        failure_details = data.get('availabilityCheckFailureDetails', [])
        if failure_details:
            availability_failures_data.append({
                'analysis_id': analysis_id,
                'failure_details_json': json.dumps(failure_details)
            })
    
    # Convert to DataFrames and clean NaN values
    dataframes = {
        'analysis': clean_dataframe_for_sql(pd.DataFrame(analysis_data)),
        'applications': clean_dataframe_for_sql(pd.DataFrame(applications_data)),
        'toolsets': clean_dataframe_for_sql(pd.DataFrame(toolsets_data)),
        'materials': clean_dataframe_for_sql(pd.DataFrame(materials_data)),
        'block_fits': clean_dataframe_for_sql(pd.DataFrame(block_fits_data)),
        'threaded_features': clean_dataframe_for_sql(pd.DataFrame(threaded_features_data)),
        'thread_options': clean_dataframe_for_sql(pd.DataFrame(thread_options_data)),
        'advisory_infos': clean_dataframe_for_sql(pd.DataFrame(advisory_infos_data)),
        'availability_failures': clean_dataframe_for_sql(pd.DataFrame(availability_failures_data))
    }
    
    return dataframes

def bulk_load_executemany(conn, dataframes):
    """Bulk load using executemany() - no S3 required"""
    cursor = conn.cursor()
    results = {}
    
    # Load each table
    if not dataframes['analysis'].empty:
        data = [tuple(row) for row in dataframes['analysis'].values]
        cursor.executemany(
            "INSERT INTO analysis (analysis_id, model_id, surface_area, x_extent, y_extent, z_extent, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            data
        )
        results['analysis'] = len(data)
        print(f"✅ analysis: {len(data)} rows")
    
    if not dataframes['applications'].empty:
        data = [tuple(row) for row in dataframes['applications'].values]
        cursor.executemany(
            "INSERT INTO applications (analysis_id, application_name, application_version) VALUES (%s, %s, %s)",
            data
        )
        results['applications'] = len(data)
        print(f"✅ applications: {len(data)} rows")
    
    # Toolsets with VARIANT column - handle NaN in JSON strings
    if not dataframes['toolsets'].empty:
        for _, row in dataframes['toolsets'].iterrows():
            # Escape quotes in JSON and handle None values
            viewer_files_json = row['viewer_files_json'] if row['viewer_files_json'] is not None else '[]'
            viewer_files_json = viewer_files_json.replace("'", "''")  # Escape single quotes
            
            cursor.execute(
                f"INSERT INTO toolsets (analysis_id, toolset_id, is_5_axis, is_minimal_milling, machining_minutes_part, machining_minutes_bushing, leftover_material_volume, viewer_files, scene) SELECT %s, %s, %s, %s, %s, %s, %s, PARSE_JSON('{viewer_files_json}'), %s",
                (row['analysis_id'], row['toolset_id'], row['is_5_axis'], row['is_minimal_milling'], 
                 row['machining_minutes_part'], row['machining_minutes_bushing'], row['leftover_material_volume'], row['scene'])
            )
        results['toolsets'] = len(dataframes['toolsets'])
        print(f"✅ toolsets: {len(dataframes['toolsets'])} rows")
    
    if not dataframes['materials'].empty:
        data = [tuple(row) for row in dataframes['materials'].values]
        cursor.executemany(
            "INSERT INTO materials (analysis_id, toolset_id, material_id, available) VALUES (%s, %s, %s, %s)",
            data
        )
        results['materials'] = len(data)
        print(f"✅ materials: {len(data)} rows")
    
    if not dataframes['block_fits'].empty:
        data = [tuple(row) for row in dataframes['block_fits'].values]
        cursor.executemany(
            "INSERT INTO block_fits (analysis_id, toolset_id, material_id, block_id, max_parts, is_safe) VALUES (%s, %s, %s, %s, %s, %s)",
            data
        )
        results['block_fits'] = len(data)
        print(f"✅ block_fits: {len(data)} rows")
    
    if not dataframes['threaded_features'].empty:
        data = [tuple(row) for row in dataframes['threaded_features'].values]
        cursor.executemany(
            "INSERT INTO threaded_features VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            data
        )
        results['threaded_features'] = len(data)
        print(f"✅ threaded_features: {len(data)} rows")
    
    if not dataframes['thread_options'].empty:
        data = [tuple(row) for row in dataframes['thread_options'].values]
        cursor.executemany(
            "INSERT INTO thread_options VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            data
        )
        results['thread_options'] = len(data)
        print(f"✅ thread_options: {len(data)} rows")
    
    # Advisory infos with VARIANT columns
    if not dataframes['advisory_infos'].empty:
        for _, row in dataframes['advisory_infos'].iterrows():
            viewer_files_json = row['viewer_files_json'] if row['viewer_files_json'] is not None else '[]'
            viewer_files_json = viewer_files_json.replace("'", "''")
            
            properties_json = row['properties_json'] if row['properties_json'] is not None else 'null'
            properties_json = properties_json.replace("'", "''")
            
            cursor.execute(
                f"INSERT INTO advisory_infos (analysis_id, toolset_value, scene, type, application, viewer_files, properties) SELECT %s, %s, %s, %s, %s, PARSE_JSON('{viewer_files_json}'), PARSE_JSON('{properties_json}')",
                (row['analysis_id'], row['toolset_value'], row['scene'], row['type'], row['application'])
            )
        results['advisory_infos'] = len(dataframes['advisory_infos'])
        print(f"✅ advisory_infos: {len(dataframes['advisory_infos'])} rows")
    
    if not dataframes['availability_failures'].empty:
        for _, row in dataframes['availability_failures'].iterrows():
            failure_details_json = row['failure_details_json'].replace("'", "''")
            cursor.execute(
                f"INSERT INTO availability_failures (analysis_id, failure_details) SELECT %s, PARSE_JSON('{failure_details_json}')",
                (row['analysis_id'],)
            )
        results['availability_failures'] = len(dataframes['availability_failures'])
        print(f"✅ availability_failures: {len(dataframes['availability_failures'])} rows")
    
    return results

def main():
    print("📂 Loading JSON files...")
    json_data = []
    for filename in JSON_FILES:
        with open(os.path.join(DATA_DIR, filename), 'r') as f:
            json_data.append(json.load(f))
    print(f"✅ Loaded {len(json_data)} JSON files")
    
    print("\n🔄 Parsing JSON to DataFrames...")
    dataframes = parse_json_to_dataframes(json_data)
    
    # Print summary
    print("\n📊 Data Summary:")
    for table_name, df in dataframes.items():
        print(f"  {table_name}: {len(df)} rows")
    
    print(f"\n🔗 Connecting to Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    
    print("\n📤 Bulk loading with executemany()...")
    results = bulk_load_executemany(conn, dataframes)
    
    conn.commit()
    conn.close()
    
    print("\n🎉 Done! Results:")
    for table, result in results.items():
        print(f"  {table}: {result}")

if __name__ == "__main__":
    main()