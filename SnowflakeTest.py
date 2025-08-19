import os
import json
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

DATA_DIR = r"C:\Users\nick.pemsingh\Desktop\CNC_INSTANT"
JSON_FILES = ["analysis_output_lom.json", "analysis_output_thin.json", "analysis_output_thread.json"]

def main():
    # Load files
    json_data = []
    for filename in JSON_FILES:
        with open(os.path.join(DATA_DIR, filename), 'r') as f:
            json_data.append(json.load(f))
    
    # Connect
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    
    for data in json_data:
        analysis_id = data['sourceDetails']['modelId']
        part_metrics = data.get('partMetrics', {})
        
        # Insert analysis
        cursor.execute(
            "INSERT INTO analysis (analysis_id, model_id, surface_area, x_extent, y_extent, z_extent, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (analysis_id, analysis_id, part_metrics.get('surfaceArea'), part_metrics.get('xExtent'), part_metrics.get('yExtent'), part_metrics.get('zExtent'), part_metrics.get('volume'))
        )
        print(f"✓ Inserted analysis: {analysis_id}")
        
        # Insert applications
        applications = data.get('applications', [])
        for app in applications:
            cursor.execute(
                "INSERT INTO applications (analysis_id, application_name, application_version) VALUES (%s, %s, %s)",
                (analysis_id, app.get('name'), app.get('version'))
            )
        print(f"✓ Inserted {len(applications)} applications")
        
        # Insert toolsets - SKIP VARIANT COLUMNS FOR NOW
        toolsets = data.get('toolsets', [])
        for toolset in toolsets:
            threaded_features = toolset.get('threadedFeatures')
            scene = None
            
            if threaded_features and threaded_features.get('displayInfo'):
                scene = threaded_features.get('displayInfo', {}).get('scene')
            
            cursor.execute(
                "INSERT INTO toolsets (analysis_id, toolset_id, is_5_axis, is_minimal_milling, machining_minutes_part, machining_minutes_bushing, leftover_material_volume, scene) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    analysis_id,
                    toolset.get('toolsetId'),
                    toolset.get('is5Axis'),
                    toolset.get('isMinimalMilling'),
                    toolset.get('machiningMinutesPart'),
                    toolset.get('machiningMinutesBushing'),
                    toolset.get('leftoverMaterialVolume'),
                    scene
                )
            )
        print(f"✓ Inserted {len(toolsets)} toolsets (viewer_files skipped)")
        
        # Insert materials
        material_count = 0
        for toolset in toolsets:
            toolset_id = toolset.get('toolsetId')
            materials = toolset.get('materials', [])
            for material in materials:
                cursor.execute(
                    "INSERT INTO materials VALUES (%s, %s, %s, %s)",
                    (analysis_id, toolset_id, material.get('materialId'), material.get('available'))
                )
                material_count += 1
        print(f"✓ Inserted {material_count} materials")
        
        # Insert block fits
        block_count = 0
        for toolset in toolsets:
            toolset_id = toolset.get('toolsetId')
            materials = toolset.get('materials', [])
            for material in materials:
                material_id = material.get('materialId')
                block_fits = material.get('estimatedBlockFits', [])
                for block in block_fits:
                    cursor.execute(
                        "INSERT INTO block_fits VALUES (%s, %s, %s, %s, %s, %s)",
                        (analysis_id, toolset_id, material_id, block.get('blockId'), block.get('maxParts'), block.get('isSafe'))
                    )
                    block_count += 1
        print(f"✓ Inserted {block_count} block fits")
        
        # Insert threaded features
        feature_count = 0
        for toolset in toolsets:
            toolset_id = toolset.get('toolsetId')
            threaded_features = toolset.get('threadedFeatures')
            
            if threaded_features is None:
                continue
                
            features = threaded_features.get('features', [])
            for feature in features:
                display_info = feature.get('displayInfo', {})
                identifiable_location = feature.get('identifiableLocation', {})
                
                cursor.execute(
                    "INSERT INTO threaded_features VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        analysis_id,
                        toolset_id,
                        feature.get('featureId'),
                        feature.get('featureType'),
                        identifiable_location.get('holeDiameter'),
                        identifiable_location.get('holePointX'),
                        identifiable_location.get('holePointY'),
                        identifiable_location.get('holePointZ'),
                        display_info.get('depth'),
                        display_info.get('through'),
                        display_info.get('axisX'),
                        display_info.get('axisY'),
                        display_info.get('axisZ'),
                        display_info.get('topX'),
                        display_info.get('topY'),
                        display_info.get('topZ')
                    )
                )
                feature_count += 1
        print(f"✓ Inserted {feature_count} threaded features")
        
        # Insert thread options
        option_count = 0
        for toolset in toolsets:
            toolset_id = toolset.get('toolsetId')
            threaded_features = toolset.get('threadedFeatures')
            
            if threaded_features is None:
                continue
                
            features = threaded_features.get('features', [])
            for feature in features:
                feature_id = feature.get('featureId')
                thread_options = feature.get('threadOptions', [])
                
                for option in thread_options:
                    display_info = option.get('displayInfo', {})
                    
                    cursor.execute(
                        "INSERT INTO thread_options VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (
                            analysis_id,
                            toolset_id,
                            feature_id,
                            option.get('threadId'),
                            display_info.get('majorDiameter'),
                            display_info.get('minorDiameter'),
                            display_info.get('threadDepth'),
                            display_info.get('taperAngleRadians'),
                            display_info.get('topDisplayOffset'),
                            display_info.get('topOffset')
                        )
                    )
                    option_count += 1
        print(f"✓ Inserted {option_count} thread options")
        
        # INSERT ADVISORY INFOS - SKIP VARIANT COLUMNS
        advisory_infos = data.get('advisoryInfos', [])
        for advisory in advisory_infos:
            # Extract toolset value from discriminators
            toolset_value = None
            discriminators = advisory.get('discriminators', [])
            for disc in discriminators:
                if disc.get('name') == 'TOOLSET':
                    toolset_value = disc.get('value')
                    break
            
            # Get metadata application
            metadata = advisory.get('metadata', {})
            application = metadata.get('application')
            
            cursor.execute(
                "INSERT INTO advisory_infos (analysis_id, toolset_value, scene, type, application) VALUES (%s, %s, %s, %s, %s)",
                (
                    analysis_id,
                    toolset_value,
                    advisory.get('scene'),
                    advisory.get('type'),
                    application
                )
            )
                
        print(f"✓ Inserted {len(advisory_infos)} advisory infos (VARIANT columns skipped)")
        
        # Insert availability failures - SKIP FOR NOW
        failure_details = data.get('availabilityCheckFailureDetails', [])
        if failure_details:
            print(f"✓ Skipped availability failures: {len(failure_details)} records")
        else:
            print(f"✓ No availability failures to insert")
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Done! (VARIANT columns skipped - can be added later)")

if __name__ == "__main__":
    main()