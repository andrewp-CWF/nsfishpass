#----------------------------------------------------------------------------------
#
# Copyright 2022 by Canadian Wildlife Federation, Alberta Environment and Parks
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#----------------------------------------------------------------------------------

#
# ASSUMPTION - data is in equal area projection where distance functions return values in metres
#
import appconfig

dbTargetSchema = appconfig.config['PROCESSING']['output_schema']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']
workingWatershedId = appconfig.config['PROCESSING']['watershed_id']

with appconfig.connectdb() as conn:
    
    query = f"""
        CREATE SCHEMA IF NOT EXISTS {dbTargetSchema};
    
        CREATE TABLE IF NOT EXISTS {dbTargetSchema}.{dbTargetStreamTable}(
          {appconfig.dbIdField} uuid not null,
          source_id uuid not null,
          {appconfig.dbWatershedIdField} varchar not null,
          stream_name varchar,
          strahler_order integer,
          geometry geometry(LineString, {appconfig.dataSrid}),
          primary key ({appconfig.dbIdField})
        );
    
        CREATE INDEX {dbTargetSchema}_{dbTargetStreamTable}_geometry_idx ON  {dbTargetSchema}.{dbTargetStreamTable} using gist(geometry);
        
        --ensure results are readable
        GRANT USAGE ON SCHEMA {dbTargetSchema} TO public;
        GRANT SELECT ON {dbTargetSchema}.{dbTargetStreamTable} to public;
    
        DELETE FROM {dbTargetSchema}.{dbTargetStreamTable} WHERE {appconfig.dbWatershedIdField} = '{workingWatershedId}';

        INSERT INTO {dbTargetSchema}.{dbTargetStreamTable} ({appconfig.dbIdField}, source_id, {appconfig.dbWatershedIdField}, stream_name, strahler_order, geometry)
        SELECT uuid_generate_v4(), {appconfig.dbIdField}, {appconfig.dbWatershedIdField}, stream_name, strahler_order, geometry
        FROM {appconfig.dataSchema}.{appconfig.streamTable}
        WHERE {appconfig.dbWatershedIdField} = '{workingWatershedId}';
        
        
        --TODO: remove this when values are provided
        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} add column {appconfig.streamTableChannelConfinementField} numeric;
        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} add column {appconfig.streamTableVelocityField} numeric;
        
        UPDATE {dbTargetSchema}.{dbTargetStreamTable} set {appconfig.streamTableChannelConfinementField} = floor(random() * 100);
        UPDATE {dbTargetSchema}.{dbTargetStreamTable} set {appconfig.streamTableVelocityField} = floor(random() * 100);
        
   
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit();
    
print(f"""Initializing processing for watershed {workingWatershedId} complete.""")