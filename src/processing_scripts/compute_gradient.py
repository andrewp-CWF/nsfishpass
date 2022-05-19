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
# Assumes stream network forms a tree where ever node has 0 or 1 out nodes
# Assume - data projection is m length projection or else need to modify how length is computed
# Requires stream name field, in this field a value of UNNAMED represents no-name
#
# In addition to computing vertex and segment gradient it also computes the
# maximum vertex gradient for the stream segment
#
import appconfig

dbTargetSchema = appconfig.config['PROCESSING']['output_schema']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']
dbMainstemField = appconfig.config['MAINSTEM_PROCESSING']['mainstem_id']
dbDownMeasureField = appconfig.config['MAINSTEM_PROCESSING']['downstream_route_measure']
dbUpMeasureField = appconfig.config['MAINSTEM_PROCESSING']['upstream_route_measure']

db3dGeomField = appconfig.config['ELEVATION_PROCESSING']['smoothedgeometry_field']

dbVertexTable = appconfig.config['GRADIENT_PROCESSING']['vertex_gradient_table']
dbDownMeasureField = appconfig.config['MAINSTEM_PROCESSING']['downstream_route_measure']
dbUpMeasureField = appconfig.config['MAINSTEM_PROCESSING']['upstream_route_measure']

db4dGeomField = "geometryzm"
dbSegmentGradientField = appconfig.config['GRADIENT_PROCESSING']['segment_gradient_field']
dbMaxGradientField = appconfig.config['GRADIENT_PROCESSING']['max_vertex_gradient_field']

 
def setupGeometry(connection):
    
    # update geometry column    
    query = f"""
        alter table {dbTargetSchema}.{dbTargetStreamTable} 
        add column if not exists {db4dGeomField} geometry(LinestringZM, {appconfig.dataSrid});
        
        update {dbTargetSchema}.{dbTargetStreamTable} 
        set {db4dGeomField} = st_addmeasure({db3dGeomField}, st_length({db3dGeomField}), 0) 
    """
    
    with connection.cursor() as cursor:
        cursor.execute(query)    
            
    connection.commit()

def computeVertexGraidents(connection):

    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}.{dbVertexTable};
         
        CREATE TABLE {dbTargetSchema}.{dbVertexTable} AS 
        SELECT
            sv.{dbMainstemField},
            sv.{dbDownMeasureField} as downstream_route_measure,
            sv.elevation as elevation_a,
            st_z((st_dump(ST_LocateAlong(s2.{db4dGeomField}, (sv.{dbDownMeasureField} + 100) - s2.{dbDownMeasureField} ))).geom) as elevation_b,
            (st_z((st_dump(ST_LocateAlong(s2.{db4dGeomField}, (sv.{dbDownMeasureField} + 100) - s2.{dbDownMeasureField} ))).geom) - sv.elevation) / 100 as gradient,
            sv.pnt as vertex_pnt,
            ST_LocateAlong(s2.{db4dGeomField}, (sv.{dbDownMeasureField} + 100) - s2.{dbDownMeasureField} ) as upstream_pnt    
        FROM (
           SELECT
            s.{dbMainstemField},
            (((1.0 - ST_LineLocatePoint(s.{db4dGeomField}, ST_PointN(s.{db4dGeomField}, generate_series(1, ST_NPoints(s.{db4dGeomField}) - 1)))) * st_length(s.{db4dGeomField})) + s.downstream_route_measure) as downstream_route_measure,
            ST_Z(ST_PointN(s.{db4dGeomField}, generate_series(1, ST_NPoints(s.{db4dGeomField}) - 1))) AS elevation,
            ST_PointN(s.{db4dGeomField}, generate_series(1, ST_NPoints(s.{db4dGeomField}) - 1)) as pnt
            FROM {dbTargetSchema}.{dbTargetStreamTable} s
            ORDER BY {dbMainstemField}, {dbDownMeasureField}
        ) as sv
        INNER JOIN {dbTargetSchema}.{dbTargetStreamTable} s2 ON sv.{dbMainstemField} = s2.{dbMainstemField} 
          AND sv.{dbDownMeasureField} + 100 >= s2.{dbDownMeasureField} 
          AND sv.{dbDownMeasureField} + 100 < s2.{dbUpMeasureField};
          

        DELETE FROM {dbTargetSchema}.{dbVertexTable} WHERE elevation_a = -999999 or elevation_b = -999999;
        
        ALTER TABLE {dbTargetSchema}.{dbVertexTable} ADD COLUMN grade_class smallint;
        
        UPDATE {dbTargetSchema}.{dbVertexTable} set grade_class = 
          CASE
              WHEN gradient >= .05 AND gradient < .07 THEN 5
              WHEN gradient >= .07 AND gradient < .10 THEN 7
              WHEN gradient >= .10 AND gradient < .12 THEN 10
              WHEN gradient >= .12 AND gradient < .15 THEN 12
              WHEN gradient >= .15 AND gradient < .20 THEN 15
              WHEN gradient >= .20 AND gradient < .25 THEN 20
              WHEN gradient >= .25 AND gradient < .30 THEN 25
              WHEN gradient >= .30 THEN 30
              ELSE 0
        END;

        
        
 --SELECT
    --mainstem_id,
    --min(downstream_route_measure) AS downstream_route_measure,
    --grade_class as gradient_class
  --FROM
 -- (
    --SELECT
    --  mainstem_id,
    --  downstream_route_measure,
    --  grade_class,
    --  count(step OR NULL) OVER (ORDER BY mainstem_id, downstream_route_measure) AS grp
    --FROM  (
     -- SELECT mainstem_id, downstream_route_measure, grade_class
     --      , lag(grade_class, 1, grade_class) OVER (ORDER BY mainstem_id, downstream_route_measure) <> grade_class AS step
    --  FROM ws17010302.vertex_gradient
   -- ) sub1
  --WHERE grade_class != 0
  --) sub2
  --GROUP BY mainstem_id, grade_class, grp
  --ORDER BY mainstem_id, downstream_route_measure
    """
    #print (query)
    with connection.cursor() as cursor:
        cursor.execute(query)    
            
    connection.commit()


def computeSegmentGradient(connection):

    query = f"""
        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN IF NOT EXISTS {dbSegmentGradientField} double precision;
        
        UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
        SET {dbSegmentGradientField} = (ST_Z (ST_PointN ({db4dGeomField}, 1)) - ST_Z (ST_PointN ({db4dGeomField}, -1))) / ST_Length ({db4dGeomField})
    """
    #print (query)
    with connection.cursor() as cursor:
        cursor.execute(query)    
            
    connection.commit()


def computeMaxSegmentGradient(connection):
    
    query = f"""
        CREATE INDEX {dbTargetSchema}_{dbVertexTable}_vertex_pnt_idx on {dbTargetSchema}.{dbVertexTable} using gist(vertex_pnt);

        --compute a max gradient for each stream segments based on the vertex points
        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} 
            add column if not exists {dbMaxGradientField} double precision;

        UPDATE {dbTargetSchema}.{dbTargetStreamTable} set {dbMaxGradientField} = null;
        
        WITH gvalues AS (
            SELECT a.id, max(b.gradient) as maxv
            FROM {dbTargetSchema}.{dbVertexTable} b, {dbTargetSchema}.{dbTargetStreamTable} a
            WHERE st_intersects(a.geometry, b.vertex_pnt)
            GROUP BY a.id)
        UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
        SET {dbMaxGradientField} = gvalues.maxv 
        FROM gvalues 
        WHERE gvalues.id = streams.id;

        UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
        SET {dbMaxGradientField} = {dbSegmentGradientField} 
        WHERE {dbMaxGradientField} is null;
    """
        
    #print (query)
    with connection.cursor() as cursor:
        cursor.execute(query)    
            
    connection.commit()    
#--- main program ---    
with appconfig.connectdb() as conn:
    
    conn.autocommit = False
    
    print("Computing Gradient")
    print("  setting up tables")
    setupGeometry(conn)
    
    print("  computing vertex gradients")
    computeVertexGraidents(conn)
    
    print("  computing segment gradients")
    computeSegmentGradient(conn)
    computeMaxSegmentGradient(conn)
    
print("done")

