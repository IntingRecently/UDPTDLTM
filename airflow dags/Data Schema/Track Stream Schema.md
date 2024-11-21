# Schema Definitions

This schema describes the structure of tracks stream dataset.

## Fields

1. **artists_id**:  
   - Type: String  
   - Description: Unique identifier for the artist(s) associated with the track.

2. **album_id**:  
   - Type: String  
   - Description: Unique identifier for the album containing the track.

3. **track_id**:  
   - Type: String  
   - Description: Unique identifier for the track.

4. **track_uri**:  
   - Type: String  
   - Description: Uniform Resource Identifier (URI) for the track, used for direct referencing.

5. **track_name**:  
   - Type: String  
   - Description: The title of the track.

6. **date**:  
   - Type: Date  
   - Description: The date on which the streaming data was recorded.

7. **stream_daily**:  
   - Type: Integer  
   - Description: The number of streams the track received on a specific date.

8. **stream_total**:  
   - Type: Integer  
   - Description: The cumulative number of streams the track has received up to the specified date.