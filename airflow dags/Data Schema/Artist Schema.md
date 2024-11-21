# Schema Definitions

This schema describes the structure of artists dataset.

## Fields

1. **artist_id**:  
   - Type: String  
   - Description: Unique identifier for the artist.

2. **artist_uri**:  
   - Type: String  
   - Description: Uniform Resource Identifier (URI) for the artist, used for direct referencing.

3. **artist_name**:  
   - Type: String  
   - Description: The name of the artist.

4. **artist_genres**:  
   - Type: Array of Strings  
   - Description: A list of genres associated with the artist.

5. **artist_popularity**:  
   - Type: Integer  
   - Description: Popularity score of the artist (Value range: 0 to 100).

6. **artist_follower**:  
   - Type: Integer  
   - Description: The number of followers the artist has.

7. **artist_image_url**:  
   - Type: String  
   - Description: URL to an profile image representing the artist.