# Schema Description

This schema describes the structure of tracks audio features dataset.

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

6. **track_release_date**:  
   - Type: Date  
   - Description: Release date of the track.

7. **track_date_added**:  
   - Type: Date  
   - Description: The date when the track was added to the playlist.

8. **track_duration_ms**:  
   - Type: Integer  
   - Description: The duration of the track in milliseconds.

9. **track_popularity**:  
   - Type: Integer  
   - Description: Popularity score of the track (from 0 to 100).

10. **track_position**:  
    - Type: Integer  
    - Description: Position of the track within its playlist (from 1 to 50).

11. **is_explicit**:  
    - Type: Boolean  
    - Description: Indicates whether the track contains explicit content.

12. **country**:  
    - Type: String  
    - Description: The country associated with the playlist have this track.

13. **date**:  
    - Type: Date  
    - Description: A general date crawling this track.

14. **danceability**:  
    - Type: Float  
    - Description: Measure of how suitable a track is for dancing (A value of 0.0 is least danceable and 1.0 is most danceable).

15. **energy**:  
    - Type: Float  
    - Description: Measure of intensity and activity in the track (For example, death metal has high energy, while a Bach prelude scores low on the scale. Value range: 0.0 to 1.0).

16. **key**:  
    - Type: Integer  
    - Description: Musical key of the track, encoded as integers (e.g. 0 = C, 1 = C♯/D♭, 2 = D, and so on. If no key was detected, the value is -1).

17. **loudness**:  
    - Type: Float  
    - Description: The average loudness of the track in decibels (Value range: -60 to 0 db).

18. **mode**:  
    - Type: Integer  
    - Description: Modality of the track (0 for minor, 1 for major).

19. **speechiness**:  
    - Type: Float  
    - Description: Measure of the presence of spoken words in the track 
        - Values above 0.66 describe tracks that are probably made entirely of spoken words. 
        - Values between 0.33 and 0.66 describe tracks that may contain both music and speech, either in sections or layered, including such cases as rap music. 
        - Values below 0.33 most likely represent music and other non-speech-like tracks.

20. **acousticness**:  
    - Type: Float  
    - Description: Measure of how acoustic the track is (Value range: 0.0 to 1.0).

21. **instrumentalness**:  
    - Type: Float  
    - Description: Likelihood of the track being instrumental (value range: 0.0 to 1.0).

22. **liveness**:  
    - Type: Float  
    - Description: Measure of the presence of an audience in the recording (value range: 0.0 to 1.0).

23. **valence**:  
    - Type: Float  
    - Description: Measure of the musical positivity of the track (Value range: 0.0 to 1.0).

24. **tempo**:  
    - Type: Float  
    - Description: The estimated tempo of the track in beats per minute (BPM). In musical terminology, tempo is the speed or pace of a given piece and derives directly from the average beat duration.

25. **time_signature**:  
    - Type: Integer  
    - Description: The overall time signature of the track.
        - The time_signature is represented as an integer ranging from 3 to 7, which corresponds to the number of beats in each bar:
            - 3: Indicates a time signature of "3/4" (3 beats per bar).
            - 4: Indicates a time signature of "4/4" (4 beats per bar, also known as "common time").
            - 5: Indicates a time signature of "5/4" (5 beats per bar).
            - 6: Indicates a time signature of "6/4" (6 beats per bar).
            - 7: Indicates a time signature of "7/4" (7 beats per bar).