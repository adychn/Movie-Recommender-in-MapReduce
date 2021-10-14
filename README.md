## Movie Recommender in MapReduce
- Recommended movies to users based on user rating history with item collaborative filtering.
- Built a co-occurrence matrix and a user rating matrix from raw data, combined matrices using MapReduce.
- Used Docker containers to emulate a Hadoop master and slaves environment.
- The whole process was connected by five MapReduce jobs with I/O files stored in HDFS.

## Process
### Input file format
```
userX, movieY, ratingZ
1, 10001, 5.0 
2, 10002, 6.0
...
```

### 1st MR
Group movies and its ratings by user ID.
```
Mapper:
key = userID
value = movieY:ratingZ

Reducer: 
key = userID   
value = movieY:ratingZ, movie2:4.8, movie3:5.5...
```

### 2nd MR
Generate co-occurrence matrix from movies. Add up pairs of movies with total counts.
```
Mapper:
key = movie_row:movie_col
value = 1

Reducer: 
key = movie_row:movie_col
value = count
```

### 3rd MR
Normalize the co-occurence matrix. Read in previous output. Since it is a symmetry matrix, you can normalized according to the first or second movie. Choose to  normalized on row by making movie_col as a key, for later easier time to do matrix column multiplication. By reading a row of matrix at a time, you can sum up the row to get a row sum, and then nomalize each column value with respect to the row sum. How much weight does movie_row has for movie_col.
```
Mapper:
key = movie_row
value = movie_col:count

Reducer: 
key = movie_col    
value = movie_row=probability
```

### 4th MR
Do matrix multiply, rating matrix x normalized co-occurence matrix. Rating matrix is generated from the original input file. What we want to do in the reducer here is to multiply every user rating with every movie probability, and we write out user:movie probability*rating.
```
Rating mapper (input is from the original raw file): 
key = movie
value = user:rating

Normalized co-occurence mapper:
key = movie_col    
value = movie_row=proba

Reducer: 
key = user:movie_row    
value = proba * rating
```

### 5th MR
Gather all userX:movieY, and sum up all probability*rating for that user with that movie.
```
Mapper: 
key = user:movie_row    
value = [value1, value2, value3...]

Reducer: 
key = user:movie    
value = rating
```

