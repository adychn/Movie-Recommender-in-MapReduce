## Movie Recommender in MapReduce
• Recommended movies to users based on user rating history with item collaborative filtering.

• Built a co-occurrence matrix and a user rating matrix from raw data, combined matrices using MapReduce.

• Used Docker containers to emulate a Hadoop master and slaves environment.

• The whole process was connected by five MapReduce jobs with I/O files stored in HDFS.


## Process
Input format: 1,10001,5.0 (userX, movieY, ratingZ)

### 1st MR
Divide input data by user id.

Reducer output: userX    movieY:ratingZ, movie2:4.8, movie3:5.5...


### 2nd MR
Generate co-occurrence matrix from movies. Add up pairs of movies with total counts.

Reducer output: movieA:movieB    count


### 3rd MR
Normalize the co-occurence matrix. Read in previous output. Since it is a symmetry matrix, you can normalized according to the first or second movie. Choose to  normalized on row by making movie_col as a key, for later easier time to do matrix column multiplication. By reading a row of matrix at a time, you can sum up the row to get a row sum, and then nomalize each column value with respect to the row sum.

Mapper input:   movie_row:movie_col    count

Mapper output:  movie_row    movie_col:count

Reducer output: movie_col    movie_row=probability


### 4th MR
Do matrix multiply, rating matrix x normalized co-occurence matrix. Rating matrix is generated from the original input file. What we want to do in the reducer here is to multiply every user rating with every movie probability, and we write out user:movie probability*rating.

Rating mapper input: original raw file. user, movie, rating.

Rating mapper output: movie_col    user:rating

Normalized co-occurence mapper input:  movie_col    movie_row=probability

Normalized co-occurence mapper output: movie_col    movie_row=probability

Reducer output: user:movie_row    proba * rating


### 5th MR
Gather all userX:movieY, and sum up all probability*rating for that user with that movie.

Mapper input: user:movie_row    proba * rating...

Reducer output: user:movie_row    rating


