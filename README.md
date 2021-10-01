Read user, movie, rating input text files use MapReduce, built a HDFS master and slave environment through a Docker image.

The whole process was connected by five MapReduce jobs with I/O files stored in HDFS.

Input: userX, movieY, ratingZ

1st MR: 
Read text files line by line. 
Output each user as a key with movies rated by the user.

1st MR: user1 /t movie1:3.5, movie2:4.8, movie3:5.5 ......


2nd MR: 
Read in 1st MR, built co-occurrence matrix (or as math calls it “incident matrix”) by putting each pair of movie with count 1.
Add up pairs of movies with total counts.

2nd map: movie1:rating, movie2:rating ......

2nd reduce: movie1:movie2 value = iterable<1, 1, 1>

3rd MR:
Read in 2nd MR, since it is a symmetry matrix, you can normalized according to the first or second movie. So I use first movieA as key, the value as movieB:count (reading each row of the matrix).
Sum up all the total counts from one row (key), and divide each movie count with the total count. Make movie_col as a key, preparing for matrix multiplication in map reduce, because in MR you need to do col multiplication. Think of doing a row x a col kind of matrix multiplication, you get a mini matrix.

3rd map: 
input: movie_row:movie_col \t count
outputKey = movie_row
outputValue = movie_col=count

3rd reduce:
input: key = movie_row, value = <movie_col1:count, movie_col2:count...>
output: key = movie_col, value = movie_row=count


4th MR: (matrix multiplication)
Read in 3rd MR, make movie_col as a key, movie=probability as value.
Read in original text file, make movie as a key, user:rating as value.
What we want to do in the reducer here is to multiply every user rating with every movie probability, and we write out user:movie probability*rating.


5th MR:
Gather all user:movie, and sum up all probability*rating for that user with that movie.
