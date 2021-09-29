Read user, movie, rating input text files use MapReduce, built a HDFS master and slave environment through a Docker image.

The whole process was connected by five MapReduce jobs with I/O files stored in HDFS.

1st MR: 
Read text files line by line. 
Output each user as a key with movies rated by the user.

2nd MR: 
Read in 1st MR, built co-occurrence matrix (or as math calls it “incident matrix”) by putting each pair of movie with count 1.
Add up pairs of movies with total counts.

3rd MR:
Read in 2nd MR, since it is a symmetry matrix, you can normalized according to the first or second movie. So I use first movieA as key, the value as movieB:count (reading each row of the matrix).
Sum up all the total counts from one row (key), and divide each movie count with the total count. Make movie_col as a key, preparing for matrix multiplication in map reduce, because in MR you need to do col multiplication. Think of doing a row x a col kind of matrix multiplication, you get a mini matrix.

4th MR: (matrix multiplication)
Read in 3rd MR, make movie_col as a key, movie=probability as value.
Read in original text file, make movie as a key, user:rating as value.
What we want to do in the reducer here is to multiply every user rating with every movie probability, and we write out user:movie probability*rating.

5th MR:
Gather all user:movie, and sum up all probability*rating for that user with that movie.
