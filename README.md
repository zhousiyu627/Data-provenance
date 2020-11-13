# This repository contains solutions to the CS591 L1 assignments 

## rewrite code:

1. Use ATuple encapsulate individual tuples
2. Add __repr__(self)
3. Use dictionaries in join

## Input Data

Queries of assignments 1 and 2 expect two space-delimited text files (similar to CSV files). 

The first file (friends) must include records of the form:

|UID1 (int)|UID2 (int)|
|----|----|
|1   |2342|
|231 |3   |
|... |... |

The second file (ratings) must include records of the form:

|UID (int)|MID (int)|RATING (int)|
|---|---|------|
|1  |10 |4     |
|231|54 |2     |
|...|...|...   |

## Author

Siyu Zhou

siyuzhou@bu.edu

U82144100

## Running queries of Assignment 1

You can run queries as shown below: 

```bash
$ python assignment_12.py --task [task_number] --friends [path_to_friends_file.txt] --ratings [path_to_ratings_file.txt] --uid [user_id] --mid [movie_id]
```

For example, the following command runs the 'likeness prediction' query of the first task for user id 10 and movie id 3:

```bash
$ python skeleton/assignment_12.py --task 1 --friends data/friends.txt --ratings data/movie_ratings.txt --uid 10
$ python skeleton/assignment_12.py --task 2 --friends data/friends.txt --ratings data/movie_ratings.txt --uid 10 --mid 3
$ python skeleton/assignment_12.py --task 3 --friends data/friends.txt --ratings data/movie_ratings.txt --uid 10
$ python skeleton/assignment_12.py --task 4 --friends data/friends.txt --ratings data/movie_ratings.txt --uid 10
```

For different dataset:
```bash
$ python skeleton/assignment_12.py --task 4 --friends data/toyfriends.txt --ratings data/toymovie.txt --uid 8
$ python skeleton/assignment_12.py --task 4 --friends data/f_t.txt --ratings data/m_t.txt --uid 1
```


To test the function:
```bash
$ pytest skeleton/tests.py
```

The 'recommendation' query of the second task does not require a movie id. If you provide a `--mid` argument, it will be simply ignored.

## Example Output

```bash
python skeleton/assignment_12.py --task 1 --friends data/friends.txt --ratings data/movie_ratings.txt --uid 8 --mid 3
```

For task 1, the terminal will output the top1 movie with its score.
Then, output a list that represent the lineage
```bash
movie2357 has avg score 5.0
Retrieve the lineage:
[[[('8', '6'), ('6', '2357', '5')], [('8', '10'), ('10', '2357', '5')]], [[('8', '6'), ('6', '2618', '5')]]]
```

```bash
python skeleton/assignment_12.py --task 2 --friends data/friends.txt --ratings data/movie_ratings.txt --uid 8 --mid 3
```

For task 2, the terminal will output a list of lineage, average and Where-provenance
```bash
Retrieve the lineage:
[[[('8', '6'), ('6', '3', '4')], [('8', '10'), ('10', '3', '2')], [('8', '14'), ('14', '3', '0')]...]]
The average is:
2.3089430894308944
Implement Where-provenance query:
[[('data/movie_ratings.txt', 10, ('6', '3', '4'), '4'), ('data/movie_ratings.txt', 49811, ...]
```

```bash
python skeleton/assignment_12.py --task 3 --friends data/friends.txt --ratings data/movie_ratings.txt --uid 8 --mid 3
```

For task 3, the terminal will output a list of lineage, average, Where-provenance and How-provenance query
```bash
Retrieve the lineage:
[[[('8', '6'), ('6', '2357', '5')], [('8', '10'), ('10', '2357', '5')]]]
movie2357 has avg score 5.0
Implement Where-provenance query:
[[('data/movie_ratings.txt', 2311, ('6', '2357', '5'), '5'), ('data/movie_ratings.txt', 52042, ('10', '2357', '5'), '5')]]
Implement How-provenance query:
TopK( (f2712*r2311@5), (f2546*r52042@5) )
```

```bash
python skeleton/assignment_12.py --task 4 --friends data/friends.txt --ratings data/movie_ratings.txt --uid 8 --mid 3
```

For task 4, the terminal will output a list of lineage, average, Where-provenance, How-provenance query and responsible-provenance query
```bash
Retrieve the lineage:
[[[('8', '6'), ('6', '2357', '5')], [('8', '10'), ('10', '2357', '5')]]]
movie2357 has avg score 5.0
Implement Where-provenance query:
[[('data/movie_ratings.txt', 2311, ('6', '2357', '5'), '5'), ('data/movie_ratings.txt', 52042, ('10', '2357', '5'), '5')]]
Implement How-provenance query:
TopK( (f2712*r2311@5), (f2546*r52042@5) )
Retrieve the lineage:
[[[('8', '6'), ('6', '2357', '5')], [('8', '10'), ('10', '2357', '5')]]]
Implement responsible-provenance query:
[(('8', '6'), 0.5), (('6', '2357', '5'), 0.5), (('8', '10'), 0.5), (('10', '2357', '5'), 0.5)]
```
