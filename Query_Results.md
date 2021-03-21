
# Q1 Results
## Q1 with profits
```
+----+------------------------+------------------+
|year|title                   |profit            |
+----+------------------------+------------------+
|2000|Billy Elliot            |2100.0            |
|2001|千と千尋の神隠し           |1732.8339666666668|
|2002|My Big Fat Greek Wedding|7274.880880000001 |
|2003|Tarnation               |532933.9449541284 |
|2004|Super Size Me           |43861.65846153846 |
|2005|웰컴 투 동막골             |4.197475625E8     |
|2006|Facing the Giants       |10078.331         |
|2007|Paranormal Activity     |1288938.6666666667|
|2008|Fireproof               |6591.2634         |
|2009|The Collector           |3252.9411764705883|
|2010|Catfish                 |10053.143333333333|
|2011|From Prada to Nada      |2688072.0430107526|
|2012|Aquí Entre Nos          |2.755583E8        |
|2013|Nurse 3-D               |9.99999E7         |
|2014|The Quiet Ones          |8817.4335         |
|2015|대호                     |221568.98         |
|2016|Split                   |2976.911088888889 |
|2017|A Ghost Story           |15484.255         |
+----+------------------------+------------------+
```

## Q1 (Correct)
```
+----+------------------------+
|year|title                   |
+----+------------------------+
|2000|Billy Elliot            |
|2001|千と千尋の神隠し           |
|2002|My Big Fat Greek Wedding|
|2003|Tarnation               |
|2004|Super Size Me           |
|2005|웰컴 투 동막골             |
|2006|Facing the Giants       |
|2007|Paranormal Activity     |
|2008|Fireproof               |
|2009|The Collector           |
|2010|Catfish                 |
|2011|From Prada to Nada      |
|2012|Aquí Entre Nos          |
|2013|Nurse 3-D               |
|2014|The Quiet Ones          |
|2015|대호                     |
|2016|Split                   |
|2017|A Ghost Story           |
+----+------------------------+
```


# Q2 Results
```
+-----------------+                                                             
|percentResult    |
+-----------------+
|87.47858956942885|
+-----------------+
```

# Q3 Results
```
+---------------+---------------------+-----+                                   
|genre          |AverageRatingPerGenre|Count|
+---------------+---------------------+-----+
|Crime          |3.1630791523933883   |905  |
|Romance        |3.1559472948670755   |1205 |
|TV Movie       |3.148118069220997    |87   |
|Thriller       |3.1485761795680007   |1425 |
|Adventure      |3.168709062525581    |707  |
|Foreign        |3.113985834407914    |271  |
|Drama          |3.139602776165753    |3721 |
|War            |3.1146633826061416   |231  |
|Documentary    |3.0728525756403298   |502  |
|Family         |3.156905491920281    |386  |
|Fantasy        |3.1429135219007036   |463  |
|History        |3.11879104584954     |306  |
|Mystery        |3.1487693926555465   |475  |
|Animation      |3.175885456020015    |223  |
|Music          |3.1890477821347663   |248  |
|Science Fiction|3.1571636839139843   |593  |
|Horror         |3.134020262392475    |723  |
|Western        |3.2140607181871026   |185  |
|Comedy         |3.1375088144154777   |2111 |
|Action         |3.158532929451071    |1175 |
+---------------+---------------------+-----+   
```

# Q4 Results
## Q4 Results RDD
Character Count:
```
2000-2004: 328.69595536959554
2005-2009: 311.7648743238944
2010-2014: 327.1910774410774
2015-2019: 282.182934315531
```
Word Count:
```
2000-2004: 57.96
2005-2009: 54.74
2010-2014: 57.38
2015-2019: 49.6
```
## Q4 Results SparkSQL
Character Count:
```
+------------+------------------+                                               
|5yearsPeriod|AvgSummaryLength  |
+------------+------------------+
|2000-2004   |333.8172804532578 |
|2005-2009   |316.08935483870965|
|2010-2014   |332.03559225512527|
|2015-2019   |286.2241594022416 |
+------------+------------------+
```
Word Count:
```
+------------+-----------------+
|5yearsPeriod|AvgSummaryLength |
+------------+-----------------+
|2000-2004   |57.94049279404928|
|2005-2009   |54.73051224944321|
|2010-2014   |57.36391694725028|
|2015-2019   |49.58563535911602|
+------------+-----------------+
```

# Q5 Results
## Q5 Results RDD
```
('Action', 8659, 588, 'Hulk', 5.0, 'The Day After Tomorrow', 2.0)
('Adventure', 8659, 369, 'Mission: Impossible II', 4.5, 'Harry Potter and the Order of the Phoenix', 2.0)
('Animation', 45811, 117, 'A Grand Day Out', 5.0, 'Astérix et la surprise de César', 0.5)
('Comedy', 45811, 1024, 'Galaxy Quest', 5.0, "There's Something About Mary", 0.5)
('Crime', 8659, 474, 'The Thomas Crown Affair', 5.0, 'Papillon', 2.0)
('Documentary', 45811, 215, 'Crumb', 5.0, 'Paper Clips', 0.5)
('Drama', 45811, 1815, 'Exodus: Gods and Kings', 5.0, 'The Great Escape', 0.5)
('Family', 45811, 198, 'Galaxy Quest', 5.0, 'Astérix et la surprise de César', 0.5)
('Fantasy', 8659, 249, 'Blood: The Last Vampire', 5.0, 'The Addams Family', 0.5)
('Foreign', 45811, 134, 'Dreams of a Life', 5.0, 'Shekvarebuli kulinaris ataserti retsepti', 1.0)
('History', 8659, 155, 'Lawrence of Arabia', 4.5, 'Anna and the King', 2.0)
('Horror', 45811, 350, 'Dawn of the Dead', 5.0, 'Outpost', 0.5)
('Music', 45811, 130, 'Nashville', 5.0, "A Hard Day's Night", 0.5)
('Mystery', 8659, 239, 'Trois couleurs : Rouge', 5.0, 'Harry Potter and the Order of the Phoenix', 2.0)
('Romance', 45811, 584, 'Along Came Polly', 5.0, "There's Something About Mary", 0.5)
('Science Fiction', 8659, 313, 'Hulk', 5.0, 'The Day After Tomorrow', 2.0)
('TV Movie', 45811, 38, 'Straight From the Heart', 4.5, 'Buck Rogers in the 25th Century', 1.5)
('Thriller', 8659, 725, 'The Thomas Crown Affair', 5.0, 'The Day After Tomorrow', 2.0)
('War', 45811, 120, 'Doctor Zhivago', 5.0, 'The Great Escape', 0.5)
('Western', 45811, 84, 'Johnny Guitar', 5.0, 'Firecreek', 1.0)
```

## Q5 Results SparkSQL
```
+---------------+------+----------+-----------------------+---------+-----------------------------------------+---------+
|genre          |userId|NumRatings|MaxTitle               |MaxRating|MinTitle                                 |MinRating|
+---------------+------+----------+-----------------------+---------+-----------------------------------------+---------+
|Action         |8659  |588       |Hulk                   |5.0      |The Day After Tomorrow                   |2.0      |
|Adventure      |8659  |369       |Mission: Impossible II |4.5      |Harry Potter and the Order of the Phoenix|2.0      |
|Animation      |45811 |117       |A Grand Day Out        |5.0      |Astérix et la surprise de César          |0.5      |
|Comedy         |45811 |1026      |Galaxy Quest           |5.0      |There's Something About Mary             |0.5      |
|Crime          |8659  |476       |The Thomas Crown Affair|5.0      |Papillon                                 |2.0      |
|Documentary    |45811 |215       |Crumb                  |5.0      |Paper Clips                              |0.5      |
|Drama          |45811 |1817      |Exodus: Gods and Kings |5.0      |The Great Escape                         |0.5      |
|Family         |45811 |198       |Galaxy Quest           |5.0      |Astérix et la surprise de César          |0.5      |
|Fantasy        |8659  |249       |Blood: The Last Vampire|5.0      |The Addams Family                        |0.5      |
|Foreign        |45811 |134       |Dreams of a Life       |5.0      |Shekvarebuli kulinaris ataserti retsepti |1.0      |
|History        |45811 |155       |The Last Emperor       |5.0      |The Great Escape                         |0.5      |
|History        |8659  |155       |Lawrence of Arabia     |4.5      |Anna and the King                        |2.0      |
|Horror         |45811 |350       |Dawn of the Dead       |5.0      |Outpost                                  |0.5      |
|Music          |45811 |130       |Nashville              |5.0      |A Hard Day's Night                       |0.5      |
|Mystery        |8659  |239       |Trois couleurs : Rouge |5.0      |Harry Potter and the Order of the Phoenix|2.0      |
|Romance        |45811 |586       |Along Came Polly       |5.0      |There's Something About Mary             |0.5      |
|Science Fiction|8659  |313       |Hulk                   |5.0      |The Day After Tomorrow                   |2.0      |
|TV Movie       |45811 |38        |Straight From the Heart|4.5      |Buck Rogers in the 25th Century          |1.5      |
|Thriller       |8659  |727       |The Thomas Crown Affair|5.0      |The Day After Tomorrow                   |2.0      |
|War            |45811 |120       |Doctor Zhivago         |5.0      |The Great Escape                         |0.5      |
|Western        |45811 |84        |Johnny Guitar          |5.0      |Firecreek                                |1.0      |
+---------------+------+----------+-----------------------+---------+-----------------------------------------+---------+
```