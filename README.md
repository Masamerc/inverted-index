# inverted-index

A simple inverted-index written in Scala & Spark RDD API.
Any text file found in ```source_files``` will be read and transformed by Spark to show the following result:
```
each record has "word" as the key and shows the total count from all text files, file that has the most count for the word and the count in that file.
+----+-----------+-------------------+-----+
|word|total_count|       top_location|count|
+----+-----------+-------------------+-----+
| and|       1198|   tamingoftheshrew|  261|
|love|        662|   loveslabourslost|  121|
| all|        655|   tamingoftheshrew|  119|
|good|        629|merrywivesofwindsor|  131|
| sir|        625|       twelfthnight|  336|
|come|        576|merrywivesofwindsor|  125|
|  am|        563|       twelfthnight|   87|
| thy|        557|       twelfthnight|  106|
|  on|        538|   loveslabourslost|   83|
| man|        537|muchadoaboutnothing|  111|
+----+-----------+-------------------+-----+
```
