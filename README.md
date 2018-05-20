# BigData_project1
Implentazione di 3 jobs sui dati estratti da un csv con l'utilizzo di 3 tecnologie diverse: MapReduce, Hive, Spark.


### TODO
- [x] Map Reduce
  - [x] Job 1
  - [x] Job 2
  - [x] Job 3
- [x] Hive:
  - [x] Job 1
  - [x] Job 2
  - [x] Job 3
- [x] Spark
  - [x] Job 1
  - [x] Job 2
  - [x] Job 3


### Tempi di esecuzione (esecuzione in locale)
#### Job 1

|           | 150 MB    | 300 MB    | 600 MB    |
| :---:     | :---:     | :---:     | :---:     |
| MapReduce | -         | 23 sec    | -         |
| Hive      | -         | 46.5 sec  | -         |
| Spark     | 9 sec     | 11 sec    | 23 sec    |

#### Job 2

|           | 150 MB    | 300 MB    | 600 MB    |
| :---:     | :---:     | :---:     | :---:     |
| MapReduce | -         | 19 sec    | -         |
| Hive      | -         | 37.3 sec  | -         |
| Spark     | 7 sec     | 9 sec     | 18 sec    |

#### Job 3

|           | 150 MB    | 300 MB    | 600 MB    |
| :---:     | :---:     | :---:     | :---:     |
| MapReduce | -         | 50 sec    | -         |
| Hive      | -         | 106.3 sec | -         |
| Spark     | 11 sec    | 29 sec    | 33 sec    |

