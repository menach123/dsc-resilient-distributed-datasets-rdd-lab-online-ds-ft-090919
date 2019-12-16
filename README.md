
## Resilient Distributed Datasets (RDDs) - Lab

Resilient Distributed Datasets (RDD) are fundamental data structures of Spark. An RDD is essentially the Spark representation of a set of data, spread across multiple machines, with APIs to let you act on it. An RDD can come from any data source, e.g. text files, a database, a JSON file, etc.


## Objectives

You will be able to:

- Apply the map(func) transformation to a given function on all elements of an RDD in different partitions 
- Apply a map transformation for all elements of an RDD 
- Compare the difference between a transformation and an action within RDDs 
- Use collect(), count(), and take() actions to trigger spark transformations  
- Use filter to select data that meets certain specifications within an RDD 
- Set number of partitions for parallelizing RDDs 
- Create RDDs from Python collections 


## What are RDDs? 

To get a better understanding of RDDs, let's break down each one of the components of the acronym RDD:

Resilient: RDDs are considered "resilient" because they have built-in fault tolerance. This means that even if one of the nodes goes offline, RDDs will be able to restore the data. This is already a huge advantage compared to standard storage. If a standard computer dies while performing an operation, all of its memory will be lost in the process. With RDDs, multiple nodes can go offline, and the action will still be held in working memory.

Distributed: The data is contained on multiple nodes of a cluster-computing operation. It is efficiently partitioned to allow for parallelism.

Dataset: The dataset has been * partitioned * across the multiple nodes. 

RDDs are the building block upon which more high-level Spark operations are based upon. Chances are, if you are performing an action using Spark, the operation involves RDDs. 



Key Characteristics of RDDs:

- Immutable: Once an RDD is created, it cannot be modified. 
- Lazily Evaluated: RDDs will not be evaluated until an action is triggered. Essentially, when RDDs are created, they are programmed to perform some action, but that function will not get activated until it is explicitly called. The reason for lazy evaluation is that allows users to organize the actions of their Spark program into smaller actions. It also saves unnecessary computation and memory load.
- In-Memory: The operations in Spark are performed in-memory rather than in the database. This is what allows Spark to perform fast operations with very large quantities of data.




### RDD Transformations vs Actions

In Spark, we first create a __base RDD__ and then apply one or more transformations to that base RDD following our processing needs. Being immutable means, **once an RDD is created, it cannot be changed**. As a result, **each transformation of an RDD creates a new RDD**. Finally, we can apply one or more **actions** to the RDDs. Spark uses lazy evaluation, so transformations are not actually executed until an action occurs.


<img src="./images/rdd_diagram.png" width=500>

### Transformations

Transformations create a new dataset from an existing one by passing each dataset element through a function and returning a new RDD representing the results. In short, creating an RDD from an existing RDD is ‘transformation’.
All transformations in Spark are lazy. They do not compute their results right away. Instead, they just remember the transformations applied to some base dataset (e.g. a file). The transformations are only computed when an action requires a result that needs to be returned to the driver program.
A transformation is an RDD that returns another RDD, like map, flatMap, filter, reduceByKey, join, cogroup, etc.

### Actions
Actions return final results of RDD computations. Actions trigger execution using lineage graph to load the data into original RDD and carry out all intermediate transformations and return the final results to the driver program or writes it out to the file system. An action returns a value (to a Spark driver - the user program).

Here are some key transformations and actions that we will explore.


| Transformations   | Actions       |
|-------------------|---------------|
| map(func)         | reduce(func)  |
| filter(func)      | collect()     |
| groupByKey()      | count()       |
| reduceByKey(func) | first()       |
| mapValues(func)   | take()        |
| sample()          | countByKey()  |
| distinct()        | foreach(func) |
| sortByKey()       |               |


Let's see how transformations and actions work through a simple example. In this example, we will perform several actions and transformations on RDDs in order to obtain a better understanding of Spark processing. 

### Create a Python collection 

We need some data to start experimenting with RDDs. Let's create some sample data and see how RDDs handle it. To practice working with RDDs, we're going to use a simple Python list.

- Create a Python list `data` of integers between 1 and 1000 using the `range()` function. 
- Sanity check: confirm the length of the list (it should be 1000)


```python
data = list(range(1,1001))
len(data)

# 1000
```




    1000



### Initialize an RDD

When using Spark to make computations, datasets are treated as lists of entries. Those lists are split into different partitions across different cores or different computers. Each list of data held in memory is a partition of the RDD. The reason why Spark is able to make computations far faster than other big data processing languages is that it allows all data to be stored __in-memory__, which allows for easy access to the data and, in turn, high-speed processing. Here is an example of how the alphabet might be split into different RDDs and held across a distributed collection of nodes:

<img src ="./images/partitions_1.png" width ="500">  
To initialize an RDD, first import `pyspark` and then create a SparkContext assigned to the variable `sc`. Use `'local[*]'` as the master.


```python
import pyspark
sc = pyspark.SparkContext('local[*]')
```

Once you've created the SparkContext, you can use the `.parallelize()` method to create an RDD that will distribute the list of numbers across multiple cores. Here, create one called `rdd` with 10 partitions using `data` as the collection you are parallelizing.


```python
rdd = sc.parallelize(data, numSlices=10)
print(type(rdd))
# <class 'pyspark.rdd.RDD'>
```

    <class 'pyspark.rdd.RDD'>
    

Determine how many partitions are being used with this RDD with the `.getNumPartitions()` method.


```python
rdd.getNumPartitions()
# 10
```




    10



### Basic descriptive RDD actions

Let's perform some basic operations on our RDD. In the cell below, use the methods:
* `count`: returns the total count of items in the RDD 
* `first`: returns the first item in the RDD
* `take`: returns the first `n` items in the RDD
* `top`: returns the top `n` items
* `collect`: returns everything from your RDD


It's important to note that in a big data context, calling the collect method will often take a very long time to execute and should be handled with care!


```python
# count
rdd.count()
```




    1000




```python
# first
rdd.first()
```




    1




```python
# take
rdd.take(3)
```




    [1, 2, 3]




```python
# top
rdd.top(10)
```




    [1000, 999, 998, 997, 996, 995, 994, 993, 992, 991]




```python
# collect
rdd.collect()
```




    [1,
     2,
     3,
     4,
     5,
     6,
     7,
     8,
     9,
     10,
     11,
     12,
     13,
     14,
     15,
     16,
     17,
     18,
     19,
     20,
     21,
     22,
     23,
     24,
     25,
     26,
     27,
     28,
     29,
     30,
     31,
     32,
     33,
     34,
     35,
     36,
     37,
     38,
     39,
     40,
     41,
     42,
     43,
     44,
     45,
     46,
     47,
     48,
     49,
     50,
     51,
     52,
     53,
     54,
     55,
     56,
     57,
     58,
     59,
     60,
     61,
     62,
     63,
     64,
     65,
     66,
     67,
     68,
     69,
     70,
     71,
     72,
     73,
     74,
     75,
     76,
     77,
     78,
     79,
     80,
     81,
     82,
     83,
     84,
     85,
     86,
     87,
     88,
     89,
     90,
     91,
     92,
     93,
     94,
     95,
     96,
     97,
     98,
     99,
     100,
     101,
     102,
     103,
     104,
     105,
     106,
     107,
     108,
     109,
     110,
     111,
     112,
     113,
     114,
     115,
     116,
     117,
     118,
     119,
     120,
     121,
     122,
     123,
     124,
     125,
     126,
     127,
     128,
     129,
     130,
     131,
     132,
     133,
     134,
     135,
     136,
     137,
     138,
     139,
     140,
     141,
     142,
     143,
     144,
     145,
     146,
     147,
     148,
     149,
     150,
     151,
     152,
     153,
     154,
     155,
     156,
     157,
     158,
     159,
     160,
     161,
     162,
     163,
     164,
     165,
     166,
     167,
     168,
     169,
     170,
     171,
     172,
     173,
     174,
     175,
     176,
     177,
     178,
     179,
     180,
     181,
     182,
     183,
     184,
     185,
     186,
     187,
     188,
     189,
     190,
     191,
     192,
     193,
     194,
     195,
     196,
     197,
     198,
     199,
     200,
     201,
     202,
     203,
     204,
     205,
     206,
     207,
     208,
     209,
     210,
     211,
     212,
     213,
     214,
     215,
     216,
     217,
     218,
     219,
     220,
     221,
     222,
     223,
     224,
     225,
     226,
     227,
     228,
     229,
     230,
     231,
     232,
     233,
     234,
     235,
     236,
     237,
     238,
     239,
     240,
     241,
     242,
     243,
     244,
     245,
     246,
     247,
     248,
     249,
     250,
     251,
     252,
     253,
     254,
     255,
     256,
     257,
     258,
     259,
     260,
     261,
     262,
     263,
     264,
     265,
     266,
     267,
     268,
     269,
     270,
     271,
     272,
     273,
     274,
     275,
     276,
     277,
     278,
     279,
     280,
     281,
     282,
     283,
     284,
     285,
     286,
     287,
     288,
     289,
     290,
     291,
     292,
     293,
     294,
     295,
     296,
     297,
     298,
     299,
     300,
     301,
     302,
     303,
     304,
     305,
     306,
     307,
     308,
     309,
     310,
     311,
     312,
     313,
     314,
     315,
     316,
     317,
     318,
     319,
     320,
     321,
     322,
     323,
     324,
     325,
     326,
     327,
     328,
     329,
     330,
     331,
     332,
     333,
     334,
     335,
     336,
     337,
     338,
     339,
     340,
     341,
     342,
     343,
     344,
     345,
     346,
     347,
     348,
     349,
     350,
     351,
     352,
     353,
     354,
     355,
     356,
     357,
     358,
     359,
     360,
     361,
     362,
     363,
     364,
     365,
     366,
     367,
     368,
     369,
     370,
     371,
     372,
     373,
     374,
     375,
     376,
     377,
     378,
     379,
     380,
     381,
     382,
     383,
     384,
     385,
     386,
     387,
     388,
     389,
     390,
     391,
     392,
     393,
     394,
     395,
     396,
     397,
     398,
     399,
     400,
     401,
     402,
     403,
     404,
     405,
     406,
     407,
     408,
     409,
     410,
     411,
     412,
     413,
     414,
     415,
     416,
     417,
     418,
     419,
     420,
     421,
     422,
     423,
     424,
     425,
     426,
     427,
     428,
     429,
     430,
     431,
     432,
     433,
     434,
     435,
     436,
     437,
     438,
     439,
     440,
     441,
     442,
     443,
     444,
     445,
     446,
     447,
     448,
     449,
     450,
     451,
     452,
     453,
     454,
     455,
     456,
     457,
     458,
     459,
     460,
     461,
     462,
     463,
     464,
     465,
     466,
     467,
     468,
     469,
     470,
     471,
     472,
     473,
     474,
     475,
     476,
     477,
     478,
     479,
     480,
     481,
     482,
     483,
     484,
     485,
     486,
     487,
     488,
     489,
     490,
     491,
     492,
     493,
     494,
     495,
     496,
     497,
     498,
     499,
     500,
     501,
     502,
     503,
     504,
     505,
     506,
     507,
     508,
     509,
     510,
     511,
     512,
     513,
     514,
     515,
     516,
     517,
     518,
     519,
     520,
     521,
     522,
     523,
     524,
     525,
     526,
     527,
     528,
     529,
     530,
     531,
     532,
     533,
     534,
     535,
     536,
     537,
     538,
     539,
     540,
     541,
     542,
     543,
     544,
     545,
     546,
     547,
     548,
     549,
     550,
     551,
     552,
     553,
     554,
     555,
     556,
     557,
     558,
     559,
     560,
     561,
     562,
     563,
     564,
     565,
     566,
     567,
     568,
     569,
     570,
     571,
     572,
     573,
     574,
     575,
     576,
     577,
     578,
     579,
     580,
     581,
     582,
     583,
     584,
     585,
     586,
     587,
     588,
     589,
     590,
     591,
     592,
     593,
     594,
     595,
     596,
     597,
     598,
     599,
     600,
     601,
     602,
     603,
     604,
     605,
     606,
     607,
     608,
     609,
     610,
     611,
     612,
     613,
     614,
     615,
     616,
     617,
     618,
     619,
     620,
     621,
     622,
     623,
     624,
     625,
     626,
     627,
     628,
     629,
     630,
     631,
     632,
     633,
     634,
     635,
     636,
     637,
     638,
     639,
     640,
     641,
     642,
     643,
     644,
     645,
     646,
     647,
     648,
     649,
     650,
     651,
     652,
     653,
     654,
     655,
     656,
     657,
     658,
     659,
     660,
     661,
     662,
     663,
     664,
     665,
     666,
     667,
     668,
     669,
     670,
     671,
     672,
     673,
     674,
     675,
     676,
     677,
     678,
     679,
     680,
     681,
     682,
     683,
     684,
     685,
     686,
     687,
     688,
     689,
     690,
     691,
     692,
     693,
     694,
     695,
     696,
     697,
     698,
     699,
     700,
     701,
     702,
     703,
     704,
     705,
     706,
     707,
     708,
     709,
     710,
     711,
     712,
     713,
     714,
     715,
     716,
     717,
     718,
     719,
     720,
     721,
     722,
     723,
     724,
     725,
     726,
     727,
     728,
     729,
     730,
     731,
     732,
     733,
     734,
     735,
     736,
     737,
     738,
     739,
     740,
     741,
     742,
     743,
     744,
     745,
     746,
     747,
     748,
     749,
     750,
     751,
     752,
     753,
     754,
     755,
     756,
     757,
     758,
     759,
     760,
     761,
     762,
     763,
     764,
     765,
     766,
     767,
     768,
     769,
     770,
     771,
     772,
     773,
     774,
     775,
     776,
     777,
     778,
     779,
     780,
     781,
     782,
     783,
     784,
     785,
     786,
     787,
     788,
     789,
     790,
     791,
     792,
     793,
     794,
     795,
     796,
     797,
     798,
     799,
     800,
     801,
     802,
     803,
     804,
     805,
     806,
     807,
     808,
     809,
     810,
     811,
     812,
     813,
     814,
     815,
     816,
     817,
     818,
     819,
     820,
     821,
     822,
     823,
     824,
     825,
     826,
     827,
     828,
     829,
     830,
     831,
     832,
     833,
     834,
     835,
     836,
     837,
     838,
     839,
     840,
     841,
     842,
     843,
     844,
     845,
     846,
     847,
     848,
     849,
     850,
     851,
     852,
     853,
     854,
     855,
     856,
     857,
     858,
     859,
     860,
     861,
     862,
     863,
     864,
     865,
     866,
     867,
     868,
     869,
     870,
     871,
     872,
     873,
     874,
     875,
     876,
     877,
     878,
     879,
     880,
     881,
     882,
     883,
     884,
     885,
     886,
     887,
     888,
     889,
     890,
     891,
     892,
     893,
     894,
     895,
     896,
     897,
     898,
     899,
     900,
     901,
     902,
     903,
     904,
     905,
     906,
     907,
     908,
     909,
     910,
     911,
     912,
     913,
     914,
     915,
     916,
     917,
     918,
     919,
     920,
     921,
     922,
     923,
     924,
     925,
     926,
     927,
     928,
     929,
     930,
     931,
     932,
     933,
     934,
     935,
     936,
     937,
     938,
     939,
     940,
     941,
     942,
     943,
     944,
     945,
     946,
     947,
     948,
     949,
     950,
     951,
     952,
     953,
     954,
     955,
     956,
     957,
     958,
     959,
     960,
     961,
     962,
     963,
     964,
     965,
     966,
     967,
     968,
     969,
     970,
     971,
     972,
     973,
     974,
     975,
     976,
     977,
     978,
     979,
     980,
     981,
     982,
     983,
     984,
     985,
     986,
     987,
     988,
     989,
     990,
     991,
     992,
     993,
     994,
     995,
     996,
     997,
     998,
     999,
     1000]



## Map functions

Now that you've been working a little bit with RDDs, let's make this a little more interesting. Imagine you're running a hot new e-commerce startup called BuyStuff, and you're trying to track of how much it charges customers from each item sold. In the next cell, we're going to create simulated data by multiplying the values 1-1000 with a random number from 0-1.


```python
import random
import numpy as np

nums = np.array(range(1, 1001))
sales_figures = nums * np.random.rand(1000)
sales_figures
```




    array([8.99738734e-01, 1.23058366e+00, 1.54902972e+00, 3.80758152e+00,
           2.34904253e+00, 9.22410543e-01, 5.76873232e-01, 6.09336492e+00,
           3.35722311e+00, 3.18693384e+00, 9.81550513e+00, 9.74909010e+00,
           3.59517771e+00, 4.16347541e+00, 2.14668817e+00, 7.57909156e+00,
           6.43507829e+00, 9.48363798e+00, 8.43814336e+00, 1.98254320e+01,
           1.14750499e+01, 3.59400316e+00, 1.55016799e+01, 7.27700419e+00,
           7.82705721e+00, 2.01709456e+01, 1.51405921e+01, 8.45342014e-01,
           1.56055888e+01, 2.61625371e+01, 2.39735187e+01, 2.58496331e+00,
           7.82294496e+00, 7.79171211e+00, 2.94357030e+01, 1.13042974e+00,
           1.09065557e+00, 4.85309023e+00, 1.40457709e+01, 2.10437696e+01,
           1.04453514e+01, 3.08199251e+01, 4.12952007e+01, 3.11489547e+00,
           1.04992553e+01, 4.48726013e+01, 1.53697759e+01, 4.62824445e+01,
           4.59786159e+01, 2.88750292e+01, 4.59177169e+01, 2.38080616e+01,
           1.28099206e+01, 8.46673984e+00, 3.53295950e+01, 2.86564667e+01,
           4.31520645e+01, 4.12157959e+01, 3.43396934e+01, 1.66596523e+00,
           5.73606925e+01, 1.18469856e+01, 1.17155238e+01, 6.20544133e+01,
           1.53087626e+01, 2.59992354e+01, 5.08611449e+01, 1.20554652e+01,
           5.41305622e+01, 5.67575680e+01, 1.81777123e+00, 5.96940995e+01,
           3.19916573e+01, 2.43500806e+01, 3.02606387e+01, 6.89985185e+01,
           7.42753416e+01, 4.51414817e+01, 6.21297155e+01, 5.16329733e-01,
           6.18718440e+01, 2.90929366e+01, 3.23571527e+00, 8.26070901e+01,
           1.22639349e+01, 4.46758545e+01, 6.69534784e+01, 4.55480090e+01,
           5.60304170e+01, 3.74287443e+01, 6.28222375e+01, 7.95498743e+01,
           8.89418894e+01, 6.51547927e+01, 4.60028455e+01, 3.57224919e+00,
           1.28860313e+01, 2.05297187e+01, 6.83901324e+01, 7.26638775e+01,
           2.22028715e+01, 5.94536781e+00, 8.23776523e+01, 9.05346358e+01,
           1.01095595e+02, 7.83357293e+01, 6.61323258e+01, 3.61293757e+01,
           4.25779923e+01, 4.37010485e+01, 6.25392467e+01, 5.31557425e+01,
           9.77495027e+01, 4.22616023e+01, 2.44965858e+01, 4.46134060e+01,
           9.54965066e+01, 6.20869452e+01, 4.32292093e+01, 8.85146790e+01,
           3.75110707e+01, 2.29665449e+01, 4.59452109e+01, 9.94060426e+01,
           6.97162840e+01, 5.50268309e+01, 9.65711787e+01, 5.25113826e+01,
           3.47517422e+01, 3.30394440e+01, 5.36221552e+01, 2.84246437e+00,
           8.52172707e+01, 9.99441178e+01, 4.88012743e+01, 7.49960430e+01,
           1.35785097e+01, 3.12995607e+01, 4.29930576e+00, 5.21233508e+01,
           8.17536106e+00, 1.35742484e+02, 7.14127299e+01, 1.10021528e+02,
           7.52797230e+00, 4.04526946e+01, 1.16982748e+02, 9.66089119e+01,
           1.39901526e+02, 3.57568835e+01, 1.05725573e+02, 3.18294527e+01,
           1.84464746e+01, 1.27089250e+02, 8.26281003e+01, 7.28479486e+01,
           4.97405317e+01, 1.55231211e+02, 9.21088105e+00, 9.01929196e+01,
           2.56696715e+01, 1.48219633e+02, 1.21218731e+02, 9.41638219e+01,
           8.13465014e+01, 9.41333548e+01, 7.74162178e+01, 1.52858888e+02,
           1.33356907e+02, 1.18271266e+02, 3.52930392e+01, 6.04978348e+01,
           1.62590240e+02, 1.70268571e+02, 1.66115901e+02, 2.23925603e+01,
           1.69029316e+02, 1.26285510e+02, 1.53251773e+02, 6.30131554e+01,
           7.83216181e+01, 5.32525706e+01, 1.69314259e+02, 3.23493914e+00,
           2.55590981e+01, 6.57220948e+01, 1.62972515e+02, 1.39037840e+02,
           1.03562904e+02, 1.07398279e+01, 1.42775799e+02, 1.11261619e+02,
           9.03271892e+01, 9.10762195e+01, 9.15384151e+00, 1.93835921e+02,
           8.10390803e+01, 1.96330881e+02, 4.52723530e+01, 1.42789910e+02,
           1.11848686e+02, 1.26221352e+02, 6.83888655e+01, 1.17690365e+02,
           5.79343974e+01, 1.16882652e+02, 8.19494178e+01, 7.44732492e+01,
           5.58720484e+01, 8.51948250e+01, 1.53347051e+01, 4.02974713e+00,
           1.40934292e+02, 1.25133419e+02, 9.48815555e+01, 1.21414481e+02,
           1.84562464e+02, 6.72644758e+01, 2.17178232e+02, 6.47248285e+01,
           1.46898977e+02, 1.56874568e+02, 9.10959159e+01, 1.48005333e+02,
           1.20995869e+02, 1.11583280e+02, 1.08873788e+02, 7.97084794e+01,
           9.36533516e+01, 4.30729389e+01, 2.10131203e+02, 3.27647102e+01,
           4.13091666e+01, 1.62900894e+02, 1.66313895e+02, 8.71402389e+01,
           2.18228693e+02, 1.77387863e+02, 2.13440780e+02, 1.17696462e+02,
           1.34569541e+02, 1.87089593e+02, 3.92316903e+01, 9.01609195e+01,
           5.76928305e+00, 1.72053410e+02, 2.53850974e+01, 2.29168885e+01,
           7.44854629e+01, 2.01291346e+02, 2.38469072e+01, 1.20585969e+02,
           1.74535326e+02, 2.51778093e+02, 3.12970945e+01, 2.50560325e+02,
           1.96725702e+02, 1.49971819e+01, 5.36856663e+01, 7.19131707e+01,
           1.17125852e+02, 4.82014786e+01, 1.51621460e+02, 2.52474464e+02,
           1.26302344e+01, 1.06135302e+02, 1.15430657e+02, 2.29704902e+02,
           2.03800426e+01, 2.03066317e+02, 1.88304656e+02, 3.63875781e+01,
           1.48274176e+02, 1.33828953e+02, 1.76545312e+02, 1.26904439e+02,
           2.27210361e+02, 2.16664500e+01, 5.27661201e+01, 1.00841453e+02,
           1.40029913e+01, 2.60806687e+02, 2.16376898e+02, 1.14184087e+02,
           3.52669331e+00, 2.65583061e+02, 2.78377861e+02, 2.06984480e+02,
           1.65446639e+02, 2.04855904e+02, 1.00050228e+02, 1.11831519e+01,
           1.61671785e+02, 1.84083592e+02, 9.65530945e+01, 1.97253698e+02,
           7.80417582e+00, 1.93287923e+02, 1.62405004e+02, 3.93623132e+01,
           9.31952118e+01, 1.63529365e+02, 1.07943167e+01, 2.33191247e+02,
           2.28755542e+02, 3.51410083e+01, 2.08495044e+02, 1.43409221e+02,
           7.09052448e+01, 2.15742954e+01, 1.38208998e+02, 1.34148619e+02,
           3.26070461e+01, 2.53036205e+02, 2.57298834e+02, 9.95655697e+01,
           3.89288470e+01, 3.09921432e+02, 1.46599976e+02, 5.16351164e+01,
           2.17297185e+01, 7.29451433e+01, 3.71184557e+01, 2.62575257e+02,
           2.04545282e+02, 2.63075800e+01, 3.06160254e+02, 3.06554934e+02,
           2.49934633e+02, 3.04811768e+02, 2.59477620e+02, 8.73933713e+01,
           2.39579053e+02, 1.55123458e+02, 2.78712115e+02, 6.76194920e+01,
           4.28106863e+01, 5.53615353e+01, 6.21746386e+01, 3.22296386e+02,
           1.04402753e+02, 1.08170742e+02, 7.22102869e+01, 2.57092544e+01,
           1.32010159e+02, 1.49889048e+02, 4.72994164e+01, 4.70061574e+01,
           1.10187060e+02, 2.12658109e+01, 1.98029347e+02, 1.01741710e+01,
           8.91514088e+00, 2.26135966e+02, 1.24000802e+02, 3.61713327e+01,
           3.07545806e+01, 2.29542055e+02, 2.69634148e+02, 2.24335452e+02,
           1.10470228e+01, 5.90469310e+00, 1.50225964e+02, 3.43673205e+02,
           1.17391764e+02, 3.32250342e+02, 1.16756760e+02, 3.40188144e+02,
           1.88385096e+02, 1.05712340e+02, 2.67261647e+02, 1.53176879e+02,
           2.67016230e+02, 2.51332465e+02, 1.39735251e+02, 2.68580398e+02,
           1.32127800e+02, 2.34623586e+02, 1.82927731e+02, 3.69263995e+02,
           4.15822223e+01, 4.83214916e+01, 3.45193088e+02, 1.92823869e+02,
           3.75633691e+02, 3.42384933e+02, 1.70637809e+02, 3.53835698e+02,
           1.21189181e+01, 6.83773817e+01, 3.11057494e+02, 3.09173445e+02,
           2.46509921e+01, 3.41169163e+01, 1.77312917e+02, 2.81197111e+01,
           1.81034068e+02, 2.08092339e+02, 3.34507530e+02, 8.85378648e+01,
           1.58919196e+02, 1.60215478e+02, 3.22261706e+02, 5.41605141e+01,
           6.09868285e+01, 3.63256749e+02, 1.43112693e+02, 3.22991851e+02,
           1.02135340e+02, 9.65289203e+01, 2.77745180e+02, 8.62675938e+00,
           1.12632411e+02, 1.04609924e+00, 2.78918587e+01, 3.70045259e+02,
           1.04929195e+02, 1.42974757e+02, 3.92884397e+02, 2.39894424e+02,
           2.66416069e+01, 2.01693611e+02, 9.37625161e+01, 2.93299771e+02,
           1.98157598e+02, 4.30453681e+01, 1.29325509e+02, 2.54718703e+02,
           2.09630706e+02, 2.68726182e+02, 3.09545380e+02, 1.10910871e+02,
           3.59536285e+01, 1.70371519e+02, 3.52765470e+02, 8.41263817e+01,
           3.53617295e+02, 2.55899169e+01, 1.28315244e+02, 1.42908130e+02,
           1.45043452e+02, 9.48513164e+01, 3.84333588e+02, 4.38339055e+02,
           2.92661085e+02, 4.11859574e+01, 4.36832134e+02, 1.37215787e+02,
           4.09293547e+02, 2.88772859e+02, 2.88350845e+02, 2.53339061e+02,
           3.01960170e+02, 1.88932076e+00, 3.20989292e+02, 7.94234191e+01,
           1.56605557e+02, 1.80064183e+02, 2.06476001e+02, 3.64941099e+02,
           5.48938636e+01, 2.42075330e+01, 1.60014407e+02, 1.21557613e+02,
           7.08488148e+01, 6.31426755e-01, 4.93867240e+01, 4.36829033e+02,
           2.80497662e+02, 4.20943871e+02, 1.49186311e+02, 1.01428192e+02,
           2.44394374e+02, 1.76948222e+02, 3.34667960e+02, 3.15193951e+02,
           1.42178574e+02, 1.13325148e+02, 1.79703071e+02, 3.49495404e+02,
           3.02108768e+02, 1.81369441e+02, 7.92820817e+01, 2.12654872e+02,
           2.69749155e+02, 3.12779414e+02, 3.60147530e+02, 3.77066166e+02,
           1.81025083e+02, 2.94084231e+02, 9.43637105e+01, 2.98551221e+02,
           9.53804818e+01, 1.51748147e+01, 2.77400534e+02, 1.90729090e+02,
           1.25909412e+02, 1.38222529e+01, 3.17988500e+02, 4.37304311e+02,
           3.64199247e+02, 1.76572957e+02, 4.01251590e+02, 4.77062041e+02,
           2.45376043e+02, 1.00098511e+01, 1.36426644e+02, 1.59495337e+02,
           3.15697370e+02, 4.96757763e+02, 2.18354116e+02, 3.39638104e+01,
           2.07554327e+02, 3.97145803e+02, 2.67046233e+02, 4.56778138e+00,
           9.18573469e+01, 8.46722295e+01, 5.18079364e+02, 5.03893843e+02,
           9.23397188e+01, 1.67237150e+01, 4.73075463e+02, 4.91768855e+02,
           4.40158488e+01, 7.45549642e+01, 4.86262666e+02, 6.73121932e+00,
           3.76499365e+02, 3.09741009e+02, 1.87690841e+02, 4.29655146e+02,
           4.46991190e+01, 9.35623697e+01, 3.52494204e+02, 4.31115718e+02,
           3.41219156e+02, 1.44557539e+02, 1.57669465e+02, 4.05362759e+02,
           3.64008013e+02, 4.03803538e+02, 3.02810019e+02, 1.80701526e+02,
           5.04768535e+02, 3.96411554e+02, 2.24067099e+02, 2.45541033e+02,
           2.38821459e+02, 3.72282728e+02, 1.37938983e+02, 8.77798318e+01,
           1.99510780e+02, 4.36171855e+02, 3.12277445e+02, 3.04518891e+02,
           4.80316588e+02, 4.49141742e+02, 3.46582282e+02, 1.85090802e+02,
           1.27668931e+02, 5.31355816e+02, 2.62592018e+02, 3.73396603e+02,
           1.33042181e+02, 1.54614803e+02, 5.07227480e+02, 1.37276346e+02,
           4.57847675e+01, 5.22027913e+02, 1.04840892e+02, 2.35349585e+01,
           4.65161564e+02, 5.62762198e+01, 3.44627882e+02, 1.21762298e+02,
           3.62714199e+02, 1.39825983e+02, 1.20819938e+00, 4.25017445e+02,
           2.14179507e+02, 8.79531468e+01, 5.66686918e+02, 9.20655042e+01,
           2.97754415e+02, 1.48478243e+02, 3.49597243e+02, 5.20883313e+02,
           1.35646432e+01, 3.71837592e+02, 1.18772902e+02, 2.36113791e+02,
           4.58529840e+02, 6.73527135e+01, 2.82729179e+02, 2.96045308e+02,
           1.15996419e+02, 1.96155831e+02, 2.57813050e+02, 3.14738493e+02,
           4.39627785e+02, 2.00616773e-01, 5.42753852e+01, 1.61760079e+02,
           6.20652595e+00, 3.72343852e+02, 4.43651639e+02, 4.43488754e+02,
           3.44955765e+02, 4.20670689e+02, 2.28508651e+00, 1.24089984e+02,
           1.14063806e+02, 5.67491771e+02, 5.52848930e+02, 3.08424409e+02,
           5.67424547e+02, 2.97395097e+02, 3.06310609e+02, 6.11320355e+01,
           3.56829514e+02, 2.13343357e+02, 6.16969898e+02, 2.74555048e+02,
           5.08370856e+02, 2.42612908e+02, 2.51159944e+02, 8.01522366e+01,
           3.64508536e+02, 2.89169860e+02, 2.72730565e+02, 9.29245978e+00,
           4.23626198e+02, 6.29030084e+02, 9.82009630e+01, 3.92260610e+02,
           1.52732337e+02, 4.80627131e+02, 5.59055875e+02, 1.50459590e+01,
           4.66634951e+02, 5.38617210e+02, 6.36401242e+02, 1.76811700e+02,
           3.46685773e+02, 2.41109355e+02, 5.29682701e+02, 2.94948483e+01,
           2.57361909e+02, 3.20210008e+01, 3.96487199e+02, 2.45742393e+01,
           4.89847524e+02, 5.34918209e+01, 2.07794515e+01, 6.06511354e+02,
           2.96681625e+02, 5.19217419e+02, 3.63955968e+02, 7.07430955e+01,
           4.32505080e+02, 5.65971324e+02, 3.87003008e+02, 1.49038452e+01,
           4.27377727e+01, 1.16451770e+02, 2.19940808e+02, 3.73373507e+02,
           3.99259321e+02, 5.12432934e+02, 8.41446179e-01, 3.33736701e+02,
           9.93204452e+01, 6.56097689e+02, 6.67331921e+02, 6.11547450e+02,
           4.98977981e+02, 1.45192200e+02, 5.46576809e+02, 5.39049976e+02,
           6.71206836e+02, 5.07530023e+02, 4.14782321e+02, 3.51352515e+02,
           2.16613817e+02, 6.04133994e+02, 1.64828398e+02, 5.94262923e+01,
           1.80412394e+02, 4.36450758e+02, 1.63896923e+02, 2.48566826e+02,
           2.31031761e+02, 4.67924050e+02, 3.61723240e+01, 6.24961048e+02,
           4.52407764e+02, 6.04800678e+02, 9.04278577e+01, 3.45604993e+02,
           1.89833165e+01, 5.84039408e+02, 2.26550491e+02, 3.52541179e+02,
           2.48451589e+02, 3.95020378e+02, 1.49906035e+02, 3.40924071e+02,
           2.22227077e+02, 1.61958266e+02, 6.10131768e+02, 5.96941359e+02,
           3.15674676e+02, 2.21827760e+02, 1.44569380e+02, 5.13712548e+02,
           6.09376274e+02, 6.64582548e+02, 1.02554638e+02, 6.22326480e+02,
           6.92587927e+02, 8.64015383e+01, 3.44690588e+01, 2.36458402e+01,
           1.68598195e+02, 5.74504504e+02, 3.81886379e+02, 4.86398899e+02,
           6.27898793e+02, 8.78888617e+01, 4.79638270e+02, 1.58247608e+01,
           5.37431001e+02, 4.08923762e+02, 7.10151757e+02, 2.07099036e+02,
           2.77566761e+02, 5.38420278e+01, 2.76688898e+02, 3.84106371e+01,
           7.26845245e+02, 4.83887523e+02, 3.51297324e+02, 6.22431663e+02,
           3.36306213e+02, 3.35342125e+02, 5.36285309e+02, 1.83702609e+02,
           2.33045853e+02, 5.00403139e+02, 5.54839319e+02, 9.83075672e+01,
           2.88023852e+02, 1.89823288e+02, 4.66553171e+02, 1.67254178e+01,
           5.50350001e+02, 7.22144994e+02, 3.24371357e+02, 5.84445204e+02,
           2.35748686e+02, 6.28156163e+01, 1.38236687e+02, 1.46846935e+01,
           1.94881999e+02, 3.76226434e+02, 3.70260376e+02, 7.33229993e+02,
           7.01616046e+02, 8.66230310e+01, 5.87529766e+02, 4.40106159e+02,
           7.70042923e+02, 5.69939146e+02, 5.74312806e+02, 5.51098497e+02,
           4.08097153e+02, 3.46662570e+02, 4.59042184e+02, 7.79520108e+02,
           5.91146878e+02, 1.43543824e+02, 4.56122112e+02, 6.88320111e+02,
           6.53683873e+02, 3.68565744e+02, 2.37219334e+02, 4.26604017e+02,
           4.71013333e+02, 6.78689655e+02, 6.87919758e+02, 3.33510344e+01,
           1.54811496e+02, 8.33999011e+01, 3.43016957e+02, 1.18291343e+02,
           1.31271116e+02, 7.16631371e+02, 3.22731369e+02, 2.09670624e+02,
           3.28602159e+02, 3.60568710e+02, 1.48314825e+02, 4.26721411e+02,
           7.17272879e+02, 3.87395560e+02, 5.60502061e+02, 7.30915999e+02,
           6.72004426e+02, 5.13472654e+02, 6.37539738e+01, 6.09886108e+02,
           5.91905626e+02, 1.44473473e+02, 5.03499194e+01, 6.41239952e+02,
           7.37328197e+02, 3.64252636e+02, 4.91374124e+02, 2.44205407e+02,
           6.15697459e+02, 6.97138826e+02, 4.52844072e+01, 2.37789228e+02,
           2.74286915e+02, 7.36314026e+02, 5.09085775e+02, 1.37583193e+02,
           7.44738280e+02, 6.08083815e+02, 4.51486290e+02, 3.28065363e+01,
           2.55350148e+02, 3.64036315e+02, 6.70112106e+01, 7.97487331e+02,
           7.89204631e+02, 7.68437430e+02, 1.34367571e+02, 6.00039557e+02,
           3.38926208e+02, 4.98944754e+02, 5.42492626e+00, 4.80649880e+02,
           1.15047013e+02, 1.88685845e+02, 6.92267056e+02, 7.85123683e+02,
           6.68412942e+02, 2.51894302e+02, 1.87273605e+02, 4.74508587e+02,
           4.66341336e+02, 4.24225596e+02, 7.78723031e+02, 3.24407291e+02,
           7.23226158e+01, 1.45635058e+02, 6.97940745e+02, 5.75772080e+02,
           6.84896573e+02, 3.89376821e+02, 6.21380476e+02, 5.61326882e+02,
           3.78748437e+02, 3.91787168e+02, 6.69381007e+02, 5.72068784e+02,
           1.47296423e+02, 6.57735989e+02, 1.36771354e+02, 2.35550852e+02,
           8.12518291e+02, 5.80032836e+02, 2.64725032e+02, 5.18226744e+02,
           4.30914680e+02, 2.71052471e+02, 5.55855110e+02, 5.90725530e+02,
           6.83516981e+02, 7.57177054e+02, 1.98369048e+02, 4.28605431e+02,
           2.56156854e+02, 7.64884008e+02, 4.15204760e+02, 8.05112810e+02,
           8.39370601e+02, 7.85576591e+02, 7.70153195e+02, 4.61533278e+02,
           5.37505268e+01, 4.40147163e+02, 5.17406119e+02, 3.80256026e+02,
           2.75125323e+02, 5.04216891e+02, 7.00800027e+02, 4.39788408e+02,
           2.46884122e+02, 6.38512226e+02, 1.66823211e+02, 6.54742492e+02,
           5.36634640e+02, 9.13891240e+01, 2.01382026e+02, 6.43868346e+02,
           3.80765831e+02, 8.67669060e+02, 5.27952368e+02, 7.25211261e+01,
           5.32769487e+02, 4.20812074e+02, 3.62568004e+02, 2.99405998e+02,
           2.62450727e+02, 7.46023034e+02, 7.12370119e+02, 5.66580125e+02,
           8.22754002e+02, 5.14534359e+02, 6.35229565e+02, 3.01507776e+01,
           4.74254131e+02, 4.58112277e+02, 8.78092353e+02, 9.11089280e+02,
           8.56608812e+02, 8.17481323e+01, 3.64132684e+02, 4.23772943e+01,
           2.59227599e+02, 5.56990206e+02, 2.83379532e+02, 1.78134907e+01,
           8.05271597e+02, 3.82939852e+02, 1.19865093e+02, 6.83630450e+02,
           5.55589490e+02, 1.79732270e+02, 6.50410442e+02, 7.74073599e+02,
           8.14561248e+02, 1.76236939e+02, 3.61665636e+00, 4.92391615e+02,
           3.55126853e+02, 9.83796683e+01, 5.95182099e+02, 1.29488699e+02,
           6.59660686e+01, 4.94671679e+02, 3.31158717e+02, 2.52942030e+02,
           9.69133014e+00, 5.35655564e+02, 7.17580603e+02, 6.13383079e+02,
           7.01201531e+02, 6.58938804e+02, 3.41308621e+02, 3.69543615e+01,
           6.15455398e+02, 5.11671374e+02, 2.42657929e+02, 7.43897457e+02,
           8.87075577e+02, 6.38889488e+02, 9.03219925e+02, 9.40881859e+01,
           1.05518774e+02, 8.70068563e+02, 5.81686936e+02, 2.86452469e+02,
           5.22433318e+01, 2.36912282e+02, 5.78747513e+02, 9.40345955e+02,
           5.62525027e+02, 2.31020997e+02, 8.07428822e+02, 6.39053970e+02,
           2.12280014e+02, 3.51902968e+02, 6.02837176e+02, 9.04610225e+02,
           1.75162453e+02, 3.15108246e+02, 3.94628060e+02, 5.85057109e+02,
           5.58445810e+02, 9.90387643e+01, 9.78021053e+02, 2.31091546e+02,
           7.21365917e+02, 1.32391506e+02, 6.59801841e+02, 7.96115612e+02])



We now have sales prices for 1000 items currently for sale at BuyStuff. Now create an RDD called `price_items` using the newly created data with 10 slices. After you create it, use one of the basic actions to see what's in the RDD.


```python
price_items = sc.parallelize(sales_figures, numSlices=10)
price_items.count()

```




    1000



Now let's perform some operations on this simple dataset. To begin with, create a function that will take into account how much money BuyStuff will receive after sales tax has been applied (assume a sales tax of 8%). To make this happen, create a function called `sales_tax()` that returns the amount of money our company will receive after the sales tax has been applied. The function will have this parameter:

* `item`: (float) number to be multiplied by the sales tax.


Apply that function to the rdd by using the `.map()` method and assign it to a variable `revenue_minus_tax`


```python
def sales_tax(num):
    return num * 0.92

revenue_minus_tax = price_items.map(sales_tax)
```

Remember, Spark has __lazy evaluation__, which means that the `sales_tax()` function is a transformer that is not executed until you call an action. Use one of the collection methods to execute the transformer now a part of the RDD and observe the contents of the `revenue_minus_tax` rdd.


```python
revenue_minus_tax.top(10)

```




    [899.7793686291817,
     865.1182781915198,
     838.2021374105992,
     832.2414065768047,
     830.9623312417458,
     816.1095311824387,
     807.8449645685038,
     800.4630780956381,
     798.2555352900424,
     788.0801074993623]



### Lambda Functions

Note that you can also use lambda functions if you want to quickly perform simple operations on data without creating a function. Let's assume that BuyStuff has also decided to offer a 10% discount on all of their items on the pre-tax amounts of each item. Use a lambda function within a `.map()` method to apply the additional 10% loss in revenue for BuyStuff and assign the transformed RDD to a new RDD called `discounted`.


```python
discounted = price_items.map(lambda x: x* 0.9)
```


```python
discounted.take(10)
```




    [0.8097648605398251,
     1.1075252901037431,
     1.3941267501929895,
     3.42682336514889,
     2.114138274005914,
     0.8301694883733693,
     0.5191859090995876,
     5.4840284260151435,
     3.0215007990749867,
     2.8682404548717497]



## Chaining Methods

You are also able to chain methods together with Spark. In one line, remove the tax and discount from the revenue of BuyStuff and use a collection method to see the 15 costliest items.


```python
price_items.map(sales_tax).top(15)
```




    [899.7793686291817,
     865.1182781915198,
     838.2021374105992,
     832.2414065768047,
     830.9623312417458,
     816.1095311824387,
     807.8449645685038,
     800.4630780956381,
     798.2555352900424,
     788.0801074993623,
     772.220953307471,
     756.9336816644519,
     749.3963479960872,
     747.5168275987348,
     742.8345162138526]



## RDD Lineage


We are able to see the full lineage of all the operations that have been performed on an RDD by using the `RDD.toDebugString()` method. As your transformations become more complex, you are encouraged to call this method to get a better understanding of the dependencies between RDDs. Try calling it on the `discounted` RDD to see what RDDs it is dependent on.


```python
discounted.toDebugString()
```




    b'(10) PythonRDD[16] at RDD at PythonRDD.scala:53 []\n |   ParallelCollectionRDD[5] at parallelize at PythonRDD.scala:195 []'



### Map vs. Flatmap

Depending on how you want your data to be outputted, you might want to use `.flatMap()` rather than a simple `.map()`. Let's take a look at how it performs operations versus the standard map. Let's say we wanted to maintain the original amount BuyStuff receives for each item as well as the new amount after the tax and discount are applied. Create a map function that will return a tuple with (original price, post-discount price).


```python
mapped =price_items.map(lambda x :(x, sales_tax(x)*.9))
print(mapped.count())
print(mapped.take(10))
```

    1000
    [(0.899738733933139, 0.744983671696639), (1.2305836556708256, 1.0189232668954438), (1.549029722436655, 1.2825966101775503), (3.8075815168320997, 3.1526774959369788), (2.3490425266732378, 1.9450072120854411), (0.922410542637077, 0.7637559293034998), (0.5768732323328751, 0.4776510363716206), (6.093364917794604, 5.045306151933933), (3.3572231100833183, 2.7797807351489876), (3.1869338387463886, 2.63878121848201)]
    

Note that we have 1000 tuples created to our specification. Let's take a look at how `.flatMap()` differs in its implementation. Use the `.flatMap()` method with the same function you created above.


```python
flat_mapped = price_items.flatMap(lambda x :(x, sales_tax(x)*.9))
print(flat_mapped.count())
print(flat_mapped.take(10))
```

    2000
    [0.899738733933139, 0.744983671696639, 1.2305836556708256, 1.0189232668954438, 1.549029722436655, 1.2825966101775503, 3.8075815168320997, 3.1526774959369788, 2.3490425266732378, 1.9450072120854411]
    

Rather than being represented by tuples, all of the  values are now on the same level. When we are trying to combine different items together, it is sometimes necessary to use `.flatMap()` rather than `.map()` in order to properly reduce to our specifications. This is not one of those instances, but in the upcoming lab, you just might have to use it.

## Filter
After meeting with some external consultants, BuyStuff has determined that its business will be more profitable if it focuses on higher ticket items. Now, use the `.filter()` method to select items that bring in more than $300 after tax and discount have been removed. A filter method is a specialized form of a map function that only returns the items that match a certain criterion. In the cell below:
* use a lambda function within a `.filter()` method to meet the consultant's suggestion's specifications. set `RDD = selected_items`
* calculate the total number of items remaining in BuyStuff's inventory


```python
# use the filter function
selected_items = price_items.filter(lambda x: sales_tax(x)*.90>300)

# calculate total remaining in inventory 
selected_items.count()
```




    270



## Reduce

Reduce functions are where you are in some way combing all of the variables that you have mapped out. Here is an example of how a reduce function works when the task is to sum all values:

<img src = "./images/reduce_function.png" width = "600">  


As you can see, the operation is performed within each partition first, after which, the results of the computations in each partition are combined to come up with one final answer.  

Now it's time to figure out how much money BuyStuff would make from selling one of all of its items after they've reduced their inventory. Use the `.reduce()` method with a lambda function to add up all of the values in the RDD. Your lambda function should have two variables. 


```python
selected_items.reduce(lambda x, y: x+ y)
```




    150745.12468034515



The time has come for BuyStuff to open up shop and start selling its goods. It only has one of each item, but it's allowing 50 lucky users to buy as many items as they want while they remain in stock. Within seconds, BuyStuff is sold out. Below, you'll find the sales data in an RDD with tuples of (user, item bought).


```python
import random
random.seed(42)
# generating simulated users that have bought each item
sales_data = selected_items.map(lambda x: (random.randint(1, 50), x))

sales_data.take(7)
```




    [(25, 369.2639950128622),
     (5, 375.63369072837236),
     (7, 363.25674899916373),
     (48, 370.04525873873865),
     (25, 392.8843974783909),
     (9, 384.3335883717267),
     (34, 438.3390550360844)]



It's time to determine some basic statistics about BuyStuff users.

Let's start off by creating an RDD that determines how much each user spent in total.
To do this we can use a method called `.reduceByKey()` to perform reducing operations while grouping by keys. After you have calculated the total, use the `.sortBy()` method on the RDD to rank the users from the highest spending to the least spending. 


```python
# calculate how much each user spent
total_spent = sales_data.reduceByKey(lambda x, y: x + y)
total_spent.take(10)
```




    [(50, 2934.231577812499),
     (20, 3514.5554698060832),
     (30, 477.06204113742257),
     (10, 1949.6530710880745),
     (40, 2499.9182491880865),
     (31, 4347.064807731983),
     (1, 4679.174891319223),
     (11, 3803.509573181673),
     (41, 2780.507487908285),
     (21, 1161.662882261354)]




```python
# sort the users from highest to lowest spenders
total_spent.sortBy(lambda x: x[1],ascending = False).collect()
```




    [(5, 7228.255084173294),
     (17, 5769.601316181468),
     (18, 5125.423353055746),
     (39, 4931.280390372646),
     (9, 4766.1262396921),
     (1, 4679.174891319223),
     (19, 4678.396754104098),
     (25, 4400.725531204281),
     (31, 4347.064807731983),
     (29, 4163.364540639334),
     (45, 3958.346392527314),
     (49, 3883.0942731418877),
     (33, 3852.1929828474504),
     (42, 3825.274159207469),
     (11, 3803.509573181673),
     (26, 3689.354018176904),
     (7, 3671.746270411086),
     (2, 3536.8334633024856),
     (20, 3514.5554698060832),
     (3, 3390.6278898679275),
     (12, 3316.7225629713157),
     (36, 3198.9051615550898),
     (27, 3024.3531221292524),
     (50, 2934.231577812499),
     (37, 2890.487730250892),
     (43, 2873.966620090375),
     (4, 2826.6285679999046),
     (41, 2780.507487908285),
     (44, 2682.722026434513),
     (15, 2668.1322328042356),
     (6, 2629.6993546490075),
     (40, 2499.9182491880865),
     (23, 2491.9210097145065),
     (47, 2479.519339578107),
     (13, 2365.2295136942453),
     (14, 2220.799071865717),
     (22, 2180.146300561363),
     (28, 2164.689436521778),
     (10, 1949.6530710880745),
     (46, 1919.1950662775876),
     (34, 1917.0962062374956),
     (38, 1793.0559914196515),
     (8, 1736.8962047120208),
     (24, 1216.6922825292063),
     (21, 1161.662882261354),
     (35, 1104.808092320392),
     (48, 819.488414777564),
     (32, 744.45438316227),
     (30, 477.06204113742257),
     (16, 461.5332777485037)]



Next, let's determine how many items were bought per user. This can be solved in one line using an RDD method. After you've counted the total number of items bought per person, sort the users from most number of items bought to least number of items. Time to start a customer loyalty program!


```python
total_items = sales_data.countByKey()
sorted(total_items.items(),key=lambda kv:kv[1],reverse=True)
```




    defaultdict(int,
                {12: 6,
                 10: 5,
                 45: 6,
                 30: 8,
                 28: 7,
                 1: 6,
                 4: 7,
                 39: 8,
                 44: 5,
                 32: 9,
                 19: 8,
                 27: 7,
                 35: 6,
                 21: 5,
                 26: 10,
                 2: 5,
                 38: 4,
                 31: 8,
                 13: 13,
                 14: 3,
                 41: 3,
                 7: 6,
                 9: 5,
                 33: 6,
                 50: 9,
                 47: 6,
                 34: 4,
                 18: 9,
                 16: 5,
                 43: 4,
                 15: 5,
                 48: 1,
                 22: 5,
                 42: 3,
                 11: 3,
                 24: 5,
                 36: 5,
                 40: 6,
                 37: 5,
                 49: 6,
                 25: 4,
                 20: 3,
                 29: 4,
                 46: 2,
                 17: 2,
                 8: 4,
                 3: 4,
                 5: 4,
                 6: 4,
                 23: 2})



### Additional Reading

- [The original paper on RDDs](https://cs.stanford.edu/~matei/papers/2012/nsdi_spark.pdf)
- [RDDs in Apache Spark](https://data-flair.training/blogs/create-rdds-in-apache-spark/)
- [Programming with RDDs](https://runawayhorse001.github.io/LearningApacheSpark/rdd.html)
- [RDD Transformations and Actions Summary](https://www.analyticsvidhya.com/blog/2016/10/using-pyspark-to-perform-transformations-and-actions-on-rdd/)

## Summary

In this lab we went through a brief introduction to RDD creation from a Python collection, setting a number of logical partitions for an RDD and extracting lineage. We also used transformations and actions to perform calculations across RDDs on a distributed setup. In the next lab, you'll get the chance to apply these transformations on different books to calculate word counts and various statistics.

