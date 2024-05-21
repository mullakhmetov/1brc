The text file has a simple structure with one measurement value per row:
```
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
St. John's;15.2
Cracow;12.6
...
```

The program should print out the min, mean, and max values per station, alphabetically ordered like so:
```
{Abha=5.0/18.0/27.4, Abidjan=15.7/26.0/34.1, Abéché=12.1/29.4/35.6, Accra=14.7/26.4/33.1, Addis Ababa=2.1/16.0/24.3, Adelaide=4.1/17.3/29.7, ...}
```

### Results
- naive linear city search (baseline) 7:09.52 total
- hashmap with linear probing 1:12.65 total
- mmap I/O + parallel workers 58.540 total
- localized thread results 23.650 total
- store city name location 16.403 total

### TODOs
- [x] hashmap search
- [x] proper output format {Abha=x/y/z, ...}
- [x] mmap
- [x] parallelize I/O
- [x] custom float parsing
- [x] profiling
- [x] localize thread results
- [x] avoid strcpy in hot loop
