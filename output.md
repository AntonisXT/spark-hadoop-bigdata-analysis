# 📑 Output Results

This file contains the execution results of all queries (Q1–Q5) using:
- **RDD API**
- **Spark SQL (Parquet & CSV)**

The results are exactly as obtained from the VM.

---

## 🔹 RDD Queries

### Q1
```
Apache : 10431
nginx : 9745
Microsoft-IIS : 3185
cloudflare-nginx : 2782
GSE : 2086
```

### Q2
```
Target url:  http://1001.ru/articles/post/ai-da-tumin-443  
Metadata Length:  1136  
Html size:  2010
```

### Q3
```
Warc Record ID: <urn:uuid:f4e3cb63-9268-4e94-a111-52bd65a6d198>  - Target URL: http://joeguide.com/summaries/  - Content Length: 1048973
Warc Record ID: <urn:uuid:9d84df55-fa48-4979-9740-9fcb51ed0493>  - Target URL: http://hotwheels.wikia.com/wiki/File:Mattel-brand.svg.png  - Content Length: 1048984
Warc Record ID: <urn:uuid:eb849dfe-a760-4e5d-b485-60112517037e>  - Target URL: http://de.tube8.com/amateur/adorable-brunette-masturbating-using-a-cucumber/3475551/  - Content Length: 1049044
Warc Record ID: <urn:uuid:5dc1b6d2-ffbf-400c-b7f0-0c31fe585094>  - Target URL: http://glmris.anl.gov/documents/docs/GLMRIS_Brandon_Rd_Scoping_Presentation.pdf  - Content Length: 1049058
Warc Record ID: <urn:uuid:acf04d8c-abe0-41db-9f5e-3644c9fe7f74>  - Target URL: http://k36yb-ctump.4rumer.com/t417-topic  - Content Length: 1253837
```

### Q4
```
Server: Powered by Sloths, Avg Warc Content Length: 800012.0, Avg Metadata Content Length: 70419.0
Server: Unyil, Avg Warc Content Length: 800012.0, Avg Metadata Content Length: 35190.0
Server: 172.17.99.36:443, Avg Warc Content Length: 527518.0, Avg Metadata Content Length: 1104.0
Server: bWFkaXNvbg==, Avg Warc Content Length: 404891.3333333333, Avg Metadata Content Length: 6395.0
Server: mw1263.eqiad.wmnet, Avg Warc Content Length: 365172.3333333333, Avg Metadata Content Length: 7417.666666666667
```

### Q5
```
Most popular URL: http://football-pr.com - Occurrences: 285
```

---

## 🔹 DataFrames / Spark SQL (Parquet & CSV)

### Q1
```
+----------------+-----------+
|          server|usage_count|
+----------------+-----------+
|          Apache|      10536|
|           nginx|       9850|
|   Microsoft-IIS|       3216|
|cloudflare-nginx|       2809|
|             GSE|       2117|
+----------------+-----------+
```

### Q2
```
+--------------------+---------+---------------+
|                  id|len(html)|metadata_length|
+--------------------+---------+---------------+
|<urn:uuid:7327967...|   200811|           1136|
+--------------------+---------+---------------+
```

### Q3
```
+--------------------+--------------------+-------+
|                  id|                 url| length|
+--------------------+--------------------+-------+
|<urn:uuid:838e642...|http://keyporntub...|1048973|
|<urn:uuid:9d84df5...|http://hotwheels....|1048984|
|<urn:uuid:eb849df...|http://de.tube8.c...|1049044|
|<urn:uuid:5dc1b6d...|http://glmris.anl...|1049058|
|<urn:uuid:acf04d8...|http://k36yb-ctum...|1253837|
+--------------------+--------------------+-------+
```

### Q4
```
+------------------+-----------------+-----------------+
|            server|  avg_warc_length|   avg_wat_length|
+------------------+-----------------+-----------------+
| Powered by Sloths|         800012.0|          70419.0|
|             Unyil|         800012.0|          35190.0|
|  172.17.99.36:443|         527518.0|           1104.0|
|      bWFkaXNvbg==|404891.3333333333|           6395.0|
|mw1263.eqiad.wmnet|365172.3333333333|7417.666666666667|
+------------------+-----------------+-----------------+
```

### Q5
```
+-----------------+---------+
|       target_url|url_count|
+-----------------+---------+
|http://schema.org|    11057|
+-----------------+---------+
```

---
