# BDA-CaseStudy
Product Review Analytics and Rating Aggregation


```markdown
# MapReduce Product & Customer Analytics System

A scalable, distributed Hadoop MapReduce application written in Java to analyze large multi-file e-commerce customer review datasets (`Amazon_Reviews.csv`).

## Features
- **Average Rating & Review Count Analysis**: Computes mean star ratings and total review counts grouped by customer region/country.
- **Rating Distribution Breakdown**: Measures star-rating frequencies (1-star to 5-star breakdown) per region/country.
- **Robust Field Parsing**: Utilizes custom regular expressions to parse CSV fields containing complex quotes, embedded commas, and multiline text.
- **Multi-File & Single-File Processing**: Dynamically scales across single CSV files or entire directory trees containing multiple datasets on HDFS.

---

## Dataset Schema (`Amazon_Reviews.csv`)
The MapReduce job expects a standard 9-column CSV structure:
- `Reviewer Name` (Col 0)
- `Profile Link` (Col 1)
- `Country` (Col 2)
- `Review Count` (Col 3)
- `Review Date` (Col 4)
- `Rating` (Col 5)
- `Review Title` (Col 6)
- `Review Text` (Col 7)
- `Date of Experience` (Col 8)

---

## Project Structure
```text
product_analytics/
├── ProductAnalytics.java   # MapReduce Mapper, Reducer, and Driver source code
├── product-analytics.jar   # Compiled MapReduce JAR executable
├── Amazon_Reviews.csv      # Sample input dataset
└── README.md               # Documentation

```

---

## Prerequisites & Dependencies

* Java OpenJDK 8 or 11
* Apache Hadoop 3.x cluster (Single-node or Multi-node)
* Environment variable `HADOOP_HOME` configured

---

## Execution Guide

### 1. Build Executable JAR

```bash
export CLASSPATH=$(hadoop classpath)
javac -classpath $CLASSPATH -d . ProductAnalytics.java
jar -cvf product-analytics.jar *.class

```

### 2. Upload Input Data to HDFS

```bash
hdfs dfs -mkdir -p /user/student278/venkat/input
hdfs dfs -put -f Amazon_Reviews.csv /user/student278/venkat/input/

```

### 3. Execute MapReduce Jobs

#### Job 1: Average Rating & Review Counts

```bash
hdfs dfs -rm -r -skipTrash /user/student278/venkat/output_country_rating

hadoop jar product-analytics.jar ProductAnalytics rating \
  /user/student278/venkat/input/Amazon_Reviews.csv \
  /user/student278/venkat/output_country_rating

```

#### Job 2: Rating Star Distribution

```bash
hdfs dfs -rm -r -skipTrash /user/student278/venkat/output_country_distribution

hadoop jar product-analytics.jar ProductAnalytics distribution \
  /user/student278/venkat/input/Amazon_Reviews.csv \
  /user/student278/venkat/output_country_distribution

```

### 4. View Results

```bash
hdfs dfs -cat /user/student278/venkat/output_country_rating/part-r-00000 | head -n 25
hdfs dfs -cat /user/student278/venkat/output_country_distribution/part-r-00000 | head -n 25

```

```

```
