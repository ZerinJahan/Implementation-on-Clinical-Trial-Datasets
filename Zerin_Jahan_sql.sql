-- Databricks notebook source
CREATE TABLE clinicaltrial_2021
USING csv
OPTIONS (
  path "/FileStore/tables/clinicaltrial_2021.csv/clinicaltrial_2021.csv",
  header 'true',
  inferSchema 'true',
  delimiter '|'
)

-- COMMAND ----------

select * from clinicaltrial_2021 

-- COMMAND ----------

--ques 1: The number of studies in the dataset
select count(distinct Id) from clinicaltrial_2021


-- COMMAND ----------

--ques2: Types of studies and their frequencies
select Type, COUNT(*) AS Total_no
from clinicaltrial_2021
group by Type
order by Total_no desc;

-- COMMAND ----------

--ques 3: Top 5 condition with their frequency
select TRIM(Condition) as Condition, COUNT(*) as Total_no
from (
  select explode(SPLIT(Conditions, ',')) as Condition
  from clinicaltrial_2021
) as tmp
group by TRIM(Condition)
order by Total_no desc limit 5  

-- COMMAND ----------


select Status, Completion from clinicaltrial_2021

-- COMMAND ----------

-- Ques no 5 was done before ques no 4 because in 4 the Pharma.csv file needed to extract. After ques 5 the the ques 4 was solved
--ques 5: Plot number of completed studies each month in a given year
SELECT 
  SUBSTR(Completion, 1, 3) AS month_abbr,
  COUNT(*) AS num_completed_studies
FROM clinicaltrial_2021
WHERE Status = 'Completed' AND Completion LIKE '%2021%'
GROUP BY month_abbr
ORDER BY 
  FIND_IN_SET(month_abbr, 'Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec');

-- COMMAND ----------

CREATE TABLE pharma
USING csv
OPTIONS (
  path "/FileStore/tables/pharma.csv/pharma.csv",
  header 'true',
  inferSchema 'true',
  delimiter ','
)

-- COMMAND ----------

select * from pharma 

-- COMMAND ----------

--ques 4: 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored
select c.Sponsor, COUNT(*) as No_of_Trial
from clinicaltrial_2021 c
where c.Sponsor not in (select Parent_company from pharma)
group by c.Sponsor
order by No_of_Trial desc
limit 10;

-- COMMAND ----------


