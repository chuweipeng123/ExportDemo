-- Databricks notebook source
SELECT *
FROM kythera_universe.mx_v13
LIMIT 3

-- COMMAND ----------

-- MAGIC %run Shared/Defaults

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Default = Defaults()

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC base_path_for_table = "/mnt/mx_v12_mount/wayfinder-procdna-s3-wayfinder-data/Exelixis_Kythera_Analysis/<folder_name_for_table>"

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC dbutils.fs.ls(base_path_for_table)

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS Exelixis_Universe

-- COMMAND ----------

-- Just for reference

CREATE TABLE kythera_universe.mx_v13(
service_from_date date,
service_number string,
service_to_date date,
statement_from_date date,
statement_to_date date,
total_claim_charge_amount_vendor string,
units_of_service decimal(38,2),
vendor_claim_number string,
vendor_national_drug_code string,
vendor_payer_id string,
vendor_payer_name string,
vendor_payer_state string,
vendor_received_date date,
vendor_type_coverage string,
vendorcompany string,
vendorfilemonth string,
vendorfilename string,
vendorname string,
patient_id string
)
USING PARQUET
     OPTIONS (
         'path' '/mnt/mx_v12_mount/wayfinder-procdna-s3-wayfinder-data/Exelixis_Kythera_Analysis/<folder_name_for_table>',
         'compression' 'snappy')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC help(Default)

-- COMMAND ----------

SELECT COUNT(DISTINCT patient_id) 
FROM kythera_universe.mx_v13
WHERE diagnosis_code_1 = 'C73' OR diagnosis_code_1 = '193' OR diagnosis_code_1 = 'C75' OR diagnosis_code_1 = 'Z85.85'

-- COMMAND ----------

SELECT patient_id
-- all patients (may contain duplicate) who use Vitrakvi once
FROM kythera_universe.mx_v13
WHERE national_drug_code = '5041939001' OR national_drug_code = '5041939101' OR national_drug_code = '5041939201' OR national_drug_code = '5041939302' OR national_drug_code = '7177739001' OR national_drug_code = '7177739101' OR national_drug_code = '7177739201'

-- COMMAND ----------

SELECT YEAR(service_from_date ) AS year, COUNT(DISTINCT patient_id) AS num_of_patients
FROM exelixis_universe.tc_patients
WHERE service_from_date IS NOT NULL AND diagnosis_code_1 IN ('C73', '193', 'C75', 'Z85.85')
GROUP BY YEAR(service_from_date)
ORDER BY YEAR(service_from_date)
-- YoY number of total patients where service_from_date is not null

-- COMMAND ----------

SELECT YEAR(service_from_date) AS year, COUNT(DISTINCT patient_id) AS num_of_new_patient
FROM exelixis_universe.tc_patients
WHERE (patient_id, service_from_date) IN 
(
  SELECT patient_id, MIN(service_from_date) AS first_diagnosis_date
  FROM exelixis_universe.tc_patients
  GROUP BY patient_id
  ORDER BY patient_id
)
GROUP BY YEAR(service_from_date)
ORDER BY YEAR(service_from_date)

-- COMMAND ----------

SELECT YEAR(service_from_date) AS year,diagnosis_code_1, COUNT(DISTINCT patient_id) AS num_of_new_patient
FROM exelixis_universe.tc_patients
WHERE (patient_id, service_from_dat+e) IN 
(
  SELECT patient_id, MIN(service_from_date) AS first_diagnosis_date
  FROM exelixis_universe.tc_patients
  GROUP BY patient_id
  ORDER BY patient_id
)
GROUP BY YEAR(service_from_date), diagnosis_code_1
ORDER BY YEAR(service_from_date)

-- COMMAND ----------

SET spark.sql.legacy.allowNonEmptyLocationInCTAS = true;

CREATE TABLE exelixis_universe.first_diagnosis_record
USING PARQUET
OPTIONS (
    'path' '/mnt/mx_v12_mount/wayfinder-procdna-s3-wayfinder-data/Exelixis_Kythera_Analysis/first_diagnosis_record',
    'compression' 'snappy'
)
AS
SELECT *
FROM exelixis_universe.tc_patients
WHERE (patient_id, service_from_date) IN 
(
  SELECT patient_id, MIN(service_from_date) AS first_diagnosis_date
  FROM exelixis_universe.tc_patients
  GROUP BY patient_id
  ORDER BY patient_id
)

-- COMMAND ----------



-- COMMAND ----------

SELECT YEAR(service_from_date) AS year, COUNT(DISTINCT patient_id) AS num_of_new_patients
FROM exelixis_universe.first_diagnosis_record
GROUP BY YEAR(service_from_date)
ORDER BY year

-- COMMAND ----------

SELECT procedure_code, COUNT(DISTINCT patient_id) AS num_of_patients
FROM exelixis_universe.first_diagnosis_record
GROUP BY procedure_code 
ORDER BY num_of_patients DESC

-- COMMAND ----------

SELECT YEAR(service_from_date) AS year, procedure_code, COUNT(DISTINCT patient_id)
FROM exelixis_universe.first_diagnosis_record
GROUP BY procedure_code, YEAR(service_from_date)
ORDER BY year, procedure_code

-- COMMAND ----------

SELECT patient_id, COUNT(*)
FROM exelixis_universe.first_diagnosis_record
GROUP BY patient_id



-- COMMAND ----------

SELECT *
FROM exelixis_universe.first_diagnosis_record
WHERE patient_id = 'f553dd30b4b989bb16c01cd1f8d5601145840a495db479139ee06295f6ab93adf83927b5d7e506489cd1f35c415a188555b42f25e8365c1a2fa6e47dde98a14c' AND procedure_code IS NULL

-- COMMAND ----------

SELECT *
FROM exelixis_universe.tc_patients
WHERE ndc_druginformation = 'VITRAKVI'

-- COMMAND ----------

SELECT *
FROM kythera_universe.rx_v13
WHERE national_drug_code = '5024221060' OR national_drug_code = '5024221083' OR national_drug_code = '5024221090'

-- COMMAND ----------

-- 6 months look back
SELECT diagnosis_code_2, COUNT(DISTINCT patient_id)
FROM kythera_universe.mx_v13
WHERE diagnosis_code_1 IN ('C750', 'C73', '193', 'Z85850')
GROUP BY 1


-- COMMAND ----------



-- COMMAND ----------

UPDATE exelixis_universe.tc_patients
SET first_diagnosis_flag = 
(
  SELECT CASE WHEN service_from_date = MIN(service_from_date) OVER (PARTITION BY patient_id) THEN 1 ELSE 0 END AS first_diagnosis
  FROM exelixis_universe.tc_patients
)

-- COMMAND ----------

CREATE TABLE exelixis_universe.tc_patients_delta
USING delta
OPTIONS (
    'path' '/mnt/mx_v12_mount/wayfinder-procdna-s3-wayfinder-data/Exelixis_Kythera_Analysis/tc_patients_delta',
    'compression' 'snappy'
)
AS SELECT *
FROM exelixis_universe.tc_patients;

-- COMMAND ----------

SELECT diagnosis_code_2, COUNT(DISTINCT patient_id)
FROM exelixis_universe.tc_patients
WHERE diagnosis_code_2 LIKE '198%' OR diagnosis_code_2 LIKE '196%' OR diagnosis_code_2 LIKE '197%' OR diagnosis_code_2 LIKE 'C79%' OR diagnosis_code_2 LIKE 'C77%' OR diagnosis_code_2 LIKE 'C78%' OR diagnosis_code_2 LIKE 'C77%' OR diagnosis_code_2 LIKE 'C7B'
GROUP BY 1

-- COMMAND ----------

-- create a flag for first visit 
SELECT *, 
CASE WHEN (patient_id, service_from_date) IN (SELECT patient_id, first_service_date FROM exelixis_universe.first_service_record) THEN 1
ELSE 0 END AS first_service_flag
FROM dataset

-- COMMAND ----------

 --create a flag for metastatic, don't run
 /*
  Our targeted secondary diagnosis:
  C7949, C7801, C787, C779, C781, C7951, C7982, C773, C784, C7952, C775
  19889, 1969, 1970, C7989, C770, C7800, C7839, C7830, C7880, C7889, C7801, C7802, C780m C781, C782, C783, C784, C785, C786, C787, C788, C89, C771, C772, C773, C774, C775, C776, C777, C778, C779, C7900, C7901, C7902, C7960, C7961, C7962, C7970, C7971, C7972, C7910, C7911, C7919, C792, C799, C7931, C7932, C7940, C7949, C7951, C7981, C7982, C7B00, C7B01, C7B02, C7B03, C7B04, C7B09, C7B1, C7B8 
 */

 SELECT *,
 CASE WHEN diagnosis_code_2 IN () THEN 1 
 ELSE 0
 END AS metastatic_flag
 FROM dataset 


-- COMMAND ----------

-- patient_id 
SELECT patient_id 
FROM 
(
  SELECT patient_id, datediff(metastatic_date, first_service_date) AS metastatic_days 
  FROM 
  (
    -- subquery for all patients who's cancer hasn't metaside
    SELECT patient_id, MIN(service_from_date) AS first_service_date 
    FROM exelixis_universe.tc_patients 
    WHERE exelixis_universe.tc_patients .diagnosis_code_2 NOT IN ()
    GROUP BY 1
  ) f  
  INNER JOIN 
  (
    -- subquery for all patients who's cencer has metastatic
  SELECT patient_id, MIN(service_from_date) AS metastatic_date 
  FROM dataset 
  WHERE f.diagnosis_code1 IN () AND dianosis_code_2 IN () 
  GROUP BY 1) t 
  ON f.patient_id = t.patient_id )  a 
WHERE metastaic_days > 180

-- COMMAND ----------

SELECT *
FROM kythera_universe.rx_v13
WHERE ndc_nationaldrugcode IN ('5024221060', '5024221083', '5024221090')

-- COMMAND ----------

SELECT *
FROM exelixis_universe.first_diagnosis_record
LIMIT 3

-- COMMAND ----------

CREATE TABLE exelixis_universe.test_delta
USING delta
OPTIONS (
    'path' '/mnt/mx_v12_mount/wayfinder-procdna-s3-wayfinder-data/Exelixis_Kythera_Analysis/test_delta',
    'compression' 'snappy'
)
AS SELECT *
FROM exelixis_universe.first_diagnosis_record
LIMIT 5; 

-- COMMAND ----------

SELECT *
FROM exelixis_universe.test_delta

-- COMMAND ----------

ALTER TABLE exelixis_universe.test_delta
ADD COLUMN first_diagnosis_flag INT;
UPDATE exelixis_universe.test_delta
SET first_diagnosis_flag = CASE WHEN year(service_from_date) = 2020 THEN 1 ELSE 0 END;

-- COMMAND ----------

SELECT first_diagnosis_flag 
FROM exelixis_universe.test_delta


-- COMMAND ----------

SELECT count(DISTINCT patient_ID)
FROM exelixis_universe.dtc_mx_patients
WHERE secondary_diagnosis = 1 AND patient_id IS NOT NULL;

-- COMMAND ----------

SELECT COUNT(DISTINCT patient_id)
FROM exelixis_universe.metastatic_patient

-- COMMAND ----------

WITH CTE AS (
SELECT *
FROM exelixis_universe.dtc_mx_patients
WHERE patient_id NOT IN (
  SELECT DISTINCT patient_id 
  FROM exelixis_universe.metastatic_patient
) AND secondary_diagnosis = 1 AND patient_id IS NOT NULL )

SELECT COUNT(DISTINCT c1.patient_id)
FROM CTE c1
JOIN CTE c2 ON c1.patient_id = c2.patient_id 
WHERE c1.first_diagnosis = 1 AND c2.secondary_diagnosis = 1  AND c2.service_from_date >= c1.service_from_date

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Patient Funnel (Page 8)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1.filter out all patients with DTC

-- COMMAND ----------

-- All patients with C73, 193, C750, Z85850
-- The second phase of the funnel
SELECT *
FROM exelixis_universe.dtc_mx_patients
LIMIT 3

-- first diagnosis = 1 means that this is the first diagnosis record of each patient
-- secondary diagnosis = 1 means that DTC has been metastatic

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 2. Filter out all patients with metastatic period larger than 6 months

-- COMMAND ----------

/*
Rules we should consider:
1. The patients shouldn't come with secondary diagnosis
  -- IF first_diagnosis = 1 AND secondary_diagnosis = 1 THEN ignore
2. The time between first and second diagnosis should exceeds 6 month
  -- calculate the their first diagnosis, MIN(secondary diagnosis date) and then join
3. The patients must have metastatic
  -- secondary diagnosis = 1
*/

-- Check condition one
SELECT COUNT(DISTINCT patient_id) 
FROM exelixis_universe.dtc_mx_patients
WHERE secondary_diagnosis = 1 AND first_diagnosis = 1
-- 15327 patients have been diagnosis as metastatic at the first time they are diagnosised with DTC

-- COMMAND ----------

SELECT COUNT(DISTINCT patient_id)
FROM exelixis_universe.dtc_mx_patients
WHERE secondary_diagnosis = 1

-- COMMAND ----------

CREATE TABLE exelixis_universe.metastatic_patient_cp(
patient_id string,
first_diagnosis_date date,
secondary_diagnosis_date date,
month_difference decimal(38,2)
)
USING DELTA
     OPTIONS (
         'path' '/mnt/mx_v12_mount/wayfinder-procdna-s3-wayfinder-data/Exelixis_Kythera_Analysis/metastatic_patient_cp',
         'compression' 'snappy')


-- COMMAND ----------

INSERT INTO TABLE exelixis_universe.metastatic_patient_cp
SELECT t1.patient_id, t1.service_from_date AS first_diagnosis_date, t2.secondary_diagnosis_date AS secondary_diagnosis_date, DATEDIFF(month, t1.service_from_date, t2.secondary_diagnosis_date) AS month_difference
FROM (
  SELECT patient_id, service_from_date
  FROM exelixis_universe.dtc_mx_patients
  WHERE first_diagnosis = 1 
  GROUP BY patient_id, service_from_date
)t1
INNER JOIN 
(
  SELECT patient_id, MIN(service_from_date) AS secondary_diagnosis_date
  FROM exelixis_universe.dtc_mx_patients
  WHERE secondary_diagnosis = 1 
  GROUP BY patient_id
) t2
ON t1.patient_id = t2.patient_id


-- COMMAND ----------

SELECT COUNT(DISTINCT patient_id)
FROM exelixis_universe.metastatic_patient_cp
WHERE month_difference > 6 
-- In the end, only 12742 patients are taken into account

-- COMMAND ----------

SELECT DISTINCT patient_id
FROM exelixis_universe.dtc_mx_patients
WHERE secondary_diagnosis = 1 AND patient_id IS NOT NULL AND patient_id NOT IN (
  SELECT patient_id
  FROM exelixis_universe.dtc_mx_patients
  WHERE first_diagnosis = 1 AND service_from_date > '2018-07-01'
)

-- COMMAND ----------



-- COMMAND ----------

-- Check Alison's code
SELECT COUNT(DISTINCT patient_id)
FROM exelixis_universe.tc_rx_patients
WHERE first_diagnosis = 1

-- COMMAND ----------

-- Check Alison's code
SELECT COUNT(DISTINCT patient_id)
FROM exelixis_universe.tc_rx_patients
WHERE secondary_diagnosis = 1

-- COMMAND ----------

SELECT COUNT(DISTINCT patient_id)
FROM exelixis_universe.dtc_mx_patients 
WHERE patient_id IN (
  SELECT patient_id
  FROM exelixis_universe.tc_rx_patients
  WHERE ndc_proprietaryname IN ('NEXAVAR', 'LENVIMA', 'CABOMETYX', 'VOTRIENT', 'SUTENT', 'INLYTA', 'KOSELUGO', 'AFINITOR', 'COMETRIQ', 'VITRAKVI', 'ROZLYTREK', 'GAVRETO', 'RETEVMO', 'TAFINLAR', 'MEKINIST', 'ZELBORAF', 'KEYTRUDA') --AND Year(date_of_service) >= 2017
  --AND diagnosis_code IS NULL
ORDER BY patient_id) AND diagnosis_code_2 IN ('19889', '1969', '197', 'C7989', 'C770', 'C7800', 'C7839', 'C7830', 'C7880', 'C7889', 'C7801', 'C7802', 'C780', 'C781', 'C782', 'C783', 'C784', 'C785', 'C786', 'C787', 'C788', 'C789', 'C771', 'C772', 'C773', 'C774', 'C775', 'C776', 'C777', 'C778', 'C779', 'C7900', 'C7901', 'C7902', 'C7960', 'C7961', 'C7962', 'C7970', 'C7971', 'C7972', 'C7910', 'C7911', 'C7919', 'C792', 'C799', 'C7931', 'C7932', 'C7940', 'C7949', 'C7951', 'C7952', 'C7981', 'C7982', 'C7B00', 'C7B01', 'C7B02', 'C7B03', 'C7B04', 'C7B09', 'C7B1', 'C7B8') AND Year(service_from_date) >= 2017

-- COMMAND ----------

SELECT patient_id, diagnosis_code_1, diagnosis_code_2, diagnosis_code_3, diagnosis_code_4, diagnosis_code_5, ndc_proprietaryname
FROM exelixis_universe.dtc_mx_patients
WHERE patient_id = '00ae94da97c605d9293584fc1bb6dfcc5a44299e9c044f77c421636cf9bd6c6308c3f3cca54d718c9929c6f1c9f1ae581ecf080e37807dfb4dcf75d82abb20b9'

-- COMMAND ----------

-- check alison's code
SELECT COUNT(DISTINCT patient_id)
FROM exelixis_universe.dtc_mx_patients
WHERE first_diagnosis = 1

-- COMMAND ----------

CREATE TABLE exelixis_universe.test
USING DELTA
OPTIONS (

    'path' '/mnt/mx_v12_mount/wayfinder-procdna-s3-wayfinder-data/Exelixis_Kythera_Analysis/test',

    'compression' 'snappy'
)
SELECT patient_id 
FROM exelixis_universe.metastatic_patient_cp

-- COMMAND ----------

SELECT *
FROM exelixis_universe.test

-- COMMAND ----------

SELECT 
FROM exelixis_universe.dtc_mx_patients
WHERE 