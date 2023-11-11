-- Databricks notebook source
-- DBTITLE 1,Employee Attrition project Problem Statement
-- Employees Attrition Project:

-- Employees are the backbone of the organization. Organization's performance is heavily based on the quality of the employees.Challenges that an organization has to face due to employee attrition are:
-- 1.Expensive in terms of both time and money to train the new employees,
-- 2.Loss of experienced employee,
-- 3.Impact in productivity, and
-- 4.Impact in profit


-- Business questions to brainstorm:
-- 1.What factors are contributing more to employee attrition?
-- 2.What type of measures should the company take in order to retain their employees?

-- COMMAND ----------

-- DBTITLE 1,Employee table
-- MAGIC %python
-- MAGIC df=spark.read.csv("/FileStore/tables/WA_Fn_UseC__HR_Employee_Attrition-2.csv",header=True)
-- MAGIC df.createOrReplaceTempView("Employee_Attrition")

-- COMMAND ----------

-- DBTITLE 1,Total Employee count
SELECT SUM(EmployeeCount) AS Total_Employee
FROM employee_attrition

-- COMMAND ----------

-- DBTITLE 1,Find out attrition division
SELECT SUM(EmployeeCount) AS Total_Employee, Attrition 
FROM employee_attrition 
GROUP BY 2

-- COMMAND ----------

-- DBTITLE 1,Age analysis: Find out which particular age attrition is high(18-24,25-31,32-38,39-45,46-52,52+)
SELECT SUM(EmployeeCount) AS Total_Employee,
CASE 
WHEN Age BETWEEN 18 AND 14 THEN '18-24' 
WHEN Age BETWEEN 25 AND 31 THEN '25-31'
WHEN Age BETWEEN 32 AND 38 THEN '32-38'
WHEN Age BETWEEN 39 AND 45 THEN '39-45'
WHEN Age BETWEEN 46 AND 52 THEN '46-52'
ELSE '52+'
END AS Age_group
FROM employee_attrition 
WHERE Attrition='Yes'
GROUP BY 2
ORDER BY Total_Employee DESC

-- COMMAND ----------

-- DBTITLE 1,Find out attrition by department
SELECT SUM(EmployeeCount) AS Total_Employee, Department 
FROM employee_attrition 
WHERE Attrition='Yes'
GROUP BY 2
ORDER BY 1 DESC

-- COMMAND ----------

-- DBTITLE 1,Attrition by Education
SELECT SUM(EmployeeCount) AS Total_Employee,
CASE 
WHEN Education= 1 THEN 'High School' 
WHEN Education= 2 THEN 'Undergraduate'
WHEN Education= 3 THEN 'Graduate'
WHEN Education= 4 THEN 'Master'
ELSE 'Doctorate'
END AS Education_group 
FROM employee_attrition 
WHERE Attrition='Yes'
GROUP BY 2
ORDER BY 1 DESC

-- COMMAND ----------

-- DBTITLE 1,Attrition by Environmental satisfaction
SELECT SUM(EmployeeCount) AS Total_Employee, EnvironmentSatisfaction 
FROM employee_attrition 
WHERE Attrition='Yes'
GROUP BY 2
ORDER BY 1 DESC
