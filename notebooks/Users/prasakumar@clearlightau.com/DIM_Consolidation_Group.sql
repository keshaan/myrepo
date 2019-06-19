-- Databricks notebook source
-- Drop table Dim Consolidation Group in Databricks
/*Drop Table if exists pmfd_db.dim_consolidation_group */

-- COMMAND ----------

-- Create table Dim Consolidation Group in Databricks
/*CREATE TABLE pmfd_db.dim_consolidation_group 
    (
    consolidation_group_unit_sk bigint NOT NULL,
    consolidation_group_code string NOT NULL , 
     Consolidation_Group_Name string NOT NULL , 
     Consolidation_Unit_Code string NOT NULL ,
     Consolidation_Unit_Name string NOT NULL, 
     Creation_Date Timestamp NOT NULL)*/

-- COMMAND ----------

-- Load Dim Consolidation Group in Databricks
insert into table pmfd_db.dim_consolidation_group
select row_number() over(order by trim(CONGRPCD))+(select max(consolidation_group_unit_sk) from pmfd_db.dim_consolidation_group),CONGRPCD,CONGRPNM,CONUNCD,CONUNNM from (select distinct trim(ConsolidationGroupCode_CONGRPCD) CONGRPCD, trim(ConsolidationGroupName_CONGRPNM) CONGRPNM, trim(ConsolidationUnitCode_CONUNCD) CONUNCD, trim(ConsolidationUnitName_CONUNNM) CONUNNM, current_date() from pmfd_db.Finance_Consolidated_Financials_full)
