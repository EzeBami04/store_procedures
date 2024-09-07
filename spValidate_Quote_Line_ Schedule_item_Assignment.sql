CREATE PROCEDURE [QA_Playground].[spvalidate_quote_line_schedule_GIS]
	AS
	BEGIN
	-- Declaring Variables
		DECLARE @LastJobExtractEndTime DATETIME2 = '2011-01-01'
		,@ExtractEndTime DATETIME2 = GETDATE()
		,@SPStartTime DATETIME2 = GETDATE()
		,@SPName VARCHAR(50) = 'audit_stage_insert_time'
		

-- Droppping existing table
	IF OBJECT_ID('QA_test.gis_pol_ver_planloc') IS NOT NULL
		DROP TABLE QA_test.gis_pol_ver_planloc
	IF OBJECT_ID('QA_test.gis_pol_item_ins') IS NOT NULL
		DROP TABLE QA_test.gis_pol_item_ins
	IF OBJECT_ID('QA_test.gis_pol_cov_coding') IS NOT NULL
		DROP TABLE QA_test.gis_pol_cov_coding
	IF OBJECT_ID('QA_test.gis_pol_cov_amts') IS NOT NULL
		DROP TABLE QA_test.gis_pol_cov_amts
	IF OBJECT_ID('QA_test.gis_ii_rec') IS NOT NULL
		DROP TABLE QA_test.gis_ii_rec


	IF OBJECT_ID('QA_test.intg_QLS_item') IS NOT NULL
		DROP TABLE QA_test.intg_QLS_item

	IF OBJECT_ID('QA_test.DEV_QLS_GIS') IS NOT NULL
		DROP TABLE QA_test.DEV_QLS_GIS


	 SELECT
            @LastJobExtractEndTime = Extract_EndTime
		 FROM
            [JobConfig].[JobLastRun]
		WHERE
            [stored_ProcedureName] = audit_stage_insert_time
			AND [Zone] = @zone

/*Select *
From(Select*, ROW_NUMBER() Over(Partition by pol_no, pol_ver_dt) as rnb
		From [stg_gis].pol_ver
		where Cast(pol_ver_dt as date) >= '2024-03-15'
	) x
	Where rnb=1 AND pol_tx_typ in ('22', '23', '24') ;
*/
	--- QA_test.gis_pol_ver_planloc
 CREATE TABLE QA_test.gis_pol_ver_planloc 
 WITH	(
		DISTRIBUTION =HASH (pol_no,pol_ver_dt,planloc_no), 
		CLUSTERED COLUMNSTORE INDEX
		)
		AS
	(
	SELECT *, ROW_NUMBER() Over(Partition by pol_ver_dt,planloc_no Order by audit_stage_insert_time) as rnb
	from [stg_gis].[pol_ver_planoc]
	where Cast(pol_ver_dt as date) >= '2024-03-15'
	) x
	where rnb =1;


----QA_test.gis_pol_item_ins
Create Table QA_test.gis_pol_item_ins 
with
	( 
	Distribution = Hash(pol_no, pol_ver_dt,pol_item_ins_no), 
	Clustered Columnstore Index) 
	As
	(
	Select *
	From (
			Select *, Row_number() Over(Partition by pol_item_ins_no,pol_ver_dt Order by audit_stage_insert_time) as rnb
			From [stg_gis].[pol_item_ins]) 
			where Cast(pol_ver_dt as date) >= '2024-03-15'
		) x
		where rnb =1;


	---- QA_test.gis_pol_cov_coding
CREATE  QA_test.gis_pol_cov_coding
WITH
	(DISTRIBUTION =HASH(pol_ver_dt, pol_cov_no), 
	CLUSTERED COLUMNSTORE INDEX
	)
AS
(
	SELECT *, ROW_NUMBER() Over(Partition by pol_ver_dt, pol_cov_no order by audit_stage_insert_time DESC) AS rnb
	from [stg_gis].[pol_cov_coding]
	where Cast(pol_ver_dt as date) >= '2024-03-15'
	) x
	where rnb=1;

--- QA_test.gis_pol_cov_amts
CREATE QA_test.gis_pol_cov_amts
WITH
	(DISTRIBUTION =HASH(pol_ver_dt, pol_cov_no), 
	CLUSTERED COLUMNSTORE INDEX
	)
AS
	( 
	SELECT *, ROW_NUMBER() Over (Partition by pol_ver_dt, pol_cov_no, Order by audit_stage_insert_time DESC) as rrnb
	from [stg_gis].[pol_cov_amts]
	where Cast(pol_ver_dt as date) >= '2024-03-15'
	) x
	Where rnb=1;

	-- QA_test.gis_ii_rec
CREATE QA_test.gis_ii_rec
WITH
	(DISTRIBUTION =HASH(pol_ver_dt, rec_serial_no), 
	CLUSTERED COLUMNSTORE INDEX
	)
AS
	( 
	SELECT *, ROW_NUMBER() Over (Partition by pol_ver_dt, rec_serial_no, Order by audit_stage_insert_time DESC) as rrnb
	from [stg_gis].[pol_cov_amts]
	where Cast(pol_ver_dt as date) >= '2024-03-15'
	) x
	Where rnb=1;

SELECT DISTINCT 
	CAST(intg_qls.Quote_Line_Schedule_item As bigint) as Quote_Line_Scheduled_Item_Unique_Key
	CAST(intg_qh.Policy_Transaction_Effective_Date AS BIGINT) AS Policy_Transaction_Effective_Date,
	CAST(intg_qh.Record_Expiry_Date AS DATETIME2) As Policy_Transaction_Expiry_Date ,
	CAST(pol_item_ins.pol_no AS nvarchar(256))	as		Quote_Policy_Number	,
	CAST(pol_item_ins.pol_item_ins_no AS bigint)  as	Quote_Line_Scheduled_Item_Unique_Key,
	CAST(pol_item_ins.pol_ver_dt As datetime2) as Record_Effective_Date,
	CAST(pol_cov_coding.pol_cov_no as nvarchar(100)) as		Quote_Line_Coverage_ID,
	CAST(pol_ver_planloc.locn_no as nvarchar(256)) as Address_ID,
	CAST(pol_item_ins.pol_ver_planloc_no As nvarchar(100)) AS	Location_ID,
	cast(null as varchar(255)) as	Coverable_Type_ID,
	cast(pol_item_ins.sch_no AS	int) as Schedule_Number,
	CAST(pol_item_ins.sch_seq_no AS INT)	As	Schedule_Sequence_Number,
	CAST(pol_item_ins.item_ins_typ as Varchar(255)) As Schedule_Item_Type_Source_Code,
	CAST(pol_cov_amts.cov_amt AS decimal(15,2)) As Limit_Amount,
	Cast( Null as varchar(255)) as Limit_Source_Code,
	CAST(pol_cov_amts.deduct_amt as varchar(255)) AS Deductible_Source_Code,
	CAST(pol_item_ins.pol_item_desc1 AS varchar(256)) As Schedule_Item_Description1,
	CAST(pol_item_ins.pol_item_desc2 As varchar(256)) As Schedule_Item_Description2,
	CAST(pol_item_ins.pol_item_desc3 As varchar(256))	As Schedule_Item_Description3,
	CAST(pol_item_ins.pol_item_desc4 As varchar(256))	As Schedule_Item_Description4,
	CAST(ii_rec.rec_yr AS INT) As	Make_Year_Number,
	CAST(ii_rec.rec_yr AS nvarchar(256)) As	Make_Name,
	CAST(ii_rec.rec_model aS n,varchar(256)) As Model_Name,
	CAST(pol_item_ins.appraisal_dt as datetime2) As Appraised_Date,
	CAST(TBA As nvarchar(100)) As Additional_Interest_Name,
	CAST(ii_rec.rec_hp As int) As Horsepower_Number,
	CAST(ii_rec.rec_max_speed As int) As Maximum_Speed_Number,
	CAST(ii_rec.rec_max_speed_unit AS varchar(255)) As Maximum_Speed_Unit_Source_Code,
	CAST(ii_rec.rec_len As	int) As Length_Number,
	CAST(ii_rec.rec_engine_cc As int) AS Engine_Number, 

    ISNULL( Select
        NULLIF(
            IIF(ii_rec.rec_hp IS NOT NULL AND ii_rec.rec_engine_cc IS NOT NULL, CAST(ii_rec.rec_hp As Varchar(255)), 
        IIF(ii_rec.rec_hp IS NOT NULL, CAST(ii_rec.rec_hp As Varchar(255),
            IIF(ii_rec.rec_engine_cc IS NOT NULL, CAST(ii_rec.rec_engine_cc As varchar(255))
			from [stg_gis].[ii_rec]
    ) As Engine_Unit_Source_Code, 

	CAST(TBA as nvarchar(256)) As Schedule_Item_Address, 
	CAST(TBA as nvarchar(256)) As Schedule_Item_Postal_Code,  
	CAST(TBA as nvarchar(256)) AS Form_Source_Code , 
	CAST(TBA as nvarchar(256)) AS Occupancy_Source_Code,	

	CAST(ii_rec.rec_inboard_ind AS nvarchar(100)) As Recreational_Inboard_Indicator,
	Cast(NULL As varchar(100)) as Condition_ID,
	Cast(NULL As varchar(100)) as Exclusion_ID_ID,
	Cast(NULL As varchar(100)) as Named_Insured_ID,
	CAST(ii_rec.rec_len_unit AS Varchar(255)) As Recreational_Length_Unit_Source_Code,
	CAST(ii_rec.rec_inboard_ind As varchar(100)) As Recreational_Inboard_Indicator,
	CAST(ii_rec.rec_cooking_fac_ind AS varchar(100)) as Recreational_Cooking_Facility_Indicator,
	CAST(ii_rec.rec_engine_cc AS NVARCHAR(100)) AS Recreational_Engine_CC_Number,
	Cast(NULL As bigint) as Quote_Insured_Party_Surrogate_Key,
	'GIS' as Source_System_Code
	CASE WHEN CAST(intg_qh.Record_Expiry_Date AS DATE) = '9999-12-31' THEN '1' ELSE '0' END AS Audit_Is_Active_Record,
	'0' AS Audit_Is_Delete_Record,
	CAST (NULL AS Datetime2) As Last_Valid_Quote_Date,
	CAST('-1' As Nvarchar(100)) AS Quote_Version_ID,
	CAST(NULL As Nvarchar(100)) AS Quote_owning_Coverable_ID,
	CAST(NULL AS Nvarchar(100)) As Policy_Line_Scheduled_Item_ID,
	CAST(NULL AS bigint) As  AS Policy_line_Scheduled_Item_Surrogate_key

FROM stg_gis.pol_item_ins pii
INNER JOIN stg_gis.pol_ver pv
     ON pv.pol_no = pii.pol_no
     AND pv.pol_ver_dt = pii.pol_ver_dt
LEFT JOIN stg_gis.pol_ver_planloc pvp
       ON pii.pol_no = pvp.pol_no
     AND pii.pol_ver_dt = pvp.pol_ver_dt
    AND pvp.pol_ver_planloc_no = pii.pol_ver_planloc_no
LEFT JOIN stg_gis.pol_cov_coding pcc
      ON pii.pol_no = pcc.pol_no
    AND pii.pol_ver_dt = pcc.pol_ver_dt
    AND pii.pol_item_ins_no = pcc.pol_item_ins_no
LEFT JOIN stg_gis.pol_cov_amts pca
     ON pcc.pol_no = pca.pol_no
   AND pcc.pol_ver_dt = pca.pol_ver_dt
   AND pcc.pol_item_ins_no = pca.pol_item_ins_no
   AND pcc.pol_cov_no = pca.pol_cov_no
LEFT JOIN ii_rec iir
      ON pii.pol_no = iir.pol_no
   AND pii.pol_ver_dt = iir.pol_ver_dt
   AND pii.pol_item_ins_no = iir.pol_item_ins_no
INNER JOIN intg_quote.Quote_Header intg_ph
    ON  iid.pol_no = intg_ph.Quote_Policy_Number
    and  intg_ph.Source_System_Code = 'GIS'
    and  iid.pol_ver_dt = intg_ph.policy_version_date
INNER JOIN intg_quote.Quote_Line intg_pl
    ON  iid.pol_no = intg_pl.Quote_Policy_Number
    and  pii.pol_item_ins_no = intg_pl.Quote_Coverable_ID
    and  intg_pl.Source_System_Code = 'GIS'
    and  intg_pl.policy_version_date = pv.pol_ver_dt
LEFT JOIN intg_location.Geographical_Location intg_gl
    ON  pvp.loc_no = intg_gl.Geographical_Location_ID
     and  intg_gl.Source_System_Code = 'GIS'
     AND CAST(intg_gl.Record_Expiry_Date AS DATE) = '9999-12-31'
    LEFT JOIN intg_common.reference_cross_walk -- refer to Master Code attributes for the individual join logic
WHERE pii.sch_no > 0

)
INTO QA_test.intg_QLS_item_GIS


---Duplicates check
SELECT Quote_Line_Scheduled_Item_Unique_Key, Policy_Transaction_Effective_Date, Record_Effective_Date,  COUNT(*)
FROM intg_Quote.Quote_Line_Schedule_Items
WHERE Source_System_Code = 'GIS'
GROUP BY Quote_Line_Scheduled_Item_Unique_Key, Policy_Transaction_Effective_Date, Record_Effective_Date
HAVING COUNT(*) > 1

SELECT Quote_Line_Scheduled_Item_Unique_Key, Policy_Transaction_Effective_Date, Record_Effective_Date,  COUNT(*)
FROM QA_Test.QA_Quote_Auto_GIS
WHERE Source_System_Code = 'GIS'
GROUP BY Quote_Line_Scheduled_Item_Unique_Key, Policy_Transaction_Effective_Date, Record_Effective_Date
HAVING COUNT(*) > 1


---BK NULL check
***************************************************************/
SELECT Quote_Line_Scheduled_Item_Unique_Key, Policy_Transaction_Effective_Date, Record_Effective_Date
FROM intg_Quote.Quote_Auto
WHERE Source_System_Code = 'GIS' AND (Quote_Line_Scheduled_Item_Unique_Key IS NULL OR Policy_Transaction_Effective_Date IS NULL OR Record_Effective_Date IS NULL )

SELECT Quote_Line_Scheduled_Item_Unique_Key, Policy_Transaction_Effective_Date, Record_Effective_Date
FROM QA_Test.QA_Quote_Auto_GIS
WHERE Source_System_Code = 'GIS' AND (Quote_Line_Scheduled_Item_Unique_Key IS NULL OR Policy_Transaction_Effective_Date IS NULL OR Record_Effective_Date IS NULL )

----- DEV RECORD
SELECT *
INTO QA_test.DEV_QLS_item
FROM intg_Quote.Quote_Line_Schedule_item
WHERE Source_System_Code = 'GIS' 

---Duplicate Checks


---- Data Count validation
With [COUNT_QA] 
AS( 
	Select Count(*) C
	From QA_test.intg_QLS_item_GIS) --- QA Data recoed count
,


--- Business Key Count
With [COUNT_DEV]
AS (
	Select Count(*) C
	From QA_test.DEV_QLS_item) --- Count of DEV Record
,

--- Business Key count 
[BUSINESS_MATCHED] AS(
	SELECT Quote_Line_Scheduled_Item_ID,
			Record_Effective_Date
	FROM	QA_test.intg_QLS_item_GIS
	INTERSECT
	SELECT Quote_Line_Scheduled_Item_ID,
			Record_Effective_Date
	FROM	 QA_test.DEV_QLS_item
	)
,
-- BUSINESS KEY NOT MATCHED
[BK_NOT_MATCHED] AS (
    SELECT [COUNT_QA].C - [BK_MATCHED].C AS C
    FROM [COUNT_QA],[BK_MATCHED]
	)
	,

---Macthing Records 
[MATCHED] AS(
	Select Count (*) C
	FROM	QA_test.intg_QLS_item_GIS

	INTERSECT
	Select Count (*)
	From(
	Quote_Line_Scheduled_Item_Unique_Key
	Policy_Transaction_Effective_Date,
	Policy_Transaction_Expiry_Date,
	Quote_Policy_Number	,
	Quote_Line_Scheduled_Item_Unique_Key,
	Record_Effective_Date,
	Quote_Line_Coverage_ID,
	Address_ID,
	Location_ID,
	Coverable_Type_ID,
	Schedule_Number,
	Schedule_Sequence_Number,
	Schedule_Item_Type_Source_Code,
	Limit_Amount,
	Limit_Source_Code,
	Deductible_Source_Code,
	Schedule_Item_Description1,
	Schedule_Item_Description2,
	Schedule_Item_Description3,
	Schedule_Item_Description4,
	Make_Year_Number,
	Make_Name,
	Model_Name,
	Appraised_Date,
	Additional_Interest_Name,
	Horsepower_Number,
	Maximum_Speed_Number,
	Maximum_Speed_Unit_Source_Code,
	Length_Number,
	Engine_Number, 
	Engine_Unit_Source_Code, 
	Schedule_Item_Address, 
	Schedule_Item_Postal_Code,  
	Form_Source_Code , 
	Occupancy_Source_Code,	
	Recreational_Inboard_Indicator,
	Condition_ID,
	Exclusion_ID_ID,
	Named_Insured_ID,
	Recreational_Length_Unit_Source_Code,
	Recreational_Inboard_Indicator,
	Recreational_Cooking_Facility_Indicator,
	Recreational_Engine_CC_Number,
	Quote_Insured_Party_Surrogate_Key,
	Source_System_Code,
	Last_Valid_Quote_Date,
	Quote_Version_ID,
	Quote_owning_Coverable_ID,
	Policy_Line_Scheduled_Item_ID,
	Policy_line_Scheduled_Item_Surrogate_key

	FROM QA_test.DEV_QLS_item

	)r
),

--- Records not matched
NOT_MATCHED AS (
 SELECT [COUNT_QA].C - [MATCHED].C AS C
    FROM [COUNT_QA],[MATCHED]

),

INSERT INTO QA_Playground.[QA_Validation]
SELECT 
	GETDATE() AS EXECUTED_DATETIME,
      'intg_quote.Quote_Line' AS TEST_NAME
      ,'GIS' AS SOURCE_SYSTEM
     
     --Data_Count_Validation
      ,[COUNT_QA].C AS QA_COUNT
      ,[COUNT_DEV].C AS DEV_COUNT
      ,CASE
            WHEN [COUNT_QA].C = [COUNT_DEV].C 
            THEN 'PASS'
            ELSE 'FAIL'
    END AS STEP_1_COUNT_VALIDATION
     
     --BK_Validation
     ,[BK_MATCHED].C AS BK_MATCHED_COUNT
     ,[BK_NOT_MATCHED].C AS BK_NOT_MATCHED_COUNT

     ,CASE
            WHEN [BK_MATCHED].C = [COUNT_QA].C 
            AND [BK_NOT_MATCHED].C = 0
            THEN 'PASS'
            ELSE 'FAIL'
    END AS STEP_2_BK_VALIDATION
    
     --Data_Validation
      ,[MATCHED].C AS MATCHED_RECORDS 
      ,[NOT_MATCHED].C AS NOT_MATCHED_RECORDS
     ,CASE 
             WHEN [MATCHED].C = [COUNT_DEV].C AND [NOT_MATCHED].C = 0 
             THEN 'PASS' 
             ELSE 'FAIL' 
     END AS STEP_3_DATA_VALIDATION,
	@EXECUTED_BY AS EXECUTED_BY,
	NULL AS REMARK
FROM [COUNT_QA],
    [COUNT_DEV],
    [BK_MATCHED],
    [BK_NOT_MATCHED],
    [MATCHED],
    [NOT_MATCHED] 



--Check Report
SELECT DISTINCT * FROM QA_Playground.[QA_Validation] 
WHERE TEST_NAME = 'intg_quote.Quote_Line' ORDER BY EXECUTED_DATETIME DESC

END
