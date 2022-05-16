--=================================================================================================
-- DESCRIPTION
--=================================================================================================
--  The purpose of this script is to create a set of database views in a database within your Synapse Analytics Serverless SQL Pool that will 
--  translate numeric codes for Option Sets, Status and States into human-readable descriptions for tables exported using Synapse Link for Dataverse.
--  See the following article for background: https://docs.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-choice-labels
--
-- PREREQUISITES
-- 1. You have configured Synapse Link for Dataverse to export to a Synapse Analytics Workspace
-- 2. You have sufficient read access to the lake database created by Synapse Link for Dataverse
-- 3. You have created an additional database in which views with enriched entities will be created
-- 4. You have sufficient permissions to create database objects in your newly-created database
--
-- IMPORTANT: please execute this script in the context of the Lake Database corresponding to your Synapse Link for Dataverse!
USE [SynapseLinkForDataverseDBName] --Specify the name of the database corresponding to your Synapse Link for Dataverse

--=================================================================================================
--PROVIDE INPUT PARAMETERS:
--=================================================================================================
DECLARE
	@EnrichedViewDatabase sysname, --Specify the name of the database in which views with enriched entities will be created
	@EnrichedViewSchema sysname, --Specify the name of the database schema in which views with enriched entities will be created
	@EnrichedColumnSuffix varchar(50), 	--Specify the suffix for columns enriched with human-readable descriptions. For example, the suffix of "label" will change a statecode column in the base table to a statelabel column in the enriched view.
	@LanguageCode varchar(10), --Specify the language code for localized labels. For example, English - United States is 1033 (https://docs.microsoft.com/en-us/openspecs/office_standards/ms-oe376/6c085406-a698-4e12-9d4d-c3b0ee3dbc4a)
	@BaseTableSuffix varchar(50), --If applicable, specify the suffix in the names of the base tables or views (e.g., '_partitiond'). The default is an empty string.
	@PreviewOnly bit --Indicate whether to preview the SQL Script (without creating the views) = 1 ; Create views = 0;

SET @EnrichedViewDatabase = 'MyEnrichedDatabase'
SET @EnrichedViewSchema = 'dbo'
SET @EnrichedColumnSuffix = 'label'
SET @LanguageCode = 1033
SET @BaseTableSuffix = ''
SET @PreviewOnly = 1 

--=================================================================================================
-- Do not edit the script below this point
--=================================================================================================

--Get column metadata from the Lake Database managed by Synapse Link for Dataverse
--The column metadata will be stored as a JSON document in a scalar variable
--This is needed as a workaround for the limitation of not allowing system objects to be used in distributed queries
DECLARE @ColumnMetadata nvarchar(MAX), @ColumnMetadataSQL nvarchar(MAX)

--Define the SQL statement to retrieve column metadata from the Lake Database managed by Synapse Link for Dataverse
--Results will be stored as a JSON document in a variable
SET @ColumnMetadataSQL = 'SET @ColumnMetadataOUT = (
SELECT TABLE_SCHEMA, 
	TABLE_NAME, 
	COLUMN_NAME, 
	ORDINAL_POSITION, 
	DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = ''dbo''
	AND TABLE_NAME NOT IN (''OptionsetMetadata'', ''GlobalOptionsetMetadata'',''StateMetadata'',''StatusMetadata'', ''TargetMetadata'')
	AND TABLE_NAME LIKE ''%' + @BaseTableSuffix + '''
FOR JSON AUTO)'
DECLARE @ParmDefinition NVARCHAR(MAX);  
SET @ParmDefinition = N'@ColumnMetadataOUT NVARCHAR(MAX) OUTPUT';  
EXECUTE sp_executesql @ColumnMetadataSQL, @ParmDefinition, @ColumnMetadataOUT=@ColumnMetadata OUTPUT;  

--Declare a variable to store a SQL statement for creating enriched views
DECLARE @SQL nvarchar(MAX) = ''

; WITH CM AS (
--Parse column metadata variable and construct a table based on its content
SELECT JSON_VALUE(CM.value, '$.TABLE_SCHEMA') AS TableSchema,
	 JSON_VALUE(CM.value, '$.TABLE_NAME') AS TableName,
	 LEFT(JSON_VALUE(CM.value, '$.TABLE_NAME'), LEN(JSON_VALUE(CM.value, '$.TABLE_NAME'))-LEN(@BaseTableSuffix)) AS EntityName,
	 JSON_VALUE(CM.value, '$.COLUMN_NAME') AS ColumnName,
	 CAST(JSON_VALUE(CM.value, '$.ORDINAL_POSITION') AS INT) AS OrdinalPosition,
	 JSON_VALUE(CM.value, '$.DATA_TYPE') AS DataType
FROM OPENJSON (@ColumnMetadata) AS CM
)

, OSM AS (
--Get Option Set Metadata
SELECT DISTINCT 
	EntityName, 
	OptionSetName, 
	QUOTENAME(EntityName + '_' + OptionSetName) AS Alias
FROM dbo.[OptionsetMetadata]
WHERE LocalizedLabelLanguageCode = @LanguageCode
)

, GOSM AS (
--Get Global Option Set Metadata
SELECT DISTINCT 
	OptionSetName, 
	QUOTENAME('Global_' + OptionSetName) AS Alias
FROM dbo.[GlobalOptionsetMetadata]
WHERE LocalizedLabelLanguageCode = @LanguageCode
)

, StateM AS (
--Get State Metadata
SELECT DISTINCT 
	EntityName, 
	QUOTENAME(EntityName + '_State') AS Alias
FROM dbo.[StateMetadata]
WHERE LocalizedLabelLanguageCode = @LanguageCode
)

, StatusM AS (
--Get Status Metadata
SELECT DISTINCT 
	EntityName,
	QUOTENAME(EntityName + '_Status') AS Alias
FROM dbo.[StatusMetadata]
WHERE LocalizedLabelLanguageCode = @LanguageCode
)

, SQLStatement AS (
--Enumerate all lines in the source table and replace codes with labels where applicable
SELECT CM.EntityName,
	--Before the first column of each table, construct a CREATE OR ALTER VIEW statement
	CASE WHEN CM.OrdinalPosition = 1
		THEN 'CREATE OR ALTER VIEW ' + QUOTENAME(@EnrichedViewSchema) + '.' + CM.EntityName + '
		AS
		SELECT '
		ELSE '	,'
		END
	--For each column, check if it needs to be replaced with a suitable localized label
	+ CASE 
		WHEN OSM.OptionSetName IS NOT NULL THEN OSM.Alias + '.[LocalizedLabel] AS ' + REPLACE(QUOTENAME(CM.ColumnName), 'code]', @EnrichedColumnSuffix + ']')
		WHEN GOSM.OptionSetName IS NOT NULL THEN GOSM.Alias + '.[LocalizedLabel] AS ' + REPLACE(QUOTENAME(CM.ColumnName), 'code]', @EnrichedColumnSuffix + ']')
		WHEN StateM.EntityName IS NOT NULL THEN StateM.Alias + '.[LocalizedLabel] AS ' + REPLACE(QUOTENAME(CM.ColumnName), 'code]', @EnrichedColumnSuffix + ']')
		WHEN StatusM.EntityName IS NOT NULL THEN StatusM.Alias + '.[LocalizedLabel] AS ' + REPLACE(QUOTENAME(CM.ColumnName), 'code]', @EnrichedColumnSuffix + ']')
		ELSE '[Base].' + QUOTENAME(CM.ColumnName)
		END AS [SQLLine],
	CM.OrdinalPosition
FROM CM 
	LEFT JOIN OSM
		ON CM.EntityName = OSM.EntityName
		AND CM.ColumnName = OSM.OptionSetName
		AND CM.DataType LIKE '%int' --Only include columns with integer data type
	LEFT JOIN GOSM
		ON CM.ColumnName = GOSM.OptionSetName
		AND CM.DataType LIKE '%int' --Only include columns with integer data type
	LEFT JOIN StateM
		ON CM.EntityName = StateM.EntityName
		AND CM.ColumnName = 'statecode'
		AND CM.DataType LIKE '%int' --Only include columns with integer data type
	LEFT JOIN StatusM
		ON CM.EntityName = StatusM.EntityName
		AND CM.ColumnName = 'statuscode'
		AND CM.DataType LIKE '%int' --Only include columns with integer data type

UNION ALL
--Construct the first line of the FROM clause, referencing external tables created by Synapse Link for Dataverse
SELECT DISTINCT
CM.EntityName,
'FROM ' + QUOTENAME(DB_NAME()) + '.' + QUOTENAME(CM.TableSchema) + '.' + QUOTENAME(CM.TableName) + ' AS Base' AS SQLLine,
10000 AS OrdinalPosition
FROM CM

UNION ALL 
--Construct LEFT JOIN statements for each relevant OptionSetMetadata field
SELECT DISTINCT OSM.EntityName AS EntityName,
'	LEFT JOIN ' + QUOTENAME(DB_NAME()) + '.[dbo].[OptionSetMetadata] AS ' + OSM.Alias + ' 
		ON ' + OSM.Alias + '.EntityName = ''' + OSM.EntityName + ''' 
		AND ' + OSM.Alias + '.OptionSetName = ''' +  OSM.OptionSetName + '''
		AND [Base].' + QUOTENAME(OSM.OptionSetName) + ' = ' + OSM.Alias + '.[Option]
		AND ' + OSM.Alias + '.LocalizedLabelLanguageCode = ' + @LanguageCode + '' AS SQLLine,
20000 AS OrdinalPosition
FROM OSM
	JOIN CM
		ON CM.EntityName = OSM.EntityName
		AND CM.ColumnName = OSM.OptionSetName
WHERE CM.DataType LIKE '%int' --Only capture columns with Integer Data Types

UNION ALL 
--Construct LEFT JOIN statements for each relevant GlobalOptionSetMetadata field
SELECT DISTINCT CM.TableName AS TableName,
'	LEFT JOIN ' + QUOTENAME(DB_NAME()) + '.[dbo].[GlobalOptionSetMetadata] AS ' + Alias + ' 
		ON ' + Alias + '.OptionSetName = ''' +  OptionSetName + '''
		AND [Base].' + QUOTENAME(OptionSetName) + ' = ' + Alias + '.[Option]
		AND ' + Alias + '.LocalizedLabelLanguageCode = ' + @LanguageCode + '' AS SQLLine,
30000 AS OrdinalPosition
FROM GOSM
	JOIN CM
		ON CM.ColumnName = GOSM.OptionSetName
WHERE CM.DataType LIKE '%int' --Only capture columns with Integer Data Types

UNION ALL 
--Construct LEFT JOIN statements for each relevant State Metadata field
SELECT DISTINCT CM.EntityName AS EntityName,
'	LEFT JOIN ' + QUOTENAME(DB_NAME()) + '.[dbo].[StateMetadata] AS ' + StateM.Alias + ' 
		ON ' + StateM.Alias + '.EntityName = ''' + StateM.EntityName + ''' 
		AND [Base].statecode' + ' = ' + StateM.Alias + '.[State]
		AND ' + StateM.Alias + '.LocalizedLabelLanguageCode = ' + @LanguageCode + '' AS SQLLine,
40000 AS OrdinalPosition
FROM StateM
	JOIN CM
		ON CM.EntityName = StateM.EntityName
		AND CM.ColumnName = 'statecode'
WHERE CM.DataType LIKE '%int' --Only capture columns with Integer Data Types

UNION ALL
--Construct LEFT JOIN statements for each relevant Status Metadata field
SELECT DISTINCT CM.EntityName AS EntityName,
'	LEFT JOIN ' + QUOTENAME(DB_NAME()) + '.[dbo].[StatusMetadata] AS ' + StatusM.Alias + ' 
		ON ' + StatusM.Alias + '.EntityName = ''' + StatusM.EntityName + ''' 
		AND [Base].statuscode' + ' = ' + StatusM.Alias + '.[Status]
		AND ' + StatusM.Alias + '.LocalizedLabelLanguageCode = ' + @LanguageCode + '' AS SQLLine,
40000 AS OrdinalPosition
FROM StatusM
	JOIN CM
		ON CM.EntityName = StatusM.EntityName
		AND CM.ColumnName = 'statuscode'
WHERE CM.DataType LIKE '%int' --Only capture columns with Integer Data Types

UNION ALL
--Add statement terminator
SELECT DISTINCT
EntityName,
'; ' + CHAR(10) AS SQLLine,
100000 AS OrdinalPosition
FROM CM
)

--Construct individual statements to create views (1 view per row)
--Since CREATE VIEW statement must be the first statement in a batch, assign each view definition to a variable 
--and use the EXEC(@variable) command to create view as part of its own, separate batch.
, ViewDefinitions AS (
SELECT 'DECLARE @' + EntityName + ' NVARCHAR(MAX) = ''
	' + REPLACE(STRING_AGG(CAST(SQLLine as varchar(MAX)), CHAR(10)) WITHIN GROUP (ORDER BY EntityName, OrdinalPosition, SQLLine), '''', '''''') + ''' ' + CHAR(10) + 'EXEC [' + @EnrichedViewDatabase + '].dbo.sp_executesql @' + EntityName + CHAR(10) AS ViewDefinition
FROM SQLStatement
GROUP BY EntityName
)

--Construct a comprehensive SQL statement to create all views
SELECT @SQL = STRING_AGG(ViewDefinition, ';' + CHAR(10) + CHAR(10))
FROM ViewDefinitions

--Return a preview of the SQL Script to be generated or go ahead and create the views.
IF @PreviewOnly = 1
BEGIN
	--Return the final SQL statement
	SELECT '--================================================================================================='+ CHAR(10) +' ' + CHAR(10) AS [--SQL Statement]
	UNION ALL 
	SELECT '-- A preview of the script to generate enriched views is provided below.'  AS [--SQL Statement]
	UNION ALL 
	SELECT '-- No database objects have been created.' AS [--SQL Statement]
	UNION ALL 
	SELECT '-- Re-run this script with the @PreviewOnly parameter set to 0 to actually create the views.' AS [--SQL Statement]
	UNION ALL 
	SELECT '--================================================================================================='+ CHAR(10) +' ' + CHAR(10) AS [--SQL Statement]
	UNION ALL 
	SELECT VALUE AS [--SQL Statement] FROM STRING_SPLIT(@SQL, CHAR(10))
END
ELSE
BEGIN
	--Execute the SQL statement
	PRINT 'Beginning view creation'
	EXEC sp_executesql @SQL
	PRINT 'Completed view creation'
END