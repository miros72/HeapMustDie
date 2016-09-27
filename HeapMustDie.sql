-- ================================= --
-- =========== STERTA ============== --
-- ================================= --
CREATE DATABASE TEST;
GO

USE TEST;
GO

CREATE TABLE Books
( 
	ID INT IDENTITY(1, 1) NOT NULL,
	Title VARCHAR(1000) NULL
);
GO
-- --
DBCC IND('TEST', 'Books', 0, 1);
DBCC IND('TEST', 'Books', 0);
-- --
SELECT DB_ID('TEST') AS Database_ID, OBJECT_ID('Books') AS Table_ID;
DBCC IND(7, 245575913, 0); --<== wstaw DB ID i OBJECT ID 
-- TYLKO DLA SQL 2012 LUB NOWSZYCH --
SELECT *
FROM sys.dm_db_database_page_allocations(DB_ID(), OBJECT_ID('Books'), 0, NULL, 'DETAILED')
WHERE is_allocated = 1;
GO
-- --
INSERT INTO Books VALUES ('Romeo and Juliet');
-- --
DBCC IND('TEST', 'Books', 0, 1);
-- ================================= --
-- =========== IAM_PAGE ============ --
-- ================================= --
DBCC TRACEON(3604);
GO
DBCC PAGE ('TEST', 1, 120, 3); --<== ZAMIAST 120 WSTAW NR STRONY IAM
-- --
DECLARE @NumberOfRecords INT = 10000;
DECLARE @Title VARCHAR(1000);
WHILE (@NumberOfRecords > 0)
BEGIN
	SET @Title = 'Romeo and Juliet ' + FORMAT(@NumberOfRecords, '00000');
	INSERT INTO Books VALUES (@Title);
	SET @NumberOfRecords = @NumberOfRecords - 1;
END
-- --
DBCC PAGE ('TEST', 1, 120, 3); --<== ZAMIAST 120 WSTAW NR STRONY IAM
-- --
DBCC IND('TEST', 'Books', 0);
-- ================================= --
-- =========== DATA PAGES ========== --
-- ================================= --
DECLARE @ID INT = 1;
DECLARE @Title VARCHAR(1000) = 'Romeo and Juliet 00000';

SELECT 10001  * (DATALENGTH(@ID) + DATALENGTH(@Title)) / 8192.0 AS NumberOfPages;
GO
-- --
DBCC PAGE ('TEST', 1, 251, 3); --<== ZAMIAST 251 WSTAW WYBRANY NR STRONY DATA PAGES (NIE IAM!)
-- --
DBCC PAGE ('TEST', 1, 251, 2); --<== ZAMIAST 251 WSTAW WYBRANY NR STRONY DATA PAGES (NIE IAM!)
-- ================================= --
-- =========== UPDATE ============== --
-- ================================= --
-- WYKONANIE BACKUPU W DOMYŚLNEJ LOKALIZACJI --
BACKUP DATABASE [TEST] TO  DISK = N'TEST.bak' WITH  COPY_ONLY, NOFORMAT, NOINIT,  NAME = N'TEST-Full Database Backup', SKIP, NOREWIND, NOUNLOAD,  STATS = 10
GO
-- --
UPDATE Books SET Title = Title + Title WHERE ID = 2179; --<== ZAMIEŃ ID = 2179 NA ID KTÓRE MASZ NA PODGLĄDANEJ STRONIE
GO
-- --
DBCC PAGE ('TEST', 1, 251, 3); --<== ZAMIAST 251 WSTAW WYBRANY NR STRONY DATA PAGES (NIE IAM!)
-- --
DBCC PAGE ('TEST', 1, 251, 2); --<== ZAMIAST 251 WSTAW WYBRANY NR STRONY DATA PAGES (NIE IAM!)
-- --
UPDATE Books SET Title = REPLICATE(Title, 19) WHERE ID = 2179;
GO
-- RESTOR BAZY TEST --
USE [master]
RESTORE DATABASE [TEST] FROM  DISK = N'TEST.bak' WITH  FILE = 1,  NOUNLOAD,  STATS = 5
GO
USE [TEST]
GO
-- --
UPDATE Books SET Title = Title + Title;
GO
-- --
SELECT OBJECT_NAME(object_id) AS table_name
	,forwarded_record_count
	,page_count
FROM sys.dm_db_index_physical_stats(DB_ID(), OBJECT_ID('Books'), DEFAULT, DEFAULT, 'DETAILED');
-- RESTOR BAZY TEST --
USE [master]
RESTORE DATABASE [TEST] FROM  DISK = N'TEST.bak' WITH  FILE = 1,  NOUNLOAD,  STATS = 5
GO
USE [TEST]
GO
-- --
-- ================================= --
-- ==== Problem 1 - Czytanie ======= --
-- ================================= --
CREATE NONCLUSTERED INDEX IX_ID ON Books (ID);
-- --
SET STATISTICS IO ON;
GO
-- --
SELECT Title FROM Books WHERE ID BETWEEN 2179 AND 2199
GO
-- UWAGA: Jeżeli w waszym planie wyjdzie skanowanie tabeli to zawężcie trochę warunki np: --
SELECT Title FROM Books WHERE ID BETWEEN 2194 AND 2199
GO
-- --
UPDATE Books SET Title = REPLICATE(Title, 19) WHERE ID BETWEEN 2179 AND 2199;
-- --
-- ================================= --
-- ==== Problem 2 - Usuwanie ======= --
-- ================================= --
-- RESTOR BAZY TEST --
USE [master]
RESTORE DATABASE [TEST] FROM  DISK = N'TEST.bak' WITH  FILE = 1,  NOUNLOAD,  STATS = 5
GO
USE [TEST]
GO
-- --
UPDATE Books SET Title = REPLICATE(Title, 19);
GO
-- --
SELECT OBJECT_NAME(object_id) AS table_name
	,forwarded_record_count
	,page_count
FROM sys.dm_db_index_physical_stats(DB_ID(), OBJECT_ID('Books'), DEFAULT, DEFAULT, 'DETAILED');
-- --
DELETE FROM Books;
GO
-- --
SELECT * FROM Books;
GO
-- --
DBCC IND('TEST', 'Books', 0, 1);
-- --
SELECT 
    		t.NAME AS TableName,
    		s.Name AS SchemaName,
    		p.rows AS RowCounts,
    		SUM(a.total_pages) * 8 AS TotalSpaceKB, 
    		SUM(a.used_pages) * 8 AS UsedSpaceKB, 
    		(SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS UnusedSpaceKB
	FROM 
    		sys.tables t
	INNER JOIN      
    		sys.indexes i ON t.OBJECT_ID = i.object_id
	INNER JOIN 
    		sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
	INNER JOIN 
    		sys.allocation_units a ON p.partition_id = a.container_id
	LEFT OUTER JOIN 
    		sys.schemas s ON t.schema_id = s.schema_id
	WHERE 
    		t.NAME NOT LIKE 'dt%' 
    		AND t.is_ms_shipped = 0
    		AND i.OBJECT_ID > 255 
	GROUP BY 
    		t.Name, s.Name, p.Rows
	ORDER BY 
    		TotalSpaceKB DESC
--
-- ==================================== --
-- = High number of Forwarded Records = --
-- ==================================== --
SELECT object_name, counter_name, cntr_value 
FROM sys.dm_os_performance_counters
WHERE object_name LIKE '%Access Methods%' and counter_name = 'Forwarded Records/sec'
OR object_name LIKE '%SQL Statistics%' and counter_name = 'Batch Requests/sec';
-- --
-- ==================================== --
-- = High number of Free Space Scans == --
-- ==================================== --
DBCC PAGE ('TEST', 1, 1, 3);
-- --
SELECT object_name, counter_name, cntr_value 
FROM sys.dm_os_performance_counters
WHERE object_name LIKE '%Access Methods%' and counter_name = 'FreeSpace Scans/sec'
-- PRZED INSERTAMI WYŁĄCZ PODGLĄD PLANU --
INSERT INTO Books VALUES ('Romeo and Juliet 10001');
GO 100
-- --
SELECT object_name, counter_name, cntr_value 
FROM sys.dm_os_performance_counters
WHERE object_name LIKE '%Access Methods%' and counter_name = 'FreeSpace Scans/sec'
-- --
-- ==================================== --
-- ============= Jak żyć? ============= --
-- ==================================== --
ALTER TABLE Books REBUILD;
-- --
-- ==================================== --
-- ======== Jak się rozwieść? ========= --
-- ==================================== --
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
GO
SELECT OBJECT_NAME(object_id) AS table_name
	,forwarded_record_count
	,page_count
	,record_count
	,FORMAT(page_count * 8 / 1024.0, '### ### ##0.00') AS size_MB
	,index_type_desc
	,avg_fragmentation_in_percent
	,fragment_count
	,FORMAT(avg_page_space_used_in_percent, '0.00') AS avg_page_space_used_in_percent
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, DEFAULT, DEFAULT, 'DETAILED')
WHERE index_type_desc = 'HEAP'
ORDER BY forwarded_record_count DESC, size_MB DESC, record_count DESC;
-- --
-- ==================================== --
-- === Wybierz idealnego kandydata ==== --
-- ==================================== --
CREATE TABLE Books
( 
	ID INT NOT NULL INDEX IX_ID CLUSTERED,
	Title VARCHAR(1000) NULL
);
GO
-- --
CREATE TABLE Books
( 
	ID INT IDENTITY(1, 1) CONSTRAINT PK_ID_Books PRIMARY KEY,
	Title VARCHAR(1000) NULL
);
GO 
-- --
CREATE TABLE Books
( 
	ID INT IDENTITY(1, 1) CONSTRAINT UQ_ID_Books UNIQUE,
	Title VARCHAR(1000) NULL
);
GO
-- --
CREATE TABLE Books
( 
	ID INT IDENTITY(1, 1) CONSTRAINT PK_ID_Books PRIMARY KEY CLUSTERED,
	Title VARCHAR(1000) NULL
);
GO
-- --
CREATE TABLE Books
( 
	ID INT IDENTITY(1, 1) CONSTRAINT UQC_ID_Books UNIQUE CLUSTERED,
	Title VARCHAR(1000) NULL
);
GO
