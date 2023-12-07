
CREATE table dbo.test (
ID INT NOT NULL 
,SomeTExt VARCHAR(100) NOT NULL
)

CREATE table Advent2023.dbo.test (
ID INT NOT NULL 
,SomeTExt VARCHAR(100) NOT NULL
)


DROP VIEW IF EXISTS dbo.testView
CREATE VIEW dbo.testView AS
SELECt * from dbo.iris_data

DROP VIEW IF EXISTS dbo.testView_sb
CREATE VIEW  dbo.testView_sb  
WITH SCHEMABINDING  
AS  
SELECT 
[Petal.Length], [Species] 
from dbo.iris_data

CREATE CLUSTERED INDEX index1 ON Advent2023.dbo.iris_species (ID);

CREATE SYNONYM iriske
FOR Advent2023.dbo.iris_data;
GO

CREATE RULE range_rule  
AS   
@range>= $1000 AND @range <$20000;  

TRUNCATE TABLE dbo.iris_data


SELECT *
into #temptable
 FROM
dbo.iris_data






SELECT * FROM dbo.iris_data;
SELECT * FROM dbo.Iris_Data;


-- data types

select 
species
,cast(species as nvarchar(max)) as species_max 
from dbo.iris_species


select 
ID
,cast(ID as TINYINT) as species_max 
from dbo.iris_species


DELETE FROM iris_species WHERE ID =1

UPDATE dbo.iris_species
SET species = 'Setosa'
where ID=1


CREATE FUNCTION udfSpecies (
    @id INT
)
RETURNS TABLE
AS
RETURN
    SELECT 
        ID
        ,species
    FROM
        iris_species
    WHERE
        ID = @id;


SELECT * FROM udfSpecies(1)

ALTER FUNCTION udfSpecies (
    @id INT
)
RETURNS TABLE
AS
RETURN
    SELECT 
        ID
        ,species
    FROM
        iris_species
    WHERE
        ID = @id
        and 2=2;


CREATE FUNCTION udmfSpecies()
    RETURNS @data TABLE (
        sl VARCHAR(50),
        sw VARCHAR(50),
        pl VARCHAR(50),
        pw VARCHAR(50),
        species VARCHAR(50)
    )
AS
BEGIN
    INSERT INTO @data
    SELECT 
     [Sepal.Length]
     ,[Sepal.Width]
     ,[Petal.Length]
     ,[Petal.Width]
     ,[Species]
    FROM
        [dbo].[iris_data];
    RETURN;
END;


SELECT * FROM udmfSpecies