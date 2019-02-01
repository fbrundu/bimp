DECLARE @cnt INT;
DECLARE @name VARCHAR(8);
DECLARE @sql_script VARCHAR(max);

set @cnt = 1;
WHILE @cnt < 27
    BEGIN
           set @name = convert(varchar(8), 'UKD33-' + format(@cnt, 'd2'));
           set @sql_script = convert(varchar(max), 'create database "' + @name + '";');
           print @sql_script
           execute (@sql_script);
           SET @cnt = @cnt + 1;
    END;

set @cnt = 1;
WHILE @cnt < 8
    BEGIN
           set @name = convert(varchar(8), 'ITC11-' + format(@cnt, 'd2'));
           set @sql_script = convert(varchar(max), 'create database "' + @name + '";');
           print @sql_script
           execute (@sql_script);
           SET @cnt = @cnt + 1;
    END;

