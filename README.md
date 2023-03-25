# Lets Build CDC Pipeline from Microsoft SQL Server into Apache Hudi Transactional Datalake 


![diagram drawio](https://user-images.githubusercontent.com/39345855/227725230-98ed8b96-33ca-4a25-befd-22fecb188754.png)


# Step 1:  Create SQL Server Database as shown in videos 

# Step 2: Create Database called edw 
```
CREATE DATABASE edw;
```

# Step 3: Create Table called invoice 
```

CREATE TABLE invoice (
    invoiceid INT NOT NULL,
    itemid INT NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10, 2),
    quantity INT,
    orderdate DATE,
    destinationstate VARCHAR(50),
    shippingtype VARCHAR(50),
    referral VARCHAR(50),
    PRIMARY KEY (invoiceid, itemid)
);

```


# Step 4: Insert some data into table by running main.py 

* edit your Cluster connection string in code 
```
pip3 install -r requirements.txt

python main.py 
```

#### Enable CDC on this table by executing following commands 
```
EXEC msdb.dbo.rds_cdc_enable_db 'edw'

EXECUTE sys.sp_cdc_enable_table
  @source_schema = N'dbo',
  @source_name = N'invoice',
  @role_name = NULL
GO

EXECUTE sys.sp_cdc_change_job
  @job_type = N'capture' ,
  @pollinginterval = 3599

```



# Step 5: Create DMS Source and Target and Task as shown in video guide 



# Step 6: Create your Batch Glue ETC which will take data from raw source and create transcational datalake 
![image](https://user-images.githubusercontent.com/39345855/227725475-8ee8b10d-c2d7-4e28-bc8e-5d3d12757e95.png)


# Step 7: Query the data via Athena
![image](https://user-images.githubusercontent.com/39345855/227725531-e9889b56-d33c-4456-9696-4f0695bf7e04.png)


