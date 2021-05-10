--Creamos la base de datos si no existe y entramos en ella
CREATE DATABASE IF NOT EXISTS jmontero_tfm;
USE jmontero_tfm;


--Creamos la tabla de identidades si no existe ya
CREATE EXTERNAL TABLE IF NOT EXISTS identity_raw(data String)
LOCATION "/user/jmontero/hive/data/identities"
TBLPROPERTIES("skip.header.line.count"="1");
LOAD DATA INPATH "/user/jmontero/TFM/Raw/Static/Test/Identity/*" INTO TABLE identity_raw;
LOAD DATA INPATH "/user/jmontero/TFM/Raw/Static/Train/Identity/*" INTO TABLE identity_raw;



--Creamos la tabla si no existiese en la que obtenemos los campos
--que nos interesan
CREATE TABLE IF NOT EXISTS identity_final AS 
SELECT 
split(data,",")[0] as TransactionID, 
split(data,",")[39] as DeviceType, 
split(data, ",")[40] as DeviceInfo 
from identity_raw;



--Creamos la tabla de transacciones si no existe ya
CREATE EXTERNAL TABLE IF NOT EXISTS transactions_raw(data String)
LOCATION "/user/jmontero/hive/data/identities"
TBLPROPERTIES("skip.header.line.count"="1");
LOAD DATA INPATH "/user/jmontero/TFM/Raw/Static/Train/Transaction/*" INTO TABLE transactions_raw;



--Creamos la tabla si existiese en la que obtenemos los campos
--que nos interesan
CREATE TABLE IF NOT EXISTS transactions_final AS 
SELECT 
split(data,",")[0] as TransactionID, 
split(data,",")[1] as isFraud,
split(data,",")[3] as TransactionAmt, 
split(data,",")[4] as ProductCD,
split(data,",")[8] as card4,
split(data,",")[10] as card6,  
split(data,",")[15] as P_emaildomain,
split(data,",")[16] as R_emaildomain
from transactions_raw;



--Creamos la tabla de resultados si no existe ya
CREATE EXTERNAL TABLE IF NOT EXISTS predictions_raw(TransactionID int, isFraud double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
LOCATION "/user/jmontero/hive/data/predictions"
TBLPROPERTIES("skip.header.line.count"="1");
LOAD DATA INPATH "/user/jmontero/TFM/Processed/Static/*" INTO TABLE predictions_raw;
LOAD DATA INPATH "/user/jmontero/TFM/Processed/Dynamic/*" INTO TABLE predictions_raw;



--Borramos la tabla si existiese y creamos una en la que obtenemos los campos
--que nos interesan
CREATE TABLE IF NOT EXISTS predictions_final AS 
SELECT * from predictions_raw;




