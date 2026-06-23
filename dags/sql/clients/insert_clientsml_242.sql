-- INSERT: insert_clientsml_242.sql — pyodbc placeholders
INSERT INTO source.clientsml
       ( productid, contractid, clientid, email, capdata
       , FirstName, LastName, countrycode, country, Estate
       , Ciudad, address, zip, Corpcode, Corp
       , ingreso, egreso, rank, EstatusN, EstatusL
       , createdAt, updatedAt, deletedAt)
VALUES ( ?, ?, ?, ?, ?
       , ?, ?, ?, ?, ?
       , ?, ?, ?, ?, ?
       , ?, ?, ?, ?, ?
       , ?, ?, ?)
