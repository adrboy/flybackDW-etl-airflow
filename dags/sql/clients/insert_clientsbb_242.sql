-- INSERT: insert_clientsbb_242.sql — pyodbc placeholders
INSERT INTO source.clientsbb
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
