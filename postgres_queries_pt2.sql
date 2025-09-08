--- SQL Queries for the project part 2
---------------------------------------
--- 1.Find all the staff members working in a vaccination on May 10, 2021 

SELECT S.ssno, S.name, S.phone, S.role, S.vaccinestatus, S.station
FROM workon AS W, vaccinations AS V, staffmembers AS S
WHERE S.ssno = W.staff AND
    W.shiftid = V.shiftid AND
    V.date = '2021-05-10';
    
--- 2.List all the doctors who would be available on Wednesdays in Helsinki. 

SELECT SM.ssno, SM.name
FROM shifts AS S, vaccinationstations AS V, workon AS W, staffmembers AS SM
WHERE SM.role = 'doctor' AND
    V.address LIKE '%HELSINKI' AND
    S.weekday = 'Wednesday' AND
    V.name = SM.station AND
    SM.ssno = W.staff AND
    S.shiftid = W.shiftid;

  
--- 3.a.For each vaccination batch, state the current location of the batch, and the last location in the transportation log

SELECT VB.batchid, location, Log.arrival
FROM vaccinebatch AS VB LEFT JOIN
    (SELECT *
    FROM transportationlog AS T1
    INNER JOIN (
        SELECT batchid AS B, max(datearr) AS max
        FROM transportationlog
        GROUP BY B) AS T2
    ON T1.batchid = T2.B AND T1.datearr = T2.max) AS Log
    ON VB.batchID = Log.B;
  
--- 3.b.List separately the vaccine batch numbers with inconsistent location data, along with the phone number of the hospital or
--- clinic where the vaccine batch should currently be.

SELECT VB.batchid, location AS current_location, Log.arrival AS correct_location, phone AS correct_location_phone
FROM vaccinationstations AS VS, vaccinebatch AS VB,
    (SELECT *
    FROM transportationlog AS T1
    INNER JOIN (
        SELECT batchid AS B, max(datearr) AS max
        FROM transportationlog
        GROUP BY B) AS T2
    ON T1.batchid = T2.B AND T1.datearr = T2.max) AS Log
WHERE VB.batchID = Log.B
    AND VB.location != Log.arrival
    AND Log.arrival = VS.name;

--- 4.
SELECT VP.patient, VE.batchid, VE.station, VE.date
FROM diagnosis AS D, vaccinations AS VE, symptoms AS S, vaccinepatients AS VP
WHERE D.date > '2021-05-10' AND
    S.criticality = 1 AND
    S.name = D.symptom AND
    VP.patient = D.patient AND
    VP.location = VE.station;

--- 5.
CREATE VIEW patientstatus AS
SELECT ssno,
       name,
       count,
       CASE
       WHEN count >= 2 THEN 1
       WHEN count = 1 THEN 0
       WHEN count IS NULL THEN 0
       END AS vaccinestatus
FROM
    (SELECT *
    FROM patients AS P
    LEFT JOIN(
        SELECT patient, COUNT( * ) AS count
        FROM vaccinepatients AS VP
        GROUP BY patient) AS Vaccinated
    ON P.ssno = Vaccinated.patient) AS totaldose;

SELECT * FROM patientstatus;


--- 6a. List the total doses of vaccine in each hospital
SELECT A.arrival, SUM(amount) AS total
FROM vaccinebatch AS V,
    (SELECT VB.batchid, location, Log.arrival
    FROM vaccinebatch AS VB,
        (SELECT *
        FROM transportationlog AS T1
        INNER JOIN (
            SELECT batchid AS B, max(datearr) AS max
            FROM transportationlog
            GROUP BY B) AS T2
            ON T1.batchid = T2.B AND T1.datearr = T2.max) AS Log
            WHERE VB.batchID = Log.B) AS A
WHERE V.batchid = A.batchid
GROUP BY arrival;


--- 6b. List the total doses of each vaccinetype in each hospital
SELECT A.arrival, SUM(amount) AS total, type
FROM vaccinebatch AS V,
    (SELECT VB.batchid, location, Log.arrival
    FROM vaccinebatch AS VB,
        (SELECT *
        FROM transportationlog AS T1
        INNER JOIN (
            SELECT batchid AS B, max(datearr) AS max
            FROM transportationlog
            GROUP BY B) AS T2 
            ON T1.batchid = T2.B AND T1.datearr = T2.max) AS Log
            WHERE VB.batchID = Log.B) AS A
WHERE V.batchid = A.batchid
GROUP BY arrival, type;

--- 7. List the average frequency of the symptoms for each vaccine types

SELECT A.type, A.symptom, symptomcount*100/typecount AS frequency
FROM
(SELECT VB.type, D.symptom, COUNT(*) AS symptomcount
FROM diagnosis AS D,
vaccinepatients AS VP,
vaccinations AS V,
vaccinebatch AS VB
WHERE D.patient = VP.patient AND
VP.date = V.date AND
V.station = VP.location AND
V.batchid = VB.batchid AND
D.date >= VP.date
GROUP BY type, symptom) AS A JOIN
(SELECT VB.type, COUNT(*) AS typecount
FROM diagnosis AS D,
vaccinepatients AS VP,
vaccinations AS V,
vaccinebatch AS VB
WHERE D.patient = VP.patient AND
VP.date = V.date AND
V.station = VP.location AND
V.batchid = VB.batchid
GROUP BY type) AS B ON A.type = B.type;

