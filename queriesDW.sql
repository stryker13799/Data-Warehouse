#q1	
SELECT SUPPLIER_ID,
       Sum(CASE
             WHEN DATES.Quarter = 1 THEN QUANTITY
             ELSE 0
           end) AS "Quarter 1",
       Sum(CASE
             WHEN DATES.Quarter = 2 THEN QUANTITY
             ELSE 0
           end) AS "Quarter 2",
       Sum(CASE
             WHEN DATES.Quarter = 3 THEN QUANTITY
             ELSE 0
           end) AS "Quarter 3",
       Sum(CASE
             WHEN DATES.QUARTER = 4 THEN QUANTITY
             ELSE 0
           end) AS "Quarter 4",
       Sum(CASE
             WHEN DATES.MONTH = 1 THEN QUANTITY
             ELSE 0
           end) AS 'January',
       Sum(CASE
             WHEN DATES.MONTH = 2 THEN QUANTITY
             ELSE 0
           end) AS 'February',
       Sum(CASE
             WHEN DATES.MONTH = 3 THEN QUANTITY
             ELSE 0
           end) AS 'March',
       Sum(CASE
             WHEN DATES.MONTH = 4 THEN QUANTITY
             ELSE 0
           end) AS 'April',
       Sum(CASE
             WHEN DATES.MONTH = 5 THEN QUANTITY
             ELSE 0
           end) AS 'May',
       Sum(CASE
             WHEN DATES.MONTH = 6 THEN QUANTITY
             ELSE 0
           end) AS 'June',
       Sum(CASE
             WHEN DATES.MONTH = 7 THEN QUANTITY
             ELSE 0
           end) AS 'July',
       Sum(CASE
             WHEN DATES.MONTH = 8 THEN QUANTITY
             ELSE 0
           end) AS 'August',
       Sum(CASE
             WHEN DATES.MONTH = 9 THEN QUANTITY
             ELSE 0
           end) AS 'September',
       Sum(CASE
             WHEN DATES.MONTH = 10 THEN QUANTITY
             ELSE 0
           end) AS 'October',
       Sum(CASE
             WHEN DATES.MONTH = 11 THEN QUANTITY
             ELSE 0
           end) AS 'November',
       Sum(CASE
             WHEN DATES.MONTH = 12 THEN QUANTITY
             ELSE 0
           end) AS 'December'
FROM   FACT
       NATURAL JOIN SUPPLIER
       NATURAL JOIN DATES
GROUP  BY SUPPLIER_ID;


#q2
SELECT STORE_NAME    AS "Store Name",
       PRODUCT_NAME  AS "Product Name",
       Sum(QUANTITY) AS "Total Sales"
FROM   FACT
       NATURAL JOIN STORE
       NATURAL JOIN PRODUCT
GROUP  BY STORE_NAME,
          PRODUCT_NAME
ORDER  BY STORE_NAME,
          PRODUCT_NAME;

#q3.
SELECT PRODUCT_NAME,
       Sum(QUANTITY) AS 'Amount Sold'
FROM   FACT
       NATURAL JOIN PRODUCT
       NATURAL JOIN DATES
WHERE  ( DATES.WEEKEND='Yes' )
GROUP  BY PRODUCT_NAME
ORDER  BY Sum(QUANTITY) DESC
LIMIT  5;
 
#q4
SELECT PRODUCT_NAME,
       Sum(CASE
             WHEN DATES.QUARTER = 1 THEN QUANTITY
             ELSE 0
           end) AS "Quarter 1",
       Sum(CASE
             WHEN DATES.QUARTER = 2 THEN QUANTITY
             ELSE 0
           end) AS "Quarter 2",
       Sum(CASE
             WHEN DATES.QUARTER = 3 THEN QUANTITY
             ELSE 0
           end) AS "Quarter 3",
       Sum(CASE
             WHEN DATES.QUARTER = 4 THEN QUANTITY
             ELSE 0
           end) AS "Quarter 4"
FROM   FACT
       NATURAL JOIN PRODUCT
       NATURAL JOIN DATES
WHERE  DATES.YEAR = 2016
GROUP  BY PRODUCT_NAME;
 
 #q5
SELECT PRODUCT_NAME,
       Sum(CASE
             WHEN Floor(DATES.QUARTER / 3) = 0 THEN QUANTITY
             ELSE 0
           end) AS "First Half of Year",
       Sum(CASE
             WHEN Floor(DATES.QUARTER / 3) = 1 THEN QUANTITY
             ELSE 0
           end) AS "Second Half of Year",
           SUM(QUANTITY) as "Total Yearly Sale"
FROM   FACT
       NATURAL JOIN PRODUCT
       NATURAL JOIN DATES
WHERE  DATES.YEAR = 2016
GROUP  BY PRODUCT_NAME;

#q6
SELECT *
FROM   masterdata
WHERE  product_name = 'Tomatoes';
#Tomatoes has two suppliers, with starkly different prices (1.79 and 19.40), which are not 
#feasibly possible, so this is an anomaly.
 
 #q7
DROP VIEW IF EXISTS STOREANALYSIS_MV;

CREATE VIEW STOREANALYSIS_MV AS
SELECT STORE_ID,
       PRODUCT_ID,
       Sum(SALE_PRICE) AS STORE_TOTAL
FROM   FACT
       NATURAL JOIN PRODUCT
       NATURAL JOIN STORE
GROUP  BY STORE_ID,
          PRODUCT_ID;
          
SELECT * FROM STOREANALYSIS_MV;