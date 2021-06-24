USE MOVIES

SELECT *
FROM Movie
WHERE studioName = 'Disney' AND year = 1990
ORDER BY length, title ASC

-- Напишете заявка, която извежда адресът на студио ‘MGM’
SELECT Address
FROM Studio
WHERE name = 'MGM'
--------------------------------------------------------------------------------------------------
-- Напишете заявка, която извежда рождената дата на актрисата Sandra Bullock
USE MOVIES
SELECT Birthdate
FROM MOVIESTAR
WHERE name = 'Sandra Bullock'
--------------------------------------------------------------------------------------------------
-- Напишете заявка, която извежда имената на всички актьори, които са участвали
-- във филм през 1980, в заглавието на които има думата ‘Empire’
SELECT STARNAME
FROM STARSIN
WHERE movieyear = 1980 AND movietitle like 'Empire%'
--------------------------------------------------------------------------------------------------
-- Напишете заявка, която извежда имената всички изпълнители на филми с нетна
-- стойност над 10 000 000 долара
SELECT name
FROM MOVIEEXEC
WHERE networth >= 10000000
--------------------------------------------------------------------------------------------------
-- Напишете заявка, която извежда имената на всички актьори, които са мъже или
-- живеят в Malibu
SELECT name
from MOVIESTAR
WHERE address like 'Malibu%' or gender = 'M'
--------------------------------------------------------------------------------------------------
USE PC
-- Напишете заявка, която извежда номер на модел, честота на процесора (speed) и
-- размер на диска (hd) за всички компютри с цена по-малка от 1200 долара.
-- Задайте псевдоними за атрибутите честота и размер на диска, съответно MHz и
-- GB
SELECT model, speed as MHZ, hd as GB
FROM LAPTOP
WHERE price < 1200
--------------------------------------------------------------------------------------------------
-- Напишете заявка, която извежда моделите и цените в евро на всички лаптопи.
-- Нека приемем, че в базата цените се съхраняват в долари, а курсът е 1.1 долара
-- за евро. Да се изведат първо най-евтините лаптопи
SELECT model, price*1.1 as newPrice
FROM laptop
ORDER by price ASC
--------------------------------------------------------------------------------------------------
-- Напишете заявка, която извежда номер на модел, размер на паметта, размер на
-- екран за лаптопите, чиято цена е по-голяма от 1000 долара
SELECT model, ram, screen
FROM laptop
WHERE price > 1000
--------------------------------------------------------------------------------------------------
-- Напишете заявка, която извежда всички цветни принтери
SELECT *
FROM printer
WHERE color like 'y'
--------------------------------------------------------------------------------------------------
-- Напишете заявка, която извежда номер на модел, честота и размер на диска за
-- тези компютри с DVD 12x или 16x и цена по-малка от 2000 долара
SELECT model, speed,hd
FROM pc
WHERE (cd = '12x' or cd = '16x') and price < 2000
--------------------------------------------------------------------------------------------------
-- Нека рейтингът на един лаптоп се определя по следната формула: честота на
-- процесора + размер на RAM паметта + 10*размер на екрана. Да се изведат
-- кодовете, моделите и рейтингите на всички лаптопи. Резултатът да бъде
-- подреден така, че първо да бъдат лаптопите с най-висок рейтинг, а продукти с
-- еднакъв рейтинг да бъдат подредени по код.
SELECT code, model, l.speed + l.ram + 10*l.screen as rating
FROM laptop l
ORDER by rating DESC, code ASC
--------------------------------------------------------------------------------------------------
USE SHIPS
--------------------------------------------------------------------------------------------------
-- Напишете заявка, която извежда името на класа и страната за всички класове с
-- брой на оръдията по-малък от 10
SELECT CLASS, Country
from classes
where numguns < 10
--------------------------------------------------------------------------------------------------
-- Напишете заявка, която извежда имената на всички кораби, пуснати на вода
-- преди 1918. Задайте псевдоним на колоната shipName
select * 
from ships
where LAUNCHED < 1918
--------------------------------------------------------------------------------------------------
-- Напишете заявка, която извежда имената на корабите потънали в битка и
-- имената на битките в които са потънали
SELECT ship, battle
FROM OUTCOMES
WHERE result = 'sunk'
--------------------------------------------------------------------------------------------------
-- Напишете заявка, която извежда имената на корабите с име, съвпадащо с името
-- на техния клас
SELECT name
FROM ships s
WHERE s.name LIKE s.class
--------------------------------------------------------------------------------------------------
-- Напишете заявка, която извежда имената на всички кораби започващи с буквата R
SELECT name
FROM ships
WHERE name LIKE 'R%'
--------------------------------------------------------------------------------------------------
-- Напишете заявка, която извежда имената на всички кораби, чието име е
-- съставено от точно две думи.
SELECT name
FROM ships
WHERE LEN(name)-LEN(REPLACE(name,' ',''))=1

USE movies
-- 1.1. Напишете заявка, която извежда имената на актьорите мъже, участвали в ‘Terms
--      of Endearment’
SELECT DISTINCT NAME
FROM MOVIESTAR
	JOIN STARSIN ON NAME = STARNAME
WHERE MOVIETITLE = 'Terms of Endearment' AND GENDER = 'M'
---------------------------------------------------------------------------------------
-- 1.2. Напишете заявка, която извежда имената на актьорите, участвали във филми,
--      продуцирани от ‘MGM’ през 1995 г.
SELECT DISTINCT STARNAME
FROM MOVIE 
	JOIN STARSIN ON TITLE = MOVIETITLE AND YEAR = MOVIEYEAR
WHERE STUDIONAME = 'MGM' AND YEAR = 1995
---------------------------------------------------------------------------------------
-- 1.3. Напишете заявка, която извежда името на президента на ‘MGM’
SELECT me.NAME
FROM STUDIO s
	JOIN MOVIEEXEC me ON s.PRESC# = me.CERT#
WHERE s.NAME = 'MGM'
---------------------------------------------------------------------------------------
-- 1.4. Напишете заявка, която извежда имената на всички филми с дължина, по-голяма
--      от дължината на филма ‘Star Wars’
-- (В таблицата MOVIE, може да има различни филми с едно и също име, ако са правени 
-- в различни години). Предвид, че в условието се говори за единствен филм - смятаме,
-- че става въпрос за този, който е сниман през 1977 и го указваме в заявката.
---------------------------------------------------------------------------------------
SELECT m1.TITLE
FROM MOVIE m1
    JOIN MOVIE m2 ON m2.TITLE = 'Star Wars' AND m2.YEAR = 1977
WHERE m1.LENGTH > m2.LENGTH
---------------------------------------------------------------------------------------
USE pc
-- 2.1. Напишете заявка, която извежда производителя и честотата на процесора на тези
--      лаптопи с размер на диска поне 9 GB
SELECT maker, speed
FROM product p
    JOIN laptop l ON p.model = l.model
WHERE hd >= 9
---------------------------------------------------------------------------------------
-- 2.2. Напишете заявка, която извежда номер на модел и цена на всички продукти,
--      произведени от производител с име ‘B’. Сортирайте резултата така, че първо да
--      се изведат най-скъпите продукти
SELECT u.model, u.price
FROM product p
    JOIN (SELECT model, price FROM pc
          UNION
          SELECT model, price FROM laptop
          UNION
          SELECT model, price FROM printer) u ON p.model = u.model
WHERE p.maker = 'B'
ORDER BY price DESC
РЕШЕНИЕ 2:
		SELECT pc.model, price 
		FROM pc JOIN product ON pc.model = product.model 
		WHERE product.maker = 'B'
		UNION
		SELECT laptop.model, price 
		FROM laptop JOIN product ON laptop.model = product.model 
		WHERE product.maker = 'B'
		UNION
		SELECT printer.model, price 
		FROM printer JOIN product ON printer.model = product.model 
		WHERE product.maker = 'B'
		ORDER BY price DESC
---------------------------------------------------------------------------------------
-- 2.3. Напишете заявка, която извежда размерите на тези дискове, които се предлагат в
--      поне два компютъра
SELECT DISTINCT p1.hd
FROM pc p1
    JOIN pc p2 ON p1.code != p2.code
WHERE p1.hd = p2.hd
---------------------------------------------------------------------------------------
-- 2.4. Напишете заявка, която извежда всички двойки модели на компютри, които имат
--      еднаква честота и памет. Двойките трябва да се показват само по веднъж,
--      например само (i, j), но не и (j, i)
SELECT p1.model, p2.model
FROM pc p1
    JOIN pc p2 ON p1.model < p2.model
WHERE p1.ram = p2.ram AND p1.speed = p2.speed
---------------------------------------------------------------------------------------
-- 2.5. Напишете заявка, която извежда производителите на поне два различни
--      компютъра с честота на процесора поне 500 MHz.
SELECT DISTINCT p1.maker
FROM pc pc1
    JOIN pc pc2 ON pc1.code != pc2.code
    JOIN product p1 ON pc1.model = p1.model
    JOIN product p2 ON pc2.model = p2.model
WHERE pc1.speed >= 500 AND pc2.speed >= 500 AND p1.maker = p2.maker
----------------------------------------------------------------------------------
USE ships
----------------------------------------------------------------------------------
-- 3.1. Напишете заявка, която извежда името на корабите, по-тежки (displacement) от
--      35000
SELECT s.NAME
FROM SHIPS s
	JOIN CLASSES c ON s.CLASS = c.CLASS
WHERE c.DISPLACEMENT > 35000
----------------------------------------------------------------------------------
-- 3.2. Напишете заявка, която извежда имената, водоизместимостта и броя оръдия на
--      всички кораби, участвали в битката при ‘Guadalcanal’
SELECT s.NAME, c.DISPLACEMENT, c.NUMGUNS
FROM SHIPS s
    JOIN CLASSES c ON s.CLASS = c.CLASS
    JOIN OUTCOMES o ON s.NAME = o.SHIP
WHERE o.BATTLE = 'Guadalcanal'
----------------------------------------------------------------------------------
-- 3.3. Напишете заявка, която извежда имената на тези държави, които имат класове
--      кораби от тип ‘bb’ и ‘bc’ едновременно
SELECT DISTINCT c1.COUNTRY
FROM CLASSES c1 
    JOIN CLASSES c2 ON c1.COUNTRY = c2.COUNTRY
WHERE c1.TYPE = 'bb' and c2.TYPE = 'bc'
----------------------------------------------------------------------------------
-- 3.4. Напишете заявка, която извежда имената на тези кораби, които са били
--      повредени в една битка, но по-късно са участвали в друга битка.
SELECT o1.SHIP
FROM OUTCOMES o1
    JOIN BATTLES b1 ON o1.BATTLE = b1.NAME
    JOIN OUTCOMES o2 ON o1.SHIP = o2.SHIP
    JOIN BATTLES b2 ON o2.BATTLE = b2.NAME
WHERE o1.RESULT = 'damaged' AND b1.DATE < b2.DATE

-- 1. За всяка филмова звезда да се изведе името, рождената дата и с кое
--    студио е записвала най-много филми. (Ако има две студиа с еднакъв 
--    брой филми, да се изведе кое да е от тях)
SELECT ms.NAME, ms.BIRTHDATE, (SELECT TOP 1 m.STUDIONAME
                               FROM STARSIN si
                                   JOIN MOVIE m ON si.MOVIETITLE = m.TITLE AND si.MOVIEYEAR = m.YEAR
                               WHERE si.STARNAME = ms.NAME
                               GROUP BY m.STUDIONAME
                               ORDER BY COUNT(*) DESC) AS STUDIO
FROM MOVIESTAR ms
--------------------------------------------------------------------------------------
-- 2. Да се изведат всички производители, за които средната цена на 
--    произведените компютри е по-ниска от средната цена на техните лаптопи.
SELECT p.maker
FROM product p
    JOIN pc pc1 ON p.model = pc1.model
GROUP BY p.maker
HAVING AVG(pc1.price) < (SELECT AVG(l.price)
                         FROM product p2
                            JOIN laptop l ON p2.model = l.model
                         WHERE p2.maker = p.maker)
--------------------------------------------------------------------------------------
-- 3. Един модел компютри може да се предлага в няколко конфигурации 
--    с евентуално различна цена. Да се изведат тези модели компютри,
--    чиято средна цена (на различните му конфигурации) е по-ниска
--    от най-евтиния лаптоп, произвеждан от същия производител.
SELECT p.model
FROM product p
    JOIN pc pc1 ON p.model = pc1.model
GROUP BY p.model, p.maker
HAVING AVG(pc1.price) < (SELECT MIN(l.price)
                         FROM product p2
                            JOIN laptop l ON p2.model = l.model
                         WHERE p2.maker = p.maker)
--------------------------------------------------------------------------------------
-- 4. Битките, в които са участвали поне 3 кораба на една и съща страна.
SELECT DISTINCT o.BATTLE
FROM OUTCOMES o
    JOIN SHIPS s ON o.SHIP = s.NAME
    JOIN CLASSES c ON c.CLASS = s.CLASS
GROUP BY o.BATTLE, c.COUNTRY
HAVING COUNT(*) >= 3
--------------------------------------------------------------------------------------
-- 5. За всеки кораб да се изведе броят на битките, в които е бил увреден.
--    Ако корабът не е участвал в битки или пък никога не е бил
--    увреждан, в резултата да се вписва 0.
SELECT s.NAME, COUNT(o.RESULT)
FROM SHIPS s
    LEFT OUTER JOIN OUTCOMES o ON s.NAME = o.SHIP AND o.RESULT = 'damaged'
GROUP BY s.NAME
--------------------------------------------------------------------------------------
-- 6. За всеки клас да се изведе името, държавата и първата година, в която 
--    е пуснат кораб от този клас
SELECT c.CLASS, c.COUNTRY, MIN(s.LAUNCHED) AS FIRST_YEAR
FROM CLASSES c
    LEFT OUTER JOIN SHIPS s ON c.CLASS = s.CLASS
GROUP BY c.CLASS, c.COUNTRY
--------------------------------------------------------------------------------------
-- 6.1. но да извличаме и името на корабите
SELECT c.CLASS, s.NAME, s.LAUNCHED
FROM CLASSES c
    LEFT OUTER JOIN SHIPS s ON c.CLASS = s.CLASS
    LEFT OUTER JOIN (SELECT CLASS, MIN(LAUNCHED) MINLAUNCHED
                     FROM SHIPS 
                     GROUP BY CLASS) y ON c.CLASS = y.CLASS
WHERE s.LAUNCHED IS NULL OR s.LAUNCHED = y.MINLAUNCHED
--------------------------------------------------------------------------------------
-- 7. За всяка държава да се изведе броят на корабите и броят на потъналите 
--    кораби. Всяка от бройките може да бъде и нула.
SELECT c.COUNTRY, COUNT(s.NAME), COUNT(o.RESULT)
FROM CLASSES c
    LEFT OUTER JOIN SHIPS s ON c.CLASS = s.CLASS
    LEFT OUTER JOIN OUTCOMES o ON o.SHIP = s.NAME AND o.RESULT = 'sunk'
GROUP BY c.COUNTRY
--------------------------------------------------------------------------------------
-- 8. Намерете за всеки клас с поне 3 кораба броя на корабите от 
--    този клас, които са с резултат ok.
SELECT s.CLASS, COUNT(DISTINCT o.SHIP)
FROM SHIPS s
    LEFT OUTER JOIN OUTCOMES o ON s.NAME = o.SHIP AND o.RESULT = 'ok'
GROUP BY s.CLASS
HAVING COUNT(DISTINCT s.NAME) >= 3
--------------------------------------------------------------------------------------
-- 9. За всяка битка да се изведе името на битката, годината на 
--    битката и броят на потъналите кораби, броят на повредените
--    кораби и броят на корабите без промяна.
SELECT  o.BATTLE, 
        YEAR(b.DATE) BATTLE_YEAR, 
        SUM(CASE o.RESULT
            WHEN 'sunk' THEN 1
            ELSE 0
        END) AS SUNK_COUNT,
        SUM(CASE o.RESULT
            WHEN 'damaged' THEN 1
            ELSE 0
        END) AS DAMAGED_COUNT,
        SUM(CASE o.RESULT
            WHEN 'ok' THEN 1
            ELSE 0
        END) AS OK_COUNT
FROM OUTCOMES o 
    JOIN BATTLES b ON o.BATTLE = b.NAME
GROUP BY o.BATTLE, b.DATE
--------------------------------------------------------------------------------------
-- 10. Да се изведат имената на корабите, които са участвали в битки в
--     продължение поне на две години.
SELECT o.SHIP
FROM BATTLES b
    JOIN OUTCOMES o ON b.NAME = o.BATTLE
GROUP BY o.SHIP
HAVING YEAR(MAX(b.DATE)) - YEAR(MIN(b.DATE)) >= 1
--------------------------------------------------------------------------------------
-- 11. За всеки потънал кораб колко години са минали от пускането му на вода 
--     до потъването.
SELECT o.SHIP, YEAR(b.DATE) - s.LAUNCHED AS YEARS_PASSED
FROM OUTCOMES o
    JOIN BATTLES b ON o.BATTLE = b.NAME
    JOIN SHIPS s ON s.NAME = o.SHIP
WHERE o.RESULT = 'sunk'
-- В базата има дефектни данни за Hood и "Yamashiro
--------------------------------------------------------------------------------------
-- 12. Имената на класовете, за които няма кораб, пуснат на вода след 1921 г., 
--     но имат пуснат поне един кораб. 
SELECT CLASS
FROM SHIPS 
GROUP BY CLASS
HAVING MAX(LAUNCHED) <= 1921
USE movies
---------------------------------------------------------------------------------------
-- 1.1. Напишете заявка, която извежда имената на актрисите, които са също и
--      продуценти с нетна стойност по-голяма от 10 милиона.
SELECT NAME
FROM MOVIESTAR
WHERE GENDER= 'F' AND NAME IN (SELECT NAME FROM MOVIEEXEC WHERE NETWORTH > 10000000)
---------------------------------------------------------------------------------------
-- 1.2. Напишете заявка, която извежда имената на тези актьори (мъже и
--      жени), които не са продуценти.
SELECT NAME
FROM MOVIESTAR
WHERE NAME NOT IN (SELECT NAME FROM MOVIEEXEC)
---------------------------------------------------------------------------------------
-- 1.3. Напишете заявка, която извежда имената на всички филми с
-- дължина по-голяма от дължината на филма 'Gone With the Wind'
-- TITLE не е достатъчно за уникално идентифициране на филм в таблицата MOVIE.
-- Необходима е двойка стойности (име на филм, година).
---------------------------------------------------------------------------------------
-- Интерпретация 1: филмите с дължина по-голяма от дължината на 'Gone With The Wind',
-- правен през 1938
SELECT TITLE
FROM MOVIE
WHERE LENGTH > (SELECT LENGTH FROM MOVIE WHERE TITLE = 'Gone With the Wind' AND YEAR = 1938)
---------------------------------------------------------------------------------------
-- Интерпретация 2: филмите с дължина по-голяма от дължината всички филми със заглавие 
-- 'Gone With The Wind'
SELECT TITLE
FROM MOVIE
WHERE LENGTH > ALL (SELECT LENGTH FROM MOVIE WHERE TITLE = 'Gone With the Wind')
-- Това ще работи само когато в базата има само един филм с име 'Gone With the Wind'. 
-- Иначе ще връща грешка.
SELECT TITLE
FROM MOVIE
WHERE LENGTH > (SELECT LENGTH FROM MOVIE WHERE TITLE = 'Gone With the Wind')
---------------------------------------------------------------------------------------
-- 1.4. Напишете заявка, която извежда имената на продуцентите и имената на
--      продукциите за които стойността им е по-голяма от продукциите
--      на продуцента 'Merv Griffin'
--      В базата MOVIES няма стойност на продукции и затова интерпретираме това 
--      условие като:
--      Имената на продуцентите и продукциите, правени от продуценти с NETWORTH
--      по-голям от NETWORTH-a на 'Merv Griffin'
SELECT NAME, TITLE
FROM MOVIE m
	JOIN MOVIEEXEC me ON m.PRODUCERC# = me.CERT#
WHERE me.NETWORTH > (SELECT NETWORTH FROM MOVIEEXEC WHERE NAME = 'Merv Griffin')
--------------------------------------------------------------------------------------
USE pc
--------------------------------------------------------------------------------------
-- 2.1. Напишете заявка, която извежда производителите на персонални компютри с
--      честота поне 500.
SELECT DISTINCT maker
FROM product
WHERE model IN (SELECT model FROM pc WHERE speed >= 500)
--------------------------------------------------------------------------------------
-- 2.2. Напишете заявка, която извежда принтерите с най-висока цена.
SELECT *
FROM printer
WHERE price >= ALL (SELECT price FROM printer)
--------------------------------------------------------------------------------------
-- 2.3. Напишете заявка, която извежда лаптопите, чиято честота е по-ниска от
--      честотата на който и да е персонален компютър (по-ниска от поне един).
SELECT *
FROM laptop
WHERE speed < ANY (SELECT speed FROM pc)
--------------------------------------------------------------------------------------
-- по-ниска от тази на всички pc-та:
SELECT *
FROM laptop
WHERE speed < ALL (SELECT speed FROM pc)
--------------------------------------------------------------------------------------
-- 2.4. Напишете заявка, която извежда модела на продукта (PC, лаптоп
--      или принтер) с най-висока цена.
SELECT DISTINCT model
FROM (SELECT model, price FROM pc
      UNION
      SELECT model, price FROM laptop
      UNION
      SELECT model, price FROM printer) t
WHERE price >= ALL (SELECT price FROM pc
                    UNION
                    SELECT price FROM laptop
                    UNION
                    SELECT price FROM printer)
--------------------------------------------------------------------------------------
-- 2.5. Напишете заявка, която извежда производителя на цветния
--      принтер с най-ниска цена.
SELECT DISTINCT maker
FROM product
WHERE model IN 
	(SELECT model
     FROM printer
     WHERE color = 'y' 
		AND price <= ALL (SELECT price FROM printer WHERE color = 'y'))
--------------------------------------------------------------------------------------
SELECT DISTINCT maker
FROM product p 
	JOIN (SELECT model
          FROM printer
          WHERE color = 'y' 
		     AND price <= ALL (SELECT price FROM printer WHERE color = 'y')
			 ) cm ON p.model = cm.model
-- 2.6. Напишете заявка, която извежда производителите на тези
--      персонални компютри с най-малко RAM памет, които имат най-бързи процесори.
SELECT DISTINCT maker
FROM product
WHERE model IN 
	(SELECT model
     FROM pc p1
     WHERE ram <= ALL (SELECT ram FROM pc)
           AND speed >= ALL (SELECT speed FROM pc p2 WHERE p1.ram = p2.ram))
--------------------------------------------------------------------------------------
USE ships
--------------------------------------------------------------------------------------
-- 3.1. Напишете заявка, която извежда страните, чиито кораби са с
--      най-голям брой оръжия.
SELECT DISTINCT COUNTRY
FROM CLASSES
WHERE NUMGUNS >= ALL (SELECT NUMGUNS FROM CLASSES)
--------------------------------------------------------------------------------------
-- 3.2. Напишете заявка, която извежда класовете, за които поне един
--      от корабите им е потънал в битка.
SELECT DISTINCT CLASS
FROM SHIPS
WHERE NAME IN (SELECT SHIP FROM OUTCOMES WHERE RESULT = 'sunk')
--------------------------------------------------------------------------------------
-- 3.3. Напишете заявка, която извежда имената на корабите с 16 инчови
--      оръдия (bore).
SELECT NAME
FROM SHIPS
WHERE CLASS IN (SELECT CLASS FROM CLASSES WHERE BORE = 16)
--------------------------------------------------------------------------------------
-- 3.4. Напишете заявка, която извежда имената на битките, в които са участвали
--      кораби от клас ‘Kongo’.
SELECT BATTLE
FROM OUTCOMES
WHERE SHIP IN (SELECT NAME FROM SHIPS WHERE CLASS = 'Kongo')
--------------------------------------------------------------------------------------
-- 3.5. Напишете заявка, която извежда имената на корабите, чиито брой оръдия е
--      най-голям в сравнение с корабите със същия калибър оръдия (bore).
SELECT NAME
FROM SHIPS s
	JOIN CLASSES c1 ON s.CLASS = c1.CLASS
WHERE c1.NUMGUNS >= ALL (SELECT NUMGUNS FROM CLASSES c2 WHERE c1.BORE = c2.BORE)
--------------------------------------------------------------------------------------
SELECT NAME
FROM SHIPS  
WHERE CLASS IN (SELECT CLASS
				FROM CLASSES c1
				WHERE c1.NUMGUNS >= ALL (SELECT NUMGUNS FROM CLASSES c2 WHERE c1.BORE = c2.BORE))
-- 1.1. Напишете заявка, която за всеки филм, по-дълъг от 120 минути,
--      извежда заглавие, година, име и адрес на студио.
USE movies
--------------------------------------------------------------------------------------
SELECT TITLE, YEAR, NAME, ADDRESS
FROM MOVIE
    JOIN STUDIO ON STUDIONAME = NAME
WHERE LENGTH > 120
-- 1.2. Напишете заявка, която извежда името на студиото и имената на
--      актьорите, участвали във филми, произведени от това студио,
--      подредени по име на студио.
SELECT DISTINCT STUDIONAME, STARNAME
FROM MOVIE
    JOIN STARSIN ON TITLE = MOVIETITLE AND YEAR = MOVIEYEAR
ORDER BY STUDIONAME
--------------------------------------------------------------------------------------
-- 1.3. Напишете заявка, която извежда имената на продуцентите на филмите,
--      в които е играл Harrison Ford.

SELECT DISTINCT e.NAME
FROM MOVIE m
    JOIN STARSIN s ON m.TITLE = s.MOVIETITLE AND m.YEAR = s.MOVIEYEAR
    JOIN MOVIEEXEC e ON e.CERT# = m.PRODUCERC#
WHERE STARNAME = 'Harrison Ford'
--------------------------------------------------------------------------------------
-- 1.4. Напишете заявка, която извежда имената на актрисите, играли във
--      филми на MGM.
SELECT DISTINCT ms.NAME
FROM MOVIE m
    JOIN STARSIN s ON m.TITLE = s.MOVIETITLE AND m.YEAR = s.MOVIEYEAR
    JOIN MOVIESTAR ms ON s.STARNAME = ms.NAME
WHERE STUDIONAME = 'MGM' AND ms.GENDER = 'F'
--------------------------------------------------------------------------------------
-- 1.5. Напишете заявка, която извежда името на продуцента и имената на
--      филмите, продуцирани от продуцента на 'Star Wars'.
SELECT *
FROM MOVIE m
JOIN MOVIEEXEC e ON e.CERT# = m.PRODUCERC#
WHERE m.PRODUCERC# = (SELECT PRODUCERC# 
                      FROM MOVIE 
					  WHERE TITLE = 'Star Wars' AND YEAR = 1977)
--------------------------------------------------------------------------------------
SELECT e.NAME, m.TITLE
FROM MOVIE m
    JOIN MOVIEEXEC e ON e.CERT# = m.PRODUCERC#
    JOIN MOVIE m2 ON m2.TITLE = 'Star Wars' AND m2.YEAR = 1977
WHERE m.PRODUCERC# = m2.PRODUCERC#
--------------------------------------------------------------------------------------
-- 1.6. Напишете заявка, която извежда имената на актьорите не участвали в
--      нито един филм.
SELECT NAME
FROM MOVIESTAR
LEFT OUTER JOIN starsin ON NAME = STARNAME
WHERE STARNAME IS NULL
--------------------------------------------------------------------------------------
-- 2.1. Напишете заявка, която извежда производител, модел и тип на продукт
--      за тези производители, за които съответния продукт не се продава
--      (няма го в таблиците PC, лаптоп или принтер).
SELECT p.maker, p.model, type
FROM product p
    LEFT OUTER JOIN (SELECT model FROM pc
                     UNION
                     SELECT model FROM laptop
                     UNION
                     SELECT model FROM printer) ct ON p.model = ct.model
WHERE ct.model IS NULL
--------------------------------------------------------------------------------------
SELECT maker, model, type
FROM product
WHERE model NOT IN (SELECT model FROM pc)
    AND model NOT IN (SELECT model FROM laptop)
    AND model NOT IN (SELECT model FROM printer)
--------------------------------------------------------------------------------------
-- 3.1. Напишете заявка, която за всеки кораб извежда името му, държавата,
--      броя оръдия и годината на пускане (launched).
SELECT s.NAME, c.COUNTRY, c.NUMGUNS, s.LAUNCHED
FROM SHIPS s
    JOIN CLASSES c ON s.CLASS = c.CLASS
--------------------------------------------------------------------------------------
-- 3.2. Напишете заявка, която извежда имената на корабите, участвали в битка
--      от 1942г.
SELECT DISTINCT SHIP
FROM OUTCOMES o
    JOIN BATTLES b ON o.BATTLE = b.NAME
WHERE YEAR(date) = 1942
--------------------------------------------------------------------------------------
-- 3.3. За всяка страна изведете имената на корабите, които никога не са
--      участвали в битка.
SELECT c.COUNTRY, s.NAME
FROM CLASSES c
    JOIN SHIPS s ON c.class = s.class
    LEFT OUTER JOIN OUTCOMES o ON s.NAME = o.SHIP
WHERE o.SHIP IS NULL
--------------------------------------------------------------------------------------
-- Допълнителна задача
-- Имената на класовете, за които няма кораб, пуснат на вода
-- (launched) след 1921 г. Ако за класа няма пуснат никакъв кораб,
-- той също трябва да излезе в резултата.
SELECT CLASS
FROM CLASSES
WHERE CLASS NOT IN (SELECT CLASS FROM SHIPS WHERE LAUNCHED > 1921)
--------------------------------------------------------------------------------------
SELECT c.CLASS
FROM CLASSES c
    LEFT OUTER JOIN SHIPS s ON c.CLASS = s.CLASS AND s.LAUNCHED > 1921
WHERE s.NAME IS NULL
-- 1.1. Напишете заявка, която извежда средната честота на компютрите
SELECT AVG(speed)
FROM pc
--------------------------------------------------------------------------------------
-- 1.2. Напишете заявка, която извежда средния размер на екраните на 
--      лаптопите  за всеки производител
SELECT maker, AVG(screen)
FROM laptop
    JOIN product ON laptop.model = product.model
GROUP BY maker
--------------------------------------------------------------------------------------
-- 1.3. Напишете заявка, която извежда средната честота на лаптопите 
--      с цена над 1000
SELECT AVG(speed)
FROM laptop
WHERE price > 1000
--------------------------------------------------------------------------------------
-- 1.4. Напишете заявка, която извежда средната цена на компютрите 
--      произведени от производител ‘A’
SELECT AVG(price) AveragePrice
FROM pc
    JOIN product ON pc.model = product.model
WHERE maker = 'A'
--------------------------------------------------------------------------------------
-- 1.5. Напишете заявка, която извежда средната цена на компютрите 
--      и лаптопите за производител ‘B’
SELECT AVG(price) AveragePrice
FROM (SELECT price
      FROM product p 
          JOIN pc ON p.model = pc.model
      WHERE maker = 'B'
      UNION ALL
      SELECT price
      FROM product p 
          JOIN laptop ON p.model = laptop.model
      WHERE maker = 'B') u
--------------------------------------------------------------------------------------
-- 1.6. Напишете заявка, която извежда средната цена на компютрите 
--      според различните им честоти
SELECT speed, AVG(price)
FROM pc
GROUP BY speed
--------------------------------------------------------------------------------------
-- 1.7. Напишете заявка, която извежда производителите, които са 
--      произвели поне по 3 различни модела компютъра
SELECT maker
FROM product
WHERE type = 'PC'
GROUP BY maker
HAVING COUNT(*) >= 3
--------------------------------------------------------------------------------------
-- 1.8. Напишете заявка, която извежда производителите на компютрите с 
--      най-висока цена
SELECT DISTINCT maker
FROM product
   JOIN pc ON product.model = pc.model
WHERE price = (SELECT MAX(price) FROM pc)
--------------------------------------------------------------------------------------
-- 1.9. Напишете заявка, която извежда средната цена на компютрите 
--      за всяка честота по-голяма от 800
SELECT speed, AVG(price) AveragePrice
FROM pc
WHERE speed > 800
GROUP BY speed
--------------------------------------------------------------------------------------
-- 1.10. Напишете заявка, която извежда средния размер на диска на 
--       тези компютри произведени от производители, които произвеждат 
--       и принтери
SELECT AVG(hd) AverageHD
FROM product
    JOIN pc on product.model = pc.model
WHERE maker IN (SELECT maker
                FROM product
                WHERE type = 'Printer')
--------------------------------------------------------------------------------------
-- 1.11. Напишете заявка, която за всеки размер на лаптоп намира разликата 
--       в цената на най-скъпия и най-евтиния лаптоп със същия размер
SELECT screen, MAX(price) - MIN(price) AS PriceDiff
FROM laptop
GROUP BY screen
--------------------------------------------------------------------------------------
-- 2.1. Напишете заявка, която извежда броя на класовете кораби
SELECT COUNT(*) AS ClassCount
FROM CLASSES
--------------------------------------------------------------------------------------
-- 2.2. Напишете заявка, която извежда средния брой на оръжия за 
--      всички кораби, пуснати на вода
SELECT AVG(NUMGUNS) AgerageGuns
FROM CLASSES c
    JOIN SHIPS s ON c.CLASS = s.CLASS
--------------------------------------------------------------------------------------
-- 2.3. Напишете заявка, която извежда за всеки клас първата и 
--      последната година, в която кораб от съответния клас е пуснат на вода
SELECT CLASS, MIN(LAUNCHED) AS FirstYear, MAX(LAUNCHED) AS LastYear
FROM SHIPS
GROUP BY CLASS
--------------------------------------------------------------------------------------
-- 2.4. Напишете заявка, която извежда броя на корабите потънали в битка 
--      според класа
-- ако не се интересуваме от класовете без потънали кораби и класовете без кораби
SELECT s.CLASS, COUNT(*) AS SunkCount
FROM SHIPS s
   JOIN OUTCOMES o ON s.NAME = o.SHIP
WHERE o.RESULT = 'sunk'
GROUP BY s.CLASS
-- ако се интересуваме от класовете без потънали кораби и класовете без кораби
-- решение 1
SELECT s.CLASS, COUNT(o.SHIP) AS SunkCount
FROM CLASSES c
   LEFT OUTER JOIN SHIPS s ON c.CLASS = s.CLASS
   LEFT OUTER JOIN OUTCOMES o ON s.NAME = o.SHIP AND o.RESULT = 'sunk'
GROUP BY s.CLASS
-- решение 2
SELECT c.CLASS, SUM(CASE o.RESULT 
                        WHEN 'sunk' THEN 1
                        ELSE 0
                    END) AS SunkCount
FROM CLASSES c
   LEFT OUTER JOIN SHIPS s ON c.CLASS = s.CLASS
   LEFT OUTER JOIN OUTCOMES o ON s.NAME = o.SHIP
GROUP BY c.CLASS
--------------------------------------------------------------------------------------
-- 2.5. Напишете заявка, която извежда броя на корабите потънали в битка 
--      според класа, за тези класове с повече от 4 пуснати на вода кораба
-- решение 1
SELECT s.CLASS, COUNT(s.NAME) ShipCount
FROM SHIPS s
    JOIN OUTCOMES o ON s.NAME = o.SHIP
WHERE o.RESULT = 'sunk' AND s.CLASS IN (SELECT CLASS
                                        FROM SHIPS
                                        GROUP BY CLASS
                                        HAVING COUNT(*) > 4)
GROUP BY s.CLASS
-- решение 2
SELECT s.CLASS, SUM(CASE o.RESULT
                       WHEN 'sunk' THEN 1
                       ELSE 0
                    END) ShipCount
FROM SHIPS s
    LEFT OUTER JOIN OUTCOMES o ON s.NAME = o.SHIP
GROUP BY s.CLASS
HAVING COUNT(DISTINCT s.NAME) > 4
--------------------------------------------------------------------------------------
-- 2.6. Напишете заявка, която извежда средното тегло на корабите, за всяка страна. 
SELECT COUNTRY, AVG(DISPLACEMENT) AVG_WEIGHT
FROM CLASSES
GROUP BY COUNTRY
USE movies
-- 1. Да се добави информация за актрисата Nicole Kidman. За нея знаем само, че е родена на 20-и юни 1967.
INSERT INTO MOVIESTAR (NAME, GENDER, BIRTHDATE) VALUES ('Nicole Kidman', 'F', '1967-06-20')
--------------------------------------------------------------------------------------
-- 2. Да се изтрият всички продуценти с печалба (networth) под 10 милиона.
DELETE FROM MOVIEEXEC WHERE NETWORTH < 10000000
--------------------------------------------------------------------------------------
-- 3. Да се изтрие информацията за всички филмови звезди, за които не се знае адреса.
DELETE FROM MOVIESTAR WHERE ADDRESS IS NULL
--------------------------------------------------------------------------------------
-- 4. Използвайки две INSERT заявки, съхранете в базата данни факта, че персонален компютър модел 1100 
--    е направен от производителя C, има процесор 2400 MHz, RAM 2048 MB, твърд диск 500 GB, 52x DVD 
--    устройство и струва $299. Нека новият компютър има код 12. Забележка: моделът и CD са от тип низ.
INSERT INTO product VALUES ('C', '1100', 'PC')
INSERT INTO pc VALUES (12, '1100', 2400, 2048, 500, '52x', 299)
--------------------------------------------------------------------------------------
-- 5. Да се изтрие всичката налична информация за компютри модел 1100.
DELETE FROM pc WHERE model = '1100'
DELETE FROM product WHERE model = '1100'
--------------------------------------------------------------------------------------За всеки персонален компютър се продава и 15-инчов лаптоп със същите параметри, но с $500 по-скъп. 
-- Кодът на такъв лаптоп е със 100 по-голям от кода на съответния компютър. Добавете тази информация в базата.
INSERT INTO laptop (code, model, speed, ram, hd, price, screen)
    SELECT code + 100 as code, model, speed, ram, hd, price + 500 as price, 15 FROM pc
--------------------------------------------------------------------------------------
-- 7. Да се изтрият всички лаптопи, направени от производител, който не произвежда принтери.

DELETE FROM laptop
WHERE model IN (SELECT model 
		        FROM product 
                WHERE type='Laptop' AND maker NOT IN (SELECT maker
	                                                  FROM product
		                                              WHERE type='Printer'))
--------------------------------------------------------------------------------------
-- 8. Производител А купува производител B. На всички продукти на В променете производителя да бъде А.
UPDATE product
    SET maker = 'A'
WHERE maker = 'B'
-- 9. Да се намали два пъти цената на всеки компютър и да се добавят по 20 GB към всеки твърд диск. 
UPDATE pc
    SET price = price/2, hd=hd + 20
--------------------------------------------------------------------------------------
-- 10. За всеки лаптоп от производител B добавете по един инч към диагонала на екрана.

UPDATE laptop
SET screen = screen+1
WHERE model IN (SELECT model
		        FROM product
		        WHERE maker = 'B')
--------------------------------------------------------------------------------------
-- 11. Два британски бойни кораба от класа Nelson - Nelson и Rodney - са били пуснати на вода едновременно 
--     през 1927 г. Имали са девет 16-инчови оръдия (bore) и водоизместимост от 34 000 тона (displacement). 
--     Добавете тези факти към базата от данни.
INSERT INTO CLASSES VALUES ('Nelson', 'bb', 'Gt.Britain', 9, 16, 34000)
INSERT INTO SHIPS VALUES ('Nelson', 'Nelson', 1927)
INSERT INTO SHIPS VALUES ('Rodney', 'Nelson', 1927)
--------------------------------------------------------------------------------------
-- 12. Изтрийте от Ships всички кораби, които са потънали в битка.
DELETE FROM SHIPS
WHERE NAME IN (SELECT SHIP FROM OUTCOMES WHERE RESULT = 'sunk')
--------------------------------------------------------------------------------------
-- 13. Променете данните в релацията Classes така, че калибърът (bore) да се измерва в сантиметри (в момента 
--     е в инчове, 1 инч ~ 2.5 см) и водоизместимостта да се измерва в метрични тонове (1 м.т. = 1.1 т.)
UPDATE CLASSES
SET BORE = BORE * 2.54, DISPLACEMENT = DISPLACEMENT / 1.1
--------------------------------------------------------------------------------------
-- 14. Изтрийте всички класове, от които има по-малко от три кораба.
DELETE FROM CLASSES
WHERE CLASS NOT IN (SELECT CLASS
                    FROM SHIPS
                    GROUP BY CLASS
                    HAVING COUNT(*) >= 3)
--------------------------------------------------------------------------------------
-- 15. Променете калибъра на оръдията и водоизместимостта на класа Iowa, така че да са същите като тези на 
--     класа Bismarck.

UPDATE CLASSES
SET BORE = (SELECT BORE FROM CLASSES WHERE CLASS = 'Bismarck'),
    DISPLACEMENT = (SELECT DISPLACEMENT FROM CLASSES WHERE CLASS ='Bismarck')
WHERE CLASS = 'Iowa'

-- 1. Създайте нова база от данни с име test
CREATE DATABASE test
GO
USE test
--------------------------------------------------------------------------------------
-- 2. Дефинирайте следните релации:
-- а) Product(maker, model, type), където моделът е низ от точно 4 символа, maker - един символ, 
--    а type - низ до 7 символа
-- б) Printer(code, model, color, price), където code е цяло число, color е 'y' или 'n' и по 
--    подразбиране е 'n', price - цена с точност до два знака след десетичната запетая
-- в) Classes(class, type), където class е до 50 символа, а type може да бъде 'bb' или 'bc'

CREATE TABLE Product (
    maker char(1),
    model char(4),
    type varchar(7)
)

CREATE TABLE Printer (
    code int,
    model char(4),
    color char(1) DEFAULT 'n',
    price decimal(9,2)
)

CREATE TABLE Classes (
    class varchar(50),
    type char(2) CHECK (type IN ('bb', 'bc'))
)
--------------------------------------------------------------------------------------
-- 2. Добавете кортежи с примерни данни към новосъздадените релации. Добавете информация за принтер, 
--    за когото знаем само кода и модела.
INSERT INTO Product VALUES ('a', 'abcd', 'printer')
INSERT INTO Printer (code, model) VALUES (1, 'abcd')
INSERT INTO Classes VALUES ('Bismark', 'bb')
--------------------------------------------------------------------------------------
-- 3. Добавете към Classes атрибут bore - число с плаваща запетая.
ALTER TABLE Classes ADD bore float
--------------------------------------------------------------------------------------
-- 4. Напишете заявка, която премахва атрибута price от Printer.
ALTER TABLE Printer DROP COLUMN price
--------------------------------------------------------------------------------------
-- 5. Изтрийте всички таблици, които сте създали в това упражнение.
DROP TABLE Classes
DROP TABLE Printer
DROP TABLE Product
--------------------------------------------------------------------------------------
-- 6. Изтрийте базата test
USE master
GO
DROP DATABASE test
GO
-- 1. Дефинирайте изглед наречен BritishShips, който дава за всеки британски кораб неговият клас, 
--    тип, брой оръдия, калибър, водоизместимост и годината, в която е пуснат на вода
CREATE VIEW BritishShips 
AS
SELECT s.NAME, c.CLASS, c.TYPE, c.NUMGUNS, c.BORE, c.DISPLACEMENT, s.LAUNCHED
FROM CLASSES c 
    JOIN SHIPS s ON c.CLASS = s.CLASS
WHERE c.COUNTRY = 'Gt.Britain'
GO
--------------------------------------------------------------------------------------
-- 2. Напишете заявка, която използва изгледа от предната задача, за да покаже броят оръдия и 
--    водоизместимост на британските бойни кораби, пуснати на вода преди 1917
SELECT NAME, NUMGUNS, DISPLACEMENT
FROM BritishShips
WHERE LAUNCHED = 1917
--------------------------------------------------------------------------------------
-- 3. Напищете съответната SQL заявка, реализираща задача 2, но без да използвате изглед
SELECT s.NAME, c.NUMGUNS, c.DISPLACEMENT
FROM CLASSES c 
    JOIN SHIPS s ON c.CLASS = s.CLASS
WHERE c.COUNTRY = 'Gt.Britain' AND s.LAUNCHED = 1917
--------------------------------------------------------------------------------------
-- 4. Средната стойност на най-тежките класове кораби от всяка страна
--    Презюмираме, че може да има повече от един клас с максималната изместимост за страна
CREATE VIEW AverageMaxes
AS
SELECT AVG(m.MAXDISPLACEMENT) AS MAX_AVG
FROM CLASSES c
    JOIN (SELECT COUNTRY, MAX(DISPLACEMENT) AS MAXDISPLACEMENT
          FROM CLASSES
          GROUP BY COUNTRY) m ON c.COUNTRY = m.COUNTRY AND c.DISPLACEMENT = m.MAXDISPLACEMENT
GO
--------------------------------------------------------------------------------------
-- 5. Създайте изглед за всички потънали кораби по битки
CREATE VIEW SunkShips
AS
SELECT BATTLE, SHIP
FROM OUTCOMES
WHERE RESULT = 'sunk'
GO
--------------------------------------------------------------------------------------
-- 6. Въведете кораба California като потънал в битката при Guadalcanal чрез изгледа от задача 5.
--    За целта задайте подходяща стойност по премълчаване на колоната result от таблицата Outcomes
ALTER TABLE OUTCOMES ADD CONSTRAINT DC_OUTCOMES_RESULT DEFAULT 'sunk' FOR RESULT
GO

INSERT INTO SunkShips (battle, ship) VALUES ('Guadalcanal', 'California')
GO

ALTER TABLE OUTCOMES DROP CONSTRAINT DC_OUTCOMES_RESULT
GO
--------------------------------------------------------------------------------------
-- 7. Създайте изглед за всички класове с поне 9 оръдия. Използвайте WITH CHECK OPTION. 
CREATE VIEW NumGuns9
AS
SELECT *
FROM CLASSES
WHERE NUMGUNS >= 9
WITH CHECK OPTION
GO
--------------------------------------------------------------------------------------
-- Опитайте се 
-- да промените през изгледа броя оръдия на класа Iowa последователно на 15 и 5.
UPDATE NumGuns9 
    SET NUMGUNS = 15
WHERE CLASS = 'Iowa'
-- Следващото не работи, заради WITH CHECK OPTION. Подобна модификация би извадила обновявания запис
-- извън обсега на изгледа.
UPDATE NumGuns9 
    SET NUMGUNS = 5
WHERE CLASS = 'Iowa'
GO
--------------------------------------------------------------------------------------
-- 8. Променете изгледа от задача 7, така че броят оръдия да може да се променя без ограничения.
ALTER VIEW NumGuns9
AS
SELECT *
FROM CLASSES
WHERE NUMGUNS >= 9
GO
--------------------------------------------------------------------------------------
-- 9. Създайте изглед с имената на битките, в които са участвали поне 3 кораба с под 9 оръдия и 
--    от тях поне един е бил увреден.
CREATE VIEW BattleList
AS
SELECT BATTLE 
FROM OUTCOMES o 
    JOIN SHIPS s ON o.SHIP = s.NAME
    JOIN CLASSES c ON s.CLASS = c.CLASS
WHERE c.NUMGUNS < 9 
GROUP BY BATTLE
HAVING COUNT(*) >= 3 AND SUM(CASE RESULT WHEN 'damaged' THEN 1 ELSE 0 END) >= 1
GO
-- 1.1. Добавете Брус Уилис в базата. Направете така, че при добавяне на филм, 
-- чието заглавие съдържа “save” или “world”, Брус save” или “save” или 
-- “world”, Брус world”, Брус Уилис автоматично да бъде добавен като актьор, 
-- играл във филма.
INSERT INTO MOVIESTAR VALUES ('Bruce Willis', 'Hollywood', 'M', '1955-03-19');
GO

CREATE TRIGGER BruceWillis ON MOVIE AFTER INSERT
AS
    INSERT INTO STARSIN (movietitle, movieyear, STARNAME)
		SELECT TITLE, YEAR, 'Bruce Willis'
		FROM inserted
		WHERE title LIKE '%save%' AND TITLE LIKE '%world%'
GO
--------------------------------------------------------------------------------------
-- 1.2. Да се направи така, че да не е възможно средната стойност на Networth 
-- да е помалка от 500 000 (ако при промени в таблицата MovieExec тази стойност 
-- стане по-малка от 500 000, промените да бъдат отхвърлени).
CREATE TRIGGER NetworthEnforce ON MOVIEEXEC AFTER INSERT, UPDATE, DELETE
AS
    IF (SELECT AVG(NETWORTH) FROM MOVIEEXEC) < 500000
    BEGIN
		RAISERROR('Data Integrity Violation', 11, 1)
        ROLLBACK
    END
GO
--------------------------------------------------------------------------------------
-- 1.3. MS SQL не поддържа ON DELETE SET NULL. Да се реализира с тригер за външния 
-- ключ Movie.producerc#.
CREATE TRIGGER SetNullOnDelete ON MOVIEEXEC INSTEAD OF DELETE
AS
	UPDATE MOVIE 
		SET PRODUCERC# = NULL 
	WHERE PRODUCERC# IN (SELECT CERT# FROM deleted)

    DELETE FROM MOVIEEXEC 
	WHERE CERT# IN (SELECT CERT# FROM deleted)
GO
--------------------------------------------------------------------------------------
-- 1.4. При добавяне на нов запис в StarsIn, ако новият ред указва несъществуващ 
-- филм или актьор, да се добавят липсващите данни в съответната таблица 
-- (неизвестните данни да бъдат NULL). Внимание: има външни ключове!
CREATE TRIGGER AutoStarAndMovie ON STARSIN INSTEAD OF INSERT
AS
    INSERT INTO MOVIESTAR (NAME)
		SELECT DISTINCT STARNAME 
		FROM inserted
		WHERE STARNAME NOT IN (SELECT NAME FROM MOVIESTAR)
     
    INSERT INTO MOVIE (TITLE, YEAR)
		SELECT DISTINCT MOVIETITLE, MOVIEYEAR
		FROM inserted
		WHERE NOT EXISTS (SELECT TITLE FROM MOVIE WHERE TITLE = MOVIETITLE AND YEAR = MOVIEYEAR)

	INSERT INTO STARSIN
		SELECT * FROM inserted
GO
--------------------------------------------------------------------------------------
-- 2.1. Да се направи така, че при изтриване на лаптоп на производител D автоматично 
-- да се добавя PC със същите параметри в таблицата с компютри. Моделът на новите 
-- компютри да бъде ‘1121’, CD устройството да бъде ‘52x’, а кодът - със 100 
-- по-голям от кода на лаптопа.
CREATE TRIGGER DeleteD ON laptop AFTER DELETE
AS
    INSERT INTO pc
		SELECT code + 100, '1121', speed, ram, hd, '52x', price
		FROM deleted d 
			JOIN product p ON d.model = p.model
		WHERE p.maker = 'D'
GO
--------------------------------------------------------------------------------------
-- 2.2. При промяна на цената на някой компютър се уверете, че няма по-евтин компютър 
-- със същата честота на процесора.
CREATE TRIGGER PcPriceEnforce1 ON pc AFTER UPDATE
AS
IF EXISTS (SELECT *
            FROM pc p
                JOIN inserted i ON p.speed = i.speed
            WHERE p.price < i.price)
BEGIN
    RAISERROR('Data Integrity Violation', 11, 1)
    ROLLBACK
END
GO
--------------------------------------------------------------------------------------
-- 2.3. Никой производител на компютри не може да произвежда и принтери.
CREATE TRIGGER MakerEnforcement1 ON product AFTER INSERT, UPDATE
AS
IF EXISTS (SELECT  *
           FROM product p1
               JOIN product p2 on p1.maker = p2.maker
           WHERE p1.type = 'PC' and p2.type = 'Printer')
    BEGIN
        RAISERROR('Data Integrity Violation', 11, 1)
        ROLLBACK
    END
GO
--------------------------------------------------------------------------------------
-- 2.4. Всеки производител на компютър трябва да произвежда и лаптоп, който да има същата 
-- или по-висока честота на процесора.

-- Причината това решение да е толкова дълго, е че искаме да гарантираме интегритета на базата 
-- относно условието на  задачата, независимо от това какви заявки се изпълняват от клиентите
-- За решението ще създадеме три тригера:
--   - pc (INSERT, UPDATE)
--   - laptop (UPDATE, DELETE)
--   - product (UPDATE)

-- Тригер за pc
-- Вариант 1

CREATE TRIGGER PcTrigger1 ON pc AFTER INSERT, UPDATE 
AS
    DECLARE @numFound int = 
        (SELECT COUNT(DISTINCT i.code)
         FROM inserted i
            JOIN product p1 ON i.model = p1.model
            JOIN product p2 ON p1.maker = p2.maker
            JOIN laptop l ON p2.model = l.model
         WHERE i.speed <= l.speed)

    IF @numFound != (SELECT COUNT(*) FROM inserted)
    BEGIN
        RAISERROR('Data Integrity Violation', 11, 1)
        ROLLBACK
    END
GO

-- Тригер за pc
-- Вариант 2

CREATE TRIGGER PcTrigger2 ON pc AFTER INSERT, UPDATE 
AS
    IF EXISTS (SELECT * 
               FROM inserted i
                   JOIN product p1 ON i.model = p1.model
               WHERE NOT EXISTS (SELECT * 
                                 FROM laptop l2
                                 WHERE l2.speed >= i.speed 
                                    AND l2.model IN (SELECT model 
                                                     FROM product p2 
                                                     WHERE p2.maker = p1.maker)))
    BEGIN
        RAISERROR('Data Integrity Violation', 11, 1)
        ROLLBACK       
    END
GO

-- 2. Тригер за laptop

-- Ако на някой лаптоп му е сменен модела, то той ще се озове при друг производител,
-- но там проблем не може да възникне.
-- Проблем може да има само със стария производител, за който имаме премахнат лаптоп
-- Затова е достатъчно да направим проверка за всички производители на лаптопи в
-- таблицата deleted дали след модификацията всичко е наред.
-- Тоест дали за всички PC-та от такива производители има лаптоп с по-голяма 
-- честота от същия производител
-- Тоест дали има PC от такъв производител, за което няма лаптоп с по-голяма честота

-- Вариант 1

CREATE TRIGGER LaptopTrigger1 ON laptop AFTER UPDATE, DELETE
AS

    IF EXISTS (SELECT * 
               FROM pc p
                   JOIN product p1 ON p.model = p1.model
                   JOIN product p2 ON p1.maker = p2.maker
                   JOIN deleted d ON d.model = p2.model
               WHERE NOT EXISTS (SELECT *
                                 FROM laptop l2
                                 WHERE l2.speed >= p.speed
                                    AND l2.model IN (SELECT model
                                                     FROM product p3
                                                     WHERE p3.maker = p1.maker)))
    BEGIN
        RAISERROR('Data Integrity Violation', 11, 1)
        ROLLBACK
    END
GO

-- Вариант 2

-- Тук смятаме максималната цена на лаптоп за всеки афектиран производител и гледаме
-- дали има PC, чиято цена е по-малка от най-високата на лаптоп от същия производител.

CREATE TRIGGER LaptopTrigger2 ON laptop AFTER UPDATE, DELETE
AS
    IF EXISTS ( SELECT *
                FROM deleted d
                    JOIN product p1 ON d.model = p1.model
                    -- външното свързване и проверката за IS NULL по-долу
                    -- е за случая, когато изтриваме всички лаптопи от
                    -- даден модел
                    LEFT OUTER JOIN (SELECT px.maker, MAX(speed) AS maxspeed 
                                        FROM product px 
                                        JOIN laptop lx ON px.model = lx.model 
                                        GROUP BY px.maker) t ON t.maker = p1.maker
                    JOIN product p2 ON p1.maker = p2.maker
                    JOIN pc ON pc.model = p2.model
                    WHERE pc.speed > t.maxspeed OR t.maxspeed IS NULL)
    BEGIN
        RAISERROR('Data Integrity Violation', 11, 1)
        ROLLBACK
    END
GO

-- 3. Тригер за product

-- Той е само за UPDATE. Няма как да се изтрие модел, без преди
-- това да се изтрият съответните записи от laptop/pc/printeр
-- (заради FOREIGN KEY ограниченията).
-- Ако тяхното изтриване е минало - значи няма какво да се
-- проверява

CREATE TRIGGER ProductTrigger ON product AFTER UPDATE
AS
    -- Правиме проверка за всички производители от deleted

    IF EXISTS ( SELECT *
                FROM deleted d
                    LEFT OUTER JOIN (SELECT px.maker, MAX(speed) AS maxspeed
                                     FROM product px
                                     JOIN laptop lx ON px.model = lx.model
                                     GROUP BY px.maker) t ON t.maker = d.maker
                    JOIN product p2 ON d.maker = p2.maker
                    JOIN pc ON pc.model = p2.model
                WHERE pc.speed > t.maxspeed OR t.maxspeed IS NULL)
    BEGIN
        RAISERROR('Data Integrity Violation', 11, 1)
        ROLLBACK
    END
GO
--------------------------------------------------------------------------------------
-- 2.5. При промяна на данните в таблицата Laptop се уверете, че средната цена на лаптопите 
-- за всеки производител е поне 2000.

USE pc
GO

CREATE TRIGGER LaptopTriggerAvgPrice ON laptop AFTER INSERT, UPDATE, DELETE
AS
    IF EXISTS(SELECT p.maker
              FROM laptop l
                  JOIN product p ON l.model = p.model
                  JOIN product p2 ON p2.maker = p.maker
              WHERE p2.model IN (SELECT model FROM inserted UNION SELECT model FROM deleted)
              GROUP BY p.maker
              HAVING AVG(l.price) < 2000)
    BEGIN
        RAISERROR('Data Integrity Violation', 11, 1)
        ROLLBACK
    END
GO
--------------------------------------------------------------------------------------
-- 2.6. Ако някой лаптоп има повече памет от някой компютър, трябва да бъде и по-скъп от него.
CREATE TRIGGER EnforePriceForPC ON pc AFTER INSERT, UPDATE
AS
	IF EXISTS (
		SELECT *
		FROM inserted
			JOIN laptop ON inserted.ram < laptop.ram
		WHERE inserted.price >= laptop.price)
    BEGIN
        RAISERROR('Data Integrity Violation', 11, 1)
        ROLLBACK
    END
GO

CREATE TRIGGER EnforePriceForLaptop ON laptop AFTER INSERT, UPDATE
AS
	IF EXISTS (
		SELECT *
		FROM pc
			JOIN inserted ON pc.ram < inserted.ram
		WHERE pc.price >= inserted.price)
    BEGIN
        RAISERROR('Data Integrity Violation', 11, 1)
        ROLLBACK
    END
GO
--------------------------------------------------------------------------------------
-- 2.7. Да приемем, че цветните матрични принтери (type = 'Matrix') са забранени за продажба. 
-- При добавяне на принтери да се игнорират цветните матрични. Ако с една заявка се добавят 
-- няколко принтера, да се добавят само тези, които не са забранени, а другите да се игнорират.
CREATE TRIGGER MatrixFilter ON printer INSTEAD OF INSERT
AS
INSERT INTO printer
	SELECT * 
	FROM inserted
	WHERE type != 'Matrix' OR color != 'y'
GO
--------------------------------------------------------------------------------------
-- 3.1. Ако бъде добавен нов клас с водоизместимост по-голяма от 35000, класът да бъде добавен 
-- в таблицата, но да му се зададе водоизместимост 35000.

USE ships
GO
-- Вариант 1
CREATE TRIGGER DisplacementCap1 ON CLASSES AFTER INSERT
AS
    INSERT INTO CLASSES 
        SELECT CLASS, 
               TYPE, 
               COUNTRY, 
               NUMGUNS, 
               BORE, 
               CASE
                  WHEN DISPLACEMENT > 35000 THEN 35000
                  ELSE DISPLACEMENT
               END AS DISPLACEMENT
        FROM inserted
GO
-- Вариант 2
CREATE TRIGGER DisplacementCap2 ON CLASSES AFTER INSERT
AS
    INSERT INTO CLASSES 
        SELECT CLASS, TYPE, COUNTRY, NUMGUNS, BORE, DISPLACEMENT 
        FROM inserted
        WHERE DISPLACEMENT <= 35000

    INSERT INTO CLASSES 
        SELECT CLASS, TYPE, COUNTRY, NUMGUNS, BORE, 35000 AS DISPLACEMENT 
        FROM inserted
        WHERE DISPLACEMENT > 35000
GO
--------------------------------------------------------------------------------------
-- 3.2. Създайте изглед, който показва за всеки клас името му и броя кораби (евентуално 0). 
-- Направете така, че при изтриване на ред да се изтрие класът и всички негови кораби.
CREATE VIEW ShipsCount
AS
    SELECT c.CLASS, COUNT(s.NAME) AS SHIPS
    FROM CLASSES c
		LEFT OUTER JOIN SHIPS s ON c.CLASS = s.CLASS
    GROUP BY c.CLASS
GO

CREATE TRIGGER ShipsCountProcess ON ShipsCount INSTEAD OF DELETE 
AS
    DELETE FROM SHIPS 
    WHERE CLASS IN (SELECT CLASS FROM deleted)

    DELETE FROM CLASSES 
    WHERE CLASS IN (SELECT CLASS FROM deleted)
GO
--------------------------------------------------------------------------------------
-- 3.3. Никой клас не може да има повече от два кораба.
CREATE TRIGGER ShipCountEnforcer ON SHIPS AFTER INSERT, UPDATE
AS
    IF EXISTS (SELECT CLASS
               FROM SHIPS
               GROUP BY CLASS
               HAVING COUNT(*) > 2)
    BEGIN
        RAISERROR('Data Integrity Violation', 11, 1)
        ROLLBACK
    END
GO
--------------------------------------------------------------------------------------
-- 3.4. Кораб с повече от 9 оръдия не може да участва в битка с кораб, който е с по-малко от 9 
-- оръдия. Напишете тригер за Outcomes.

-- ако приемеме, че след създаване на клас в таблицата CLASSES може да се модифицират
-- атрибутите на класовете (в нашия случай - броя оръдия), то тогава би следвало и за там
-- да се направи тригер за UPDATE
-- в случая приемаме, че за класове имаме само INSERT/DELETE операции (в практическия 
-- случай - при нови параметри - имаме нов клас кораби)

-- за битките, за които имаме добавени/обновени кораби, проверяваме дали има двойки, 
-- нарушаващи условието

CREATE TRIGGER OutcomesEnforce ON OUTCOMES AFTER INSERT, UPDATE
AS
	IF EXISTS(
		SELECT o1.BATTLE
		FROM OUTCOMES o1
			JOIN OUTCOMES o2 ON o1.BATTLE = o2.BATTLE AND o1.SHIP != o2.SHIP
			JOIN SHIPS s1 ON s1.NAME = o1.SHIP
			JOIN SHIPS s2 ON s2.NAME = o2.SHIP
			JOIN CLASSES c1 ON c1.CLASS = s1.CLASS
			JOIN CLASSES c2 ON c2.CLASS = s2.CLASS
		WHERE c1.NUMGUNS > 9 AND c2.NUMGUNS < 9 AND o1.BATTLE IN (SELECT BATTLE FROM inserted))
	BEGIN
		RAISERROR('Data Integrity Violation', 11, 1)
		ROLLBACK
	END
GO
--------------------------------------------------------------------------------------
-- 3.5. Кораб, който вече е потънал, не може да участва в битка, чиято дата е след датата
-- на потъването му. Напишете тригери за всички таблици, за които е необходимо.

-- ако приемеме, че след създаване на битка в таблицата BATTLES може да се модифицира
-- датата на битката - тогава трябва и за там да се направи тригер за UPDATE.
-- в случая приемаме, че за BATTLES имаме само INSERT/DELETE операции

CREATE TRIGGER OutcomeDateEnforce ON OUTCOMES AFTER INSERT, UPDATE
AS
IF EXISTS (
	SELECT *
	FROM OUTCOMES o1
		JOIN BATTLES b1 ON o1.BATTLE = b1.NAME
		JOIN OUTCOMES o2 ON o1.SHIP = o2.SHIP
		JOIN BATTLES b2 ON o2.BATTLE = b2.NAME
	WHERE o1.RESULT = 'sunk' AND b1.DATE < b2.DATE AND o1.SHIP IN (SELECT SHIP FROM inserted))
	BEGIN
		RAISERROR('Data Integrity Violation', 11, 1)
		ROLLBACK
	END
GO
