-- part1
DROP FUNCTION IF EXISTS tranferred_points();
-- Создание функции tranferred_points(),
-- которая возвращает более читаемую таблицу TransferredPoints:
CREATE OR REPLACE FUNCTION tranferred_points()
RETURNS TABLE("Peer1" VARCHAR, "Peer2" VARCHAR, "PointsAmount" BIGINT) AS $$
BEGIN
	-- Запрос на возврат данных
	RETURN QUERY
	WITH reversed AS (
		-- Создание временной таблицы с перевернутыми данными для удобства чтения
		SELECT
		-- Выборка позволяет поменять местами столбцы и преобразовать
		-- отрицательные значения для понимания передачи точек
		CASE WHEN checking_peer > checked_peer THEN checking_peer ELSE checked_peer END AS checking_peer,
		CASE WHEN checking_peer > checked_peer THEN checked_peer ELSE checking_peer END AS checked_peer,
		CASE WHEN checking_peer > checked_peer THEN points_amount ELSE -points_amount END AS PointsAmount
		FROM TransferredPoints
	)
	-- Выборка данных из временной таблицы, сгруппированных по
	-- 	парам пиров и суммированием переданных точек
	SELECT checking_peer AS Peer1, checked_peer AS Peer2, SUM(PointsAmount) FROM reversed
	GROUP BY checking_peer, checked_peer;
END;
$$ LANGUAGE plpgsql;

-- Проверка
SELECT * FROM tranferred_points();



-- part2
-- DROP FUNCTION IF EXISTS completed_tasks();
-- Создание функции completed_tasks(), которая возвращает
-- таблицу с данными о выполненных задачах:
CREATE OR REPLACE FUNCTION completed_tasks()
RETURNS TABLE("Peer" VARCHAR, "Task" TEXT, "XP" BIGINT) AS $$
BEGIN
	RETURN QUERY
	SELECT checks.peer AS Peer, SPLIT_PART(checks.task, '_', 1) AS Task, CAST(xp.xp_amount AS BIGINT) AS XP
	FROM xp
	JOIN checks ON checks.id = xp.check_id;
END;
$$ LANGUAGE plpgsql;

-- Проверка
SELECT * FROM completed_tasks();



-- part3
-- DROP FUNCTION IF EXISTS all_day_at_school(day DATE);
-- Создание функции all_day_at_school(), которая возвращает таблицу с
-- данными о пирах, не покидающих кампус весь день:
CREATE OR REPLACE FUNCTION all_day_at_school(day DATE)
RETURNS TABLE ("Peer" VARCHAR)
AS $$
BEGIN
    RETURN QUERY
	SELECT p.nickname
	FROM peers AS p
	WHERE (SELECT COUNT(*)
		   FROM timetracking AS tt
		   WHERE tt.peer = p.nickname
		   AND tt.date = day
		   AND tt.state = 2) = 1; -- т. е. пир один раз зашел и один раз вышел
END;
$$ LANGUAGE plpgsql;

-- Проверка
SELECT * FROM all_day_at_school('2022-12-01');
-- SELECT * FROM timetracking;



-- part4
-- DROP FUNCTION IF EXISTS points_change();
-- Создание функции points_change(), которая возвращает таблицу с данными об изменениях
-- в количестве пир поинтов для каждого пира в таблице transferredpoints:
CREATE OR REPLACE FUNCTION points_change()
RETURNS TABLE ("Peer" VARCHAR, "PointsChange" NUMERIC)
AS $$
BEGIN
	-- Возврат запроса на выборку данных
    RETURN QUERY
	WITH transfered AS (
	-- Суммирование полученных и переданных пир поинтов
		SELECT checking_peer AS peer_name,
		SUM(points_amount) AS points_change
		FROM transferredpoints
		GROUP BY checking_peer

		UNION ALL

		SELECT checked_peer AS peer_name,
		-SUM(points_amount) AS points_change
		FROM transferredpoints
		GROUP BY checked_peer
	)
	-- Выбор ника пира и суммарного изменения пир поинтов
    SELECT peer_name,
	SUM(t.points_change)
	FROM transfered AS t
	GROUP BY peer_name
	ORDER BY SUM(t.points_change) DESC;
END;
$$ LANGUAGE plpgsql;

-- Проверка
SELECT * FROM points_change();
-- SELECT * FROM transferredpoints;



-- part5
-- DROP FUNCTION IF EXISTS points_change_short();
-- Создание функции points_change_short(), которая возвращает таблицу с данными об изменениях
-- в количестве пир поинтов каждого пира по таблице, возвращаемой функцией tranferred_points():
CREATE OR REPLACE FUNCTION points_change_short()
RETURNS TABLE("Peer" VARCHAR, "PointsChange" NUMERIC) AS $$
BEGIN
	-- Возврат запроса на выборку данных
	RETURN QUERY
	WITH sums AS (
		SELECT "Peer1" AS Peer, SUM("PointsAmount") AS PointsChange
		FROM tranferred_points()
		GROUP BY "Peer1"
		
		UNION
		
		SELECT "Peer2" AS Peer, -SUM("PointsAmount") AS PointsChange
		FROM tranferred_points()
  		GROUP BY "Peer2"
	)
	SELECT sums.Peer, SUM(sums.PointsChange)
	FROM sums
	GROUP BY sums.Peer;
END;
$$ LANGUAGE plpgsql;

-- Проверка
SELECT * FROM points_change_short();



-- part6
-- DROP FUNCTION IF EXISTS popular_task();
-- Создание функции popular_task(), которая возвращает
-- таблицу с самым часто проверяемым заданием за каждый день:
CREATE OR REPLACE FUNCTION popular_task()
RETURNS TABLE("Day" DATE, "Task" TEXT) AS $$
BEGIN
	-- Возврат запроса на выборку данных
	RETURN QUERY
	-- 	Счетчик проверок одинаковых заданий за день
	WITH counter_of_checks AS (
    	SELECT "date",
		checks.task AS popular_task,
        COUNT(*) AS check_count
		FROM checks
		GROUP BY "date", checks.task
	),
	-- 	Назначим ранги по популярности заданий
	rank_of_checks AS (
		SELECT "date", popular_task, check_count,
		DENSE_RANK() OVER (PARTITION BY "date"
						   ORDER BY check_count DESC) AS rank
		FROM counter_of_checks
	)
	
	SELECT "date", SPLIT_PART(popular_task, '_', 1)
	FROM rank_of_checks
	WHERE rank = 1
	ORDER BY "date";
	
END;
$$ LANGUAGE plpgsql;

-- Проверка
SELECT * FROM popular_task();



-- part7
-- DROP FUNCTION IF EXISTS who_finished_the_block(name_block VARCHAR);
-- Создание функции who_finished_the_block(), которая возвращает
-- таблицу с пирами, которые выполнили весь заданный блок задач:
CREATE OR REPLACE FUNCTION who_finished_the_block(name_block VARCHAR)
RETURNS TABLE("Peer" VARCHAR, "Day" DATE) AS $$
BEGIN
    RETURN QUERY
	-- 	Сколько проектов сдал каждый пир из данного блока
    WITH how_much_peer_finish AS (
        SELECT peer, COUNT(*) AS count_of_block
        FROM checks
        WHERE task LIKE name_block || '%'
        GROUP BY peer
    ),
	-- 	Дата последнего защищенного проекта из данного блока для пиров
		late_date AS (
			SELECT peer, task, date
			FROM checks
			WHERE task LIKE 'CPP' || '%'
		)
    SELECT checks.peer, MAX(late_date.date)
    FROM how_much_peer_finish
    JOIN checks ON how_much_peer_finish.peer = checks.peer
	JOIN late_date ON how_much_peer_finish.peer = late_date.peer
    WHERE count_of_block = (
        SELECT COUNT(*)
        FROM tasks
        WHERE title LIKE name_block || '%'
    )
    GROUP BY checks.peer;
END;
$$ LANGUAGE plpgsql;

-- Проверка
SELECT * FROM who_finished_the_block('CPP');
-- SELECT * FROM checks;



-- part8
-- DROP FUNCTION IF EXISTS peer_from_friends();
-- Создание функции peer_from_friends(), которая возвращает
-- таблицу с пирами, которых друзья рекомендовали как проверяющих больше всего:
CREATE OR REPLACE FUNCTION peer_from_friends()
RETURNS TABLE("Peer" VARCHAR, "RecommendedPeer" VARCHAR) AS $$
BEGIN
	RETURN QUERY
	-- 	Сумма таблицы друзей с перевертышами
	WITH friends_sum AS (
		SELECT peer1 AS peer, peer2 AS friend
		FROM friends

		UNION ALL

		SELECT peer2 AS peer, peer1 AS friend
		FROM friends
	),
		-- Рекомендации друзей
		recommendations_from_friends AS (
			SELECT p.nickname, r.recommended_peer
			FROM peers p
			JOIN friends_sum fs ON fs.peer = p.nickname
			JOIN recommendations r ON r.peer = fs.friend AND r.recommended_peer <> p.nickname
		),
		-- Сколько раз кого порекомендовали
		 how_many_times_recommended AS (
			SELECT rf.nickname, rf.recommended_peer, COUNT(*) AS recommendation_count
			FROM recommendations_from_friends rf
			GROUP BY rf.nickname, rf.recommended_peer
		 ),
		 -- Ранк к кол-ву рекомендаций
		 how_many_with_rank AS (
			SELECT *,
			DENSE_RANK() OVER (PARTITION BY nickname ORDER BY recommendation_count DESC) AS rank
			FROM how_many_times_recommended
		 )

		 SELECT nickname, recommended_peer
		 FROM how_many_with_rank
		 WHERE rank = 1;
END;
$$ LANGUAGE plpgsql;

-- Проверка
SELECT * FROM peer_from_friends();
-- SELECT * FROM friends;
-- SELECT * FROM recommendations;



-- part9
-- DROP FUNCTION IF EXISTS how_many_percent_started(block1_name VARCHAR, block2_name VARCHAR);
-- Создание функции how_many_percent_started(), которая возвращает
-- таблицу c данными о проценте пиров завершивших блок 1, блок 2, оба, ни одного из двух:
CREATE OR REPLACE FUNCTION how_many_percent_started(block1_name VARCHAR, block2_name VARCHAR)
RETURNS TABLE("StartedBlock1" NUMERIC, "StartedBlock2" NUMERIC, "StartedBothBlocks" NUMERIC, "DidntStartAnyBlock" NUMERIC) AS $$
DECLARE
    total_peers INTEGER;
    started_block1 INTEGER;
    started_block2 INTEGER;
    started_both INTEGER;
BEGIN
    -- Общее количество пиров
    SELECT COUNT(DISTINCT nickname) INTO total_peers FROM peers;
	
    --Кол-во пиров, приступивших к блоку 1
    SELECT COUNT(DISTINCT peer) INTO started_block1
    FROM checks
    WHERE task LIKE block1_name || '%';

    -- Пир приступил к блоку 2
    SELECT COUNT(DISTINCT peer) INTO started_block2
    FROM checks
    WHERE task LIKE block2_name || '%';

    -- Пир приступил к обоим блокам
    SELECT COUNT(DISTINCT peer) INTO started_both
    FROM checks
    WHERE task LIKE block1_name || '%' AND peer IN (
        SELECT DISTINCT peer FROM checks WHERE task LIKE block2_name || '%'
    );

    -- Пир не приступил ни к одному блоку
    SELECT COUNT(DISTINCT nickname) INTO "DidntStartAnyBlock"
    FROM peers
    WHERE nickname NOT IN (
        SELECT DISTINCT peer FROM checks WHERE task LIKE block1_name || '%'
        UNION
        SELECT DISTINCT peer FROM checks WHERE task LIKE block2_name || '%'
    );

    -- Возвращаем проценты
	RETURN QUERY
		SELECT
		ROUND(started_block1 * 100.0 / total_peers) AS "StartedBlock1",
        ROUND(started_block2 * 100.0 / total_peers) AS "StartedBlock2",
        ROUND(started_both * 100.0 / total_peers) AS "StartedBothBlocks",
        ROUND("DidntStartAnyBlock" * 100.0 / total_peers) AS "DidntStartAnyBlock";
END;
$$ LANGUAGE plpgsql;

-- Проверка
SELECT * FROM how_many_percent_started('CPP', 'A');
-- SELECT * FROM checks;
-- SELECT COUNT(DISTINCT nickname) FROM peers; -- общее кол-во пиров



-- part10
-- DROP FUNCTION IF EXISTS birthday_checks();
-- Создание функции birthday_checks(), которая возвращает
-- таблицу c данными о проценте пиров, успешно прошедших проверку в день рождения,
-- а также проценте приров, проваливших проверку в день рождения:
CREATE OR REPLACE FUNCTION birthday_checks()
RETURNS TABLE ("SuccessfulChecks" BIGINT, "UnsuccessfulChecks" BIGINT)
AS $$
BEGIN
RETURN QUERY
	-- Определение информации о дне рождения каждого пира
	WITH birthday_info AS (
		SELECT nickname, extract('DAY' FROM birthday) AS day, extract('MONTH' FROM birthday) AS month
		FROM peers
    ),
	-- Проверки, которые успешно прошли в день рождения
    successful_checks AS (
		SELECT peer, extract('DAY' FROM date) AS day, extract('MONTH' FROM date) AS month
        FROM checks
        WHERE exists(SELECT * FROM p2p WHERE p2p.check_id = checks.id AND state = 'success')
        AND (exists(SELECT * FROM verter WHERE verter.check_id  = checks.id AND state = 'success') OR
        NOT exists(SELECT * FROM verter WHERE verter.check_id = checks.id))
	),
	-- Проверки, которые провалились в день рождения
	failure_checks AS (
		SELECT peer, extract('DAY' FROM date) AS day, extract('MONTH' FROM date) AS month
		FROM checks
        WHERE exists(SELECT * FROM p2p WHERE p2p.check_id = checks.id AND state = 'failure')
        OR exists(SELECT * FROM verter WHERE verter.check_id = checks.id AND state = 'failure')
	),
	-- Все проверки в день рождения
    all_checks AS (
		SELECT peer, extract('DAY' FROM date) AS day, extract('MONTH' FROM date) AS month
    	FROM checks
    ),
	-- Успешные проверки в день рождения с информацией о дне рождения
    successful_checks_in_birthday AS (
		SELECT *
        FROM successful_checks AS sc
        LEFT JOIN birthday_info AS bi ON sc.peer = bi.nickname
        WHERE bi.month = sc.month
        AND bi.day = sc.day
	),
	-- Проваленные проверки в день рождения с информацией о дне рождения
    failure_checks_in_birthday AS (
    	SELECT * FROM failure_checks AS fc
        LEFT JOIN birthday_info AS bi ON fc.peer = bi.nickname
        WHERE bi.month = fc.month AND bi.day = fc.day
	),
	-- Все проверки в день рождения с информацией о дне рождения
    all_checks_in_birthday AS (
    	SELECT * FROM all_checks AS ac
        LEFT JOIN birthday_info AS bi ON ac.peer = bi.nickname
        WHERE ac.month = bi.month AND ac.day = bi.day
    ),
	-- Количество успешных проверок в день рождения
    successful_checks_in_birthday_count AS (
		SELECT COUNT(*) FROM successful_checks_in_birthday
    ),
	-- Количество проваленных проверок в день рождения
    failure_checks_in_birthday_count AS (
		SELECT COUNT(*) FROM failure_checks_in_birthday
    ),
	-- Общее количество проверок в день рождения
    all_checks_in_birthday_count AS (
		SELECT COUNT(*) FROM all_checks_in_birthday
    )
	-- Расчет процентов успешных и проваленных проверок
	SELECT (select * from successful_checks_in_birthday_count) * 100 /
		   (select * from all_checks_in_birthday_count) as SuccessfulChecks,
		   (select * from failure_checks_in_birthday_count) * 100 /
		   (select * from all_checks_in_birthday_count) as UnsuccessfulChecks;
END;
$$ LANGUAGE plpgsql;

-- Проверка
SELECT * FROM birthday_checks();



-- part11
-- DROP FUNCTION IF EXISTS passed_1_2_without_3();
-- Создание функции passed_1_2_without_3(), которая возвращает
-- таблицу со всеми пирами, которые сдали заданные задания 1 и 2, но не сдали задание 3:
CREATE OR REPLACE FUNCTION passed_1_2_without_3()
RETURNS TABLE ("Peers" VARCHAR)
AS $$
BEGIN
RETURN QUERY
	WITH success_tasks AS (
		SELECT peer, task
		FROM checks
		JOIN p2p ON checks.id = p2p.check_id
		LEFT JOIN verter ON checks.id = verter.check_id
		WHERE p2p.state = 'success'
			AND (NOT exists(SELECT * FROM verter WHERE verter.check_id = checks.id) OR
				 verter.state = 'success')
	)
	
	SELECT DISTINCT peer AS nickname
	FROM success_tasks
	WHERE peer in (SELECT peer FROM success_tasks WHERE task LIKE '%' || '1_' || '%')
		AND peer in (SELECT peer FROM success_tasks WHERE task LIKE '%' || '2_' || '%')
		AND peer NOT IN (SELECT peer FROM success_tasks WHERE task LIKE '%' || '3_' || '%');

END;
$$ LANGUAGE plpgsql;

-- Проверка
SELECT * FROM passed_1_2_without_3();
-- SELECT * FROM checks;



-- part12
-- DROP FUNCTION IF EXISTS prev_count_for_tasks();
-- Создание функции prev_count_for_tasks(), которая возвращает
-- таблицу данными о кол-ве предшествующих заданию задач:
CREATE OR REPLACE FUNCTION prev_count_for_tasks()
RETURNS TABLE ("Peer" VARCHAR, "PrevCount" INT)
AS $$
BEGIN
RETURN QUERY
	WITH RECURSIVE r AS (
		SELECT title, 0 AS PrevCount
		FROM tasks
		WHERE parent_task IS NULL

		UNION ALL

        SELECT tasks.title, r.PrevCount + 1
        FROM r, tasks
        WHERE r.title = tasks.parent_task
	)
	SELECT * FROM r;
END
$$ LANGUAGE plpgsql;

-- Проверка
SELECT * FROM prev_count_for_tasks();



-- part13
-- DROP FUNCTION IF EXISTS find_lucky_days_for_checks(N bigint);
-- Создание функции find_lucky_days_for_checks(N bigint), которая возвращает
-- таблицу счастливыми днями для проверки:
CREATE OR REPLACE FUNCTION find_lucky_days_for_checks(N bigint)
RETURNS TABLE ("LuckyDay" DATE)
AS $$
BEGIN
RETURN QUERY
	-- Выборка проверок с временем
	WITH check_with_time AS (
		SELECT c.id as id, c.date c_date, task, MIN(p2p.time) as c_time
		FROM checks c
		JOIN p2p ON c.id = p2p.check_id
		GROUP BY c.id
	),
	-- Выборка проверок с временем и статусом успешности и необходимым опытом
	check_with_status AS (
		SELECT c.id as id, c_date, task, c_time, xp.xp_amount as exp
		FROM check_with_time c
		LEFT JOIN xp ON c.id = xp.check_id 
		WHERE xp.xp_amount IS NOT NULL
    ),
	-- Расчет процента опыта для проверок
    check_percent AS (
        SELECT c.id as id, c_date, c_time, 1 as exp_perc
        FROM check_with_status c
        JOIN tasks t ON c.task like concat('%', t.title, '%')
    ),
	-- Определение последовательных успешных проверок
    consecutive_successful_checks AS (
		SELECT id, c_date, row_number() over (partition by c_date, grp order by c_time) "Count"
        FROM (
			SELECT id, c_date, c_time, exp_perc, sum(grp) over (partition by c_date order by c_time) grp
			FROM (SELECT *, case exp_perc
				  WHEN lag(exp_perc) over (partition by c_date order by c_time, exp_perc)
				  	THEN 0 ELSE 1 end grp
                  FROM check_percent
				 ) x
		) y
	)
	-- Выборка счастливых дней, где количество подряд идущих успешных проверок больше или равно N
	SELECT c.c_date as lucky_day
	FROM (SELECT * FROM consecutive_successful_checks csc WHERE csc."Count" >= N) c
	GROUP BY c.c_date;
END
$$ LANGUAGE plpgsql;

-- Проверка
SELECT * FROM find_lucky_days_for_checks(3);



-- part14
-- DROP FUNCTION IF EXISTS peer_with_max_xp();
-- Создание функции peer_with_max_xp(), которая возвращает
-- таблицу с данными о пире с наибольшим количеством XP:
CREATE OR REPLACE FUNCTION peer_with_max_xp()
RETURNS TABLE ("Peer" VARCHAR, "XP" INTEGER)
AS $$
BEGIN
RETURN QUERY
	-- Выборка пиров и их общего количества XP
	WITH peer_and_xp AS (
		SELECT checks.peer, SUM(xp_amount)::INTEGER as XP
        FROM xp
        LEFT JOIN checks ON xp.check_id = checks.id
        GROUP BY checks.peer
	)
	-- Выборка пиров с максимальным количеством XP
	SELECT * FROM peer_and_xp
	WHERE peer_and_xp.xp = (SELECT MAX(peer_and_xp.xp) FROM peer_and_xp);
END;
$$ LANGUAGE plpgsql;

-- Проверка
SELECT * FROM peer_with_max_xp();



-- part15
-- DROP FUNCTION IF EXISTS early_peers("Time" TIME, N INT);
-- Создание функции early_peers("Time" TIME, N INT), которая возвращает таблицу с данными
-- о пирах, приходивших раньше заданного времени не менее N раз за всё время:
CREATE OR REPLACE FUNCTION early_peers("Time" TIME, N INT)
RETURNS TABLE ("Peer" VARCHAR)
AS $$
BEGIN
RETURN QUERY
	-- Выборка пиров и их общего количества ранних посещений
	WITH temp_table AS (
    	SELECT timetracking.peer, COUNT(timetracking.peer) as c
        FROM timetracking
		WHERE time < "Time" AND state = 1
        GROUP BY timetracking.peer
	)
	-- Выборка пиров, удовлетворяющих условию на количество ранних посещений
	SELECT temp_table.peer
	FROM temp_table
	WHERE temp_table.c >= N;

END;
$$ LANGUAGE plpgsql;

-- Проверка
SELECT * FROM early_peers('15:00:00', 2);
SELECT * FROM timetracking;



-- part16
-- DROP FUNCTION IF EXISTS who_went_out(M INT, N INT);
-- Создание функции who_went_out(M INT, N INT), которая возвращает таблицу с данными
-- о пирах, выходивших за последние N дней из кампуса больше M раз:
CREATE OR REPLACE FUNCTION who_went_out(M INT, N INT)
RETURNS TABLE("Peer" VARCHAR)
AS $$
BEGIN
RETURN QUERY
	-- Выборка пиров, выходивших за последние N дней из кампуса больше M раз
	SELECT tt.peer
	FROM timetracking tt
	WHERE tt.state = 2 AND tt.date BETWEEN current_date - N AND current_date
	GROUP BY tt.peer
	HAVING COUNT(*) >= M;
END;
$$ LANGUAGE plpgsql;

-- Проверка
SELECT * FROM who_went_out(3, 10);



-- part17
-- DROP FUNCTION IF EXISTS birthday_attendance();
-- Создание функции birthday_attendance(), которая возвращает таблицу с
-- процентами ранних входов для каждого месяца :
CREATE OR REPLACE FUNCTION birthday_attendance()
RETURNS TABLE(Month VARCHAR, EarlyEntries INT)
AS $$
BEGIN
	RETURN QUERY
	WITH alter_timetracking AS (
		-- Выбираем данные из timetracking с добавлением столбца month, содержащего название месяца
		SELECT peer, TO_CHAR(date, 'Month') AS month, time, state
        FROM timetracking
	),
	alter_peers AS (
		-- Выбираем данные из peers с добавлением столбца bmonth, содержащего название месяца рождения
		SELECT nickname, TO_CHAR(birthday, 'Month') AS bmonth
        FROM peers
	),
	temp_one AS (
		-- Подсчитываем общее количество входов для каждого месяца рождения
		SELECT alter_timetracking.month, COUNT(DISTINCT peer) AS c1
		FROM alter_timetracking
		JOIN alter_peers ON peer = nickname
		WHERE state = 1
		GROUP BY alter_timetracking.month
	),
	-- Подсчитываем количество ранних входов (до 12:00) для каждого месяца рождения
	temp_two AS (
		SELECT alter_timetracking.month, COUNT(DISTINCT peer) AS c2
		FROM alter_timetracking
		JOIN alter_peers ON peer = nickname
		WHERE state = 1 AND time < '12:00'
		GROUP BY alter_timetracking.month
	)
	-- Выбираем данные из временных таблиц и рассчитываем проценты
	SELECT temp_one.month::VARCHAR, (temp_two.c2 * 100 / temp_one.c1)::INT
	FROM temp_one
	JOIN temp_two ON temp_one.month = temp_two.month;
END
$$ LANGUAGE plpgsql;

-- Проверка
SELECT * FROM birthday_attendance();

