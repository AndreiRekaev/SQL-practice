--part1
CREATE OR REPLACE PROCEDURE add_p2p_check (
    checking_peer_nick VARCHAR(50),
    checked_peer_nick VARCHAR(50),
    task_title VARCHAR(50),
    p2p_status check_status,
    p2p_time time)
    LANGUAGE plpgsql
AS $$
DECLARE
    check_id_val BIGINT;
    today_date DATE;
BEGIN
    -- Получаем сегодняшнюю дату
    today_date := CURRENT_DATE;

    -- Проверяем, задан ли статус "начало"
    IF p2p_status = 'start' THEN
        -- добавляем запись в таблицу Checks
        INSERT INTO checks (peer, task, date)
        VALUES (checking_peer_nick, task_title, today_date)
        returning id INTO check_id_val; -- получаем id новой проверки

    ELSE
        -- Ищем незавершенную проверку с такими параметрами
        select id into check_id_val
            from checks
        where peer = checking_peer_nick AND task = task_title AND id NOT IN (
            select check_id
            from p2p
            where checking_peer = checking_peer_nick AND state <> 'success'
            );
    END IF;

    -- проверяем найдена ли проверка
    if check_id_val IS NOT NULL THEN
        -- добавлеям запись в таблицу p2p
        INSERT INTO p2p (check_id, checking_peer, state, time)
        VALUES (check_id_val, checking_peer_nick, p2p_status, p2p_time);
    else
        RAISE EXCEPTION 'Unable to find or create a valid check for the specified paramenters. ';
    END IF;
    END;
$$;


--part2
CREATE OR REPLACE PROCEDURE add_verter_check (
    checked_peer_nick VARCHAR(50),
    task_title VARCHAR(50),
    verter_status CHECK_STATUS,
    verter_time TIME
    )LANGUAGE plpgsql
AS $$DECLARE
    check_id_val BIGINT;
    today_date DATE;
BEGIN
    -- Получаем текущую дату
    today_date := CURRENT_DATE;
    -- Проверяем, задан ли статус "success"
    IF verter_status = 'success' THEN
        -- Ищем проверку соответствующего задания с самым поздним успешным P2P этапом
        SELECT c.id INTO check_id_val
        FROM checks c
        JOIN p2p p ON c.id = p.check_id
        WHERE c.peer = checked_peer_nick AND c.task = task_title AND p.state = 'success'
        ORDER BY p.time DESC
        LIMIT 1;
        -- Добавляем запись в таблицу Verter
        INSERT INTO verter (check_id, state, time)
        VALUES (check_id_val, verter_status, verter_time);
        ELSE
        RAISE EXCEPTION 'Invalid verter status';
        END IF;
END;$$;


--part3
CREATE OR REPLACE FUNCTION update_transferred_points()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Проверяем, что добавленная запись в P2P имеет статус "start"
    IF NEW.state = 'start' THEN
        -- Обновляем запись в TransferredPoints
        UPDATE transferredpoints
        SET points_amount = points_amount + 1
        WHERE checking_peer = NEW.checking_peer AND checked_peer = NEW.checking_peer;
    END IF;

    RETURN NEW;
END;
$$;

-- Привязываем триггер к таблице P2P
CREATE TRIGGER update_transferred_points_trigger
AFTER INSERT ON p2p
FOR EACH ROW
WHEN (NEW.state = 'start')
EXECUTE FUNCTION update_transferred_points();


--part4
CREATE OR REPLACE FUNCTION valid_xp()
RETURNS TRIGGER AS $$
BEGIN
	IF NEW.XPAmount > (SELECT Tasks.MaxXP
					  FROM Checks
					  JOIN Tasks ON Tasks.Title = Checks.Task
					  JOIN XP ON Checks.ID = XP."Check"
					  LIMIT 1)
		THEN RAISE EXCEPTION 'XP amount can''''t exceed maximum XP for current task';			
	END IF;
	IF (SELECT COUNT(*)
	   FROM Checks
	   LEFT JOIN Verter ON Verter."Check" = Checks.ID
	   LEFT JOIN P2P ON P2P."Check" = Checks.ID
	   WHERE ((Verter."State" = 'Success' OR Verter."State" IS NULL) AND P2P."State" = 'Success')) > 0
	THEN RETURN NEW;
	ELSE RETURN NULL;
	END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_valid_xp
BEFORE INSERT ON XP
FOR EACH ROW
EXECUTE FUNCTION valid_xp();
