-- 1. Иерархия папок номенклатур
WITH RECURSIVE folder_tree AS (
-- Базовый запрос для корневых папок
    SELECT 
        n.nomenclature_id,
        n.name AS folder_name,
        CAST(NULL AS INT) AS parent_id,
        1 AS level, -- Уровень вложенности (начинается с 1)
        JSON_ARRAY(n.name) AS full_path -- Путь в формате JSON массива
    FROM
    	nomenclatures n
    WHERE 
        n.root_folder_id = 0   -- Только корневые папки
        AND n.deleted = 0     -- Не удаленные
        AND n.isFolder = 1    -- Только папки
        
 -- Рекурсивная часть для дочерних папок  
    UNION ALL
    
    SELECT 
        child.nomenclature_id,
        child.name,
        parent.nomenclature_id,
        parent.level + 1, -- Увеличиваем уровень вложенности
        JSON_ARRAY_APPEND(parent.full_path, '$', child.name)-- Добавляем к пути
    FROM 
    	nomenclatures child
    JOIN 
    	folder_tree parent 
        ON child.folder = parent.nomenclature_id
    WHERE 
        child.deleted = 0 
        AND child.isFolder = 1
        AND parent.level < 4 -- Ограничение глубины
),
-- Преобразование иерархии папок в плоскую структуру для номенклатур
hierarcy_nomenclatures AS (
	SELECT
	    n.nomenclature_code,
	    n.name,
	    -- Извлечение отдельных уровней иерархии из JSON пути
	    CAST(JSON_VALUE(ft.full_path, '$[3]') as VARCHAR(50)) AS folder_3, -- Уровень 3
	    CAST(JSON_VALUE(ft.full_path, '$[2]') as VARCHAR(50)) AS folder_2, -- Уровень 2
	    CAST(JSON_VALUE(ft.full_path, '$[1]') as VARCHAR(50)) AS folder_1, -- Уровень 1
	    CAST(JSON_VALUE(ft.full_path, '$[0]') as VARCHAR(50)) AS root_folder -- Корневой уровень
	FROM 
		nomenclatures n
	LEFT JOIN
		folder_tree ft 
		ON n.folder=ft.nomenclature_id   -- Связь товара с папкой
		AND n.isFolder IS NULL -- Только номенклатуры (не папки) 
		AND n.deleted =0       -- Не удаленные товары
	ORDER BY full_path
), 
-- 2. Иерархия папок отделов с различными уровнями вложенности
-- 2.1 Первый набор подразделений (определенные parent_department_id)
subdirectories AS (
-- Базовый запрос для корневых папок. Отделы с определенными родительскими ID
	SELECT
	    d.department_id AS department_id,
	    d.department_name AS department_name,
	    d.parent_department_id AS parent_department_id,
	    d.department_id AS root_id -- Сохраняем исходный ID как корневой
	FROM
	    departments d
	WHERE
	    d.parent_department_id IN (2831576, 2831586, 42561, 42560, 2124383, 2743029, 2743027)
	    
 -- Рекурсивная часть для дочерних папок  
    UNION ALL
    
	SELECT
	    d.department_id AS department_id,
	    d.department_name AS department_name,
	    d.parent_department_id AS parent_department_id,
	    s.root_id AS root_id -- Наследуем корневой ID от родителя
	FROM
	    departments d
	JOIN 
		subdirectories s 
		ON d.parent_department_id = s.department_id
), 

-- 2.2 Второй набор подразделений (определенные department_id)
subdirectories1 AS (
-- Базовый запрос для корневых папок. Отделы с определенными родительскими ID
	SELECT
	    d.department_id AS department_id,
	    d.department_name AS department_name,
	    d.parent_department_id AS parent_department_id,
	    d.department_id AS root_id
	FROM
	    departments d
	WHERE
	    d.department_id IN (2831542, 2831543, 59044, 2743041, 2831574, 2831602, 2831609, 2831612, 3081821)
	    
 -- Рекурсивная часть для дочерних папок  
	UNION ALL
	
	SELECT
	    d.department_id AS department_id,
	    d.department_name AS department_name,
	    d.parent_department_id AS parent_department_id,
	    s.root_id AS root_id
	FROM
	    departments d
	JOIN 
		subdirectories1 s 
		ON d.parent_department_id = s.department_id
), 
-- Объединение иерархий отделов с преобразованием в структуру: отдел -> секция -> сектор
hierarchy_of_departments AS (
    -- Для первого набора подразделений
	SELECT 
	    distinct s.department_id AS department_id,
	    s.department_name AS sector,        -- Самый низкий уровень (сектор)
	    s.root_id AS original_parent_id,    -- Исходный родительский ID
	    d.department_name AS section,       -- Средний уровень (секция)
	    d2.department_name AS department    -- Высший уровень (отдел)
	FROM
	    subdirectories s
	JOIN 
		departments d 
		ON s.root_id = d.department_id        -- Связь с родительским отделом
	JOIN 
		departments d2
		ON d.parent_department_id = d2.department_id -- Связь с отделом верхнего уровня
	    
-- Для второго набора подразделений (двухуровневая структура)	   
	UNION ALL
	
	SELECT
	    distinct s1.department_id AS department_id,
	    s1.department_name AS sector,       -- Сектор
	    s1.root_id AS original_parent_id,
	    d.department_name AS section,       -- Секция (совпадает с отделом)
	    d.department_name AS department     -- Отдел
	FROM
	    subdirectories1 s1
	JOIN 
		departments d 
		ON s1.root_id = d.department_id
	    
-- Отделы без иерархии	    
	UNION ALL
	
	SELECT
	    d.department_id AS department_id,
	    d.department_name AS sector,        -- Сектор
	    d.department_id AS original_parent_id,
	    d.department_name AS section,       -- Секция (совпадает с отделом)
	    d.department_name AS department     -- Отдел
	FROM
	    departments d
	WHERE
	    d.department_id IN (2831576, 2831586, 2831572, 42561, 42560, 2124383, 2743029, 2743027)
), 

-- 3. Продажи. Разделение на два типа реализаций:
--   1. Со сменой автора реализации (отличается от автора счёта)
--   2. Без смены автора
sales AS (
    -- Продажи со сменой автора реализации
	SELECT 
		d.sbis_shipment_date,          -- Дата отгрузки
		d.document_id,                 -- ID документа
		d.`number`,                    -- Номер документа
		d2.department_id,              -- ID отдела до смены автора раелизации(автора счёта)
		d2.employee_id,                -- ID сотрудника до смены автора раелизации(автор счёта)
		"changed" as realization       -- Признак смены автора
	FROM 
		documents d 
	JOIN
		view_departments_with_root_folders d3 
		ON d.department_id = d3.department_id  -- Объединяем со списком отделов с указанными корневыми каталогами
		AND d3.root_id = 59041	-- Корневой каталог "Партнеры"(отбираем только отгрузки, где автор - Партнёр)
		AND d.sbis_shipment_date>='2025-01-01'	  -- Отбираем первое полугодие 2025 года
		AND d.sbis_shipment_date<'2025-08-01'
		AND d.shipment = 1             			  -- Только документы с признаком "отгрузка"
		AND d.invoice_total_sum_calculated>0 	  -- С положительной суммой
		AND d.deleted =0
	JOIN 
		documents d2 
		ON d.`number` =d2.`number`   -- Связь по номеру документа
		AND d2.deleted =0
		AND d2.document_type ="outbill"			  -- Только документы с признаком "исходящий счёт"
		AND d.employee_id!=d2.employee_id		  -- Сравнение автора докумнта из списка реализаций с автором счёта
	
	UNION ALL
	
	-- Все продажи без проверки смены автора реализации
	SELECT 
		d.sbis_shipment_date,          -- Дата отгрузки
		d.document_id,                 -- ID документа
		d.`number`,                    -- Номер документа
		d.department_id,               -- ID отдела д
		d.employee_id,                 -- ID сотрудника 
		"not changed" AS realization   -- Признак смены автора
	FROM 
		documents d
	WHERE d.sbis_shipment_date>='2025-01-01'
		AND d.sbis_shipment_date<'2025-08-01'
		AND d.shipment = 1
		AND d.invoice_total_sum_calculated>0
		AND d.deleted =0
)
-- 4. Итоговая витрина данных для анализа продаж
SELECT
	DATE_FORMAT(s.sbis_shipment_date, '%Y-%m-%d') AS date, -- Форматированная дата
	s.`number` AS number,                                 -- Номер документа
    
    -- Группировка отделов по категориям
	CASE 
		WHEN hd.department IN ('Представители Киров', 'Представители Ижевск', 'Рефералы')
		THEN 'Представители'
		WHEN hd.department IS NULL
		THEN 'Не известен'
		ELSE 'Офис'
	END AS root_department   
    
    -- Иерархия подразделений с обработкой NULL значений
    IFNULL(hd.department, 'Не известен') AS department,   -- Отдел
    IFNULL(hd.section, 'Не известна') AS section,         -- Секция
    IFNULL(hd.sector, 'Не известен') AS sector,           -- Сектор
    
    -- Форматирование имени сотрудника
	CASE 
		WHEN e.last_name IS NULL
		THEN 'Не известен'
		ELSE concat(e.first_name, ' ', LEFT(e.last_name, 1),'.')
	END	 AS employe_name,
	
	-- Иерархия номенклатуры
    hnf.root_folder,  -- Корневая папка
    hnf.folder_1,     -- Папка уровня 1
    hnf.folder_2,     -- Папка уровня 2
    hnf.folder_3,     -- Папка уровня 3
    hnf.name,         -- Наименование номенклатуры
      
    dtp.nomenclature_price_total, -- Сумма по номенклатуре
    s.realization                 -- Тип реализации (с изменением автора или без)
FROM 
	sales s 
JOIN 
	documents_tabular_part dtp 
	ON s.document_id =dtp.document_id
LEFT JOIN 
	hierarcy_nomenclatures hnf
	ON dtp.nomenclature_code = hnf.nomenclature_code
LEFT JOIN 
	hierarchy_of_departments hd 
	ON s.department_id =hd.department_id
LEFT JOIN
	employees e 
	ON s.employee_id=e.employee_id
