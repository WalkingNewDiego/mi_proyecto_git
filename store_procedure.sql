-- =======================
-- SP para Clientes
-- =======================
DELIMITER $$

CREATE DEFINER=`root`@`localhost` PROCEDURE sp_insert_cliente(
    IN p_cliente_id INT,
    IN p_nombre VARCHAR(100),
    IN p_apellido VARCHAR(100),
    IN p_email VARCHAR(150),
    IN p_fecha_registro DATETIME
)
BEGIN
    DECLARE v_count INT DEFAULT 0;
    DECLARE v_next_num INT DEFAULT 0;
    DECLARE v_control_numero VARCHAR(10);

    -- Verificar si ya existe el email
    SELECT COUNT(*) INTO v_count
    FROM clientes
    WHERE email = p_email;

    IF v_count = 0 THEN
        -- Calcular siguiente número de control
        SELECT IFNULL(MAX(CAST(SUBSTRING(control_numero, 3) AS UNSIGNED)), 0) + 1
        INTO v_next_num
        FROM clientes;

        -- Generar formato DR0001
        SET v_control_numero = CONCAT('DR', LPAD(v_next_num, 4, '0'));

        -- Insertar nuevo cliente
        INSERT INTO clientes (
            cliente_id, nombre, apellido, email, FechaRegistro, control_numero, fecha_registro
        )
        VALUES (
            p_cliente_id, p_nombre, p_apellido, p_email, p_fecha_registro, v_control_numero, NOW()
        );
    END IF;
END$$

DELIMITER ;

-- =======================
-- SP para Usuarios
-- =======================
DELIMITER $$

CREATE PROCEDURE sp_insert_usuario (
    IN p_userId INT,
    IN p_username VARCHAR(100),
    IN p_first_name VARCHAR(100),
    IN p_last_name VARCHAR(100),
    IN p_email VARCHAR(150),
    IN p_password_hash VARCHAR(255),
    IN p_rol VARCHAR(50),
    IN p_fecha DATETIME
)
BEGIN
    IF (SELECT COUNT(*) FROM usuarios WHERE email = p_email OR username = p_username) = 0 THEN
        INSERT INTO usuarios (userId, username, first_name, last_name, email, password_hash, rol, fecha_creacion)
        VALUES (p_userId, p_username, p_first_name, p_last_name, p_email, p_password_hash, p_rol, p_fecha);
    END IF;
END$$

DELIMITER ;

-- =======================
-- SP Rapidfuzz
-- =======================
DELIMITER $$

CREATE PROCEDURE sp_insert_csv_table (
    IN p_table_name VARCHAR(100),
    IN p_columns TEXT,
    IN p_values TEXT
)
BEGIN
    SET @query = CONCAT(
        'INSERT INTO ', p_table_name,
        ' (`', REPLACE(p_columns, ',', '`,`'), '`) ',
        'VALUES (', p_values, ');'
    );
    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END$$

DELIMITER ;