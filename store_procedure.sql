
DELIMITER $$

CREATE PROCEDURE insertarArchivoExportado(
    IN p_name_complet TEXT,
    IN p_match_query TEXT,
    IN p_match_result TEXT,
    IN p_score TEXT,
    IN p_match_result_values TEXT,
    IN p_destTable TEXT,
    IN p_sourceTable TEXT
)
BEGIN
    INSERT INTO archivo_exportado (
        name_complet,
        match_query,
        match_result,
        score,
        match_result_values,
        destTable,
        sourceTable
    )
    VALUES (
        p_name_complet,
        p_match_query,
        p_match_result,
        p_score,
        p_match_result_values,
        p_destTable,
        p_sourceTable
    );
END$$

DELIMITER ;


