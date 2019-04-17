DROP PROCEDURE IF EXISTS comments_of_subm;
DELIMITER //
CREATE PROCEDURE comments_of_subm (subm_name VARCHAR(15))
BEGIN
	SELECT * FROM comments WHERE link_id = subm_name;
END//
DELIMITER ;