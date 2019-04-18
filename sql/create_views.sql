DROP VIEW IF EXISTS crossposts;
DROP VIEW IF EXISTS crosspost_targets;
DROP PROCEDURE IF EXISTS comments_of_subm;




CREATE VIEW crossposts AS
	(
		SELECT *
		FROM submissions
		WHERE crosspost_parent IS NOT NULL
	);


CREATE VIEW crosspost_targets AS
	(
		SELECT s.* FROM submissions s, crossposts c WHERE CONCAT("t3_", s.id) = c.crosspost_parent
	);


DELIMITER //
CREATE PROCEDURE comments_of_subm (subm_name VARCHAR(15))
BEGIN
	SELECT * FROM comments WHERE link_id = subm_name;
END//
DELIMITER ;


DROP VIEW IF EXISTS mobilizations;
CREATE VIEW mobilizations AS
	(
		SELECT * FROM crossposts c
		WHERE comments_of_submission(c.crosspost_parent)
	);