DROP VIEW IF EXISTS crossposts;
CREATE VIEW crossposts AS
	(
		SELECT (*)
		FROM submissions
		WHERE crosspost_parent IS NOT NULL
	);