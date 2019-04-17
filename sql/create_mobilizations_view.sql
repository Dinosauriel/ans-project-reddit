DROP VIEW IF EXISTS mobilizations;
CREATE VIEW mobilizations AS
	(
		SELECT * FROM crossposts c
		WHERE comments_of_submission(c.crosspost_parent)
	);