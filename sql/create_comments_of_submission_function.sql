DROP FUNCTION IF EXISTS comments_of_submission;
CREATE FUNCTION comments_of_submission (@submission_name VARCHAR(15))
RETURNS TABLE
AS
RETURN
	SELECT * FROM comments WHERE link_id = @submission_name;