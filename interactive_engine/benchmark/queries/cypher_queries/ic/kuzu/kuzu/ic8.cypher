MATCH(p:PERSON {id: 4398046512194}) <-[:POST_HASCREATOR_PERSON|:COMMENT_HASCREATOR_PERSON] -(msg : POST:COMMENT) <- [:COMMENT_REPLYOF_POST|:COMMENT_REPLYOF_COMMENT] - (cmt: COMMENT) - [:COMMENT_HASCREATOR_PERSON] -> (author : PERSON)
WITH
    p, msg, cmt, author 
ORDER BY 
    cmt.creationDate DESC, 
    cmt.id ASC 
LIMIT 20 
RETURN
    author.id as personId, 
    author.firstName as personFirstName, 
    author.lastName as personLastName, 
    cmt.creationDate as commentCreationDate, 
    cmt.id as commentId, 
    cmt.content as commentContent;