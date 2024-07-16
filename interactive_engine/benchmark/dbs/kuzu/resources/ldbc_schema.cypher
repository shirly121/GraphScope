CREATE NODE TABLE PLACE(id INT64, name STRING, url STRING, type STRING, PRIMARY KEY (id));
CREATE NODE TABLE PERSON(id INT64, firstName STRING, lastName STRING, gender STRING, birthday INT64, creationDate INT64, locationIP STRING, browserUsed STRING, PRIMARY KEY (id));
CREATE NODE TABLE COMMENT(id INT64, creationDate INT64, locationIP STRING, browserUsed STRING, content STRING, length INT32, PRIMARY KEY (id));
CREATE NODE TABLE POST(id INT64, imageFile STRING, creationDate INT64, locationIP STRING, browserUsed STRING, language STRING, content STRING, length INT32, PRIMARY KEY (id));
CREATE NODE TABLE FORUM(id INT64, title STRING, creationDate INT64, PRIMARY KEY (id));
CREATE NODE TABLE ORGANISATION(id INT64, type STRING, name STRING, url STRING, PRIMARY KEY (id));
CREATE NODE TABLE TAGCLASS(id INT64, name STRING, url STRING, PRIMARY KEY (id));
CREATE NODE TABLE TAG(id INT64, name STRING, url STRING, PRIMARY KEY (id));
CREATE REL TABLE COMMENT_HASCREATOR_PERSON(FROM COMMENT TO PERSON);
CREATE REL TABLE POST_HASCREATOR_PERSON(FROM POST TO PERSON);
CREATE REL TABLE POST_HASTAG_TAG(FROM POST TO TAG);
CREATE REL TABLE FORUM_HASTAG_TAG(FROM FORUM TO TAG);
CREATE REL TABLE COMMENT_HASTAG_TAG(FROM COMMENT TO TAG);
CREATE REL TABLE COMMENT_REPLYOF_COMMENT(FROM COMMENT TO COMMENT);
CREATE REL TABLE COMMENT_REPLYOF_POST(FROM COMMENT TO POST);
CREATE REL TABLE FORUM_CONTAINEROF_POST(FROM FORUM TO POST);
CREATE REL TABLE FORUM_HASMEMBER_PERSON(FROM FORUM TO PERSON, joinDate INT64);
CREATE REL TABLE FORUM_HASMODERATOR_PERSON(FROM FORUM TO PERSON);
CREATE REL TABLE PERSON_HASINTEREST_TAG(FROM PERSON TO TAG);
CREATE REL TABLE COMMENT_ISLOCATEDIN_PLACE(FROM COMMENT TO PLACE);
CREATE REL TABLE PERSON_ISLOCATEDIN_PLACE(FROM PERSON TO PLACE);
CREATE REL TABLE POST_ISLOCATEDIN_PLACE(FROM POST TO PLACE);
CREATE REL TABLE ORGANISATION_ISLOCATEDIN_PLACE(FROM ORGANISATION TO PLACE);
CREATE REL TABLE PERSON_KNOWS_PERSON(FROM PERSON TO PERSON, creationDate INT64);
CREATE REL TABLE PERSON_LIKES_COMMENT(FROM PERSON TO COMMENT, creationDate INT64);
CREATE REL TABLE PERSON_LIKES_POST(FROM PERSON TO POST, creationDate INT64);
CREATE REL TABLE PERSON_WORKAT_ORGANISATION(FROM PERSON TO ORGANISATION, workFrom INT32);
CREATE REL TABLE PLACE_ISPARTOF_PLACE(FROM PLACE TO PLACE);
CREATE REL TABLE TAG_HASTYPE_TAGCLASS(FROM TAG TO TAGCLASS);
CREATE REL TABLE TAGCLASS_ISSUBCLASSOF_TAGCLASS(FROM TAGCLASS TO TAGCLASS);
CREATE REL TABLE PERSON_STUDYAT_ORGANISATION(FROM PERSON TO ORGANISATION, classYear INT32);
