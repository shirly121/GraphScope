CREATE NODE TABLE AKA_NAME(id INT32, name STRING, imdb_index STRING, name_pcode_cf STRING, name_pcode_nf STRING, surname_pcode STRING, md5sum STRING, PRIMARY KEY (id));
CREATE NODE TABLE AKA_TITLE(id INT32, title STRING, imdb_index STRING, kind_id INT32, production_year INT32, phonetic_code STRING, episode_of_id INT32, season_nr INT32, episode_nr INT32, note STRING, md5sum STRING, PRIMARY KEY (id));
CREATE NODE TABLE TITLE(id INT32, title STRING, imdb_index STRING, production_year INT32, imdb_id INT32, phonetic_code STRING, episode_of_id INT32, season_nr INT32, episode_nr INT32, series_years STRING, md5sum STRING, PRIMARY KEY (id));
CREATE NODE TABLE CHAR_NAME(id INT32, name STRING, imdb_index STRING, imdb_id INT32, name_pcode_nf STRING, surname_pcode STRING, md5sum STRING, PRIMARY KEY (id));
CREATE NODE TABLE COMP_CAST_TYPE(id INT32, kind STRING, PRIMARY KEY (id));
CREATE NODE TABLE COMPANY_NAME(id INT32, name STRING, country_code STRING, imdb_id INT32, name_pcode_nf STRING, name_pcode_sf STRING, md5sum STRING, PRIMARY KEY (id));
CREATE NODE TABLE COMPANY_TYPE(id INT32, kind STRING, PRIMARY KEY (id));
CREATE NODE TABLE INFO_TYPE(id INT32, info STRING, PRIMARY KEY (id));
CREATE NODE TABLE KEYWORD(id INT32, keyword STRING, phonetic_code STRING, PRIMARY KEY (id));
CREATE NODE TABLE KIND_TYPE(id INT32, kind STRING, PRIMARY KEY (id));
CREATE NODE TABLE LINK_TYPE(id INT32, link STRING, PRIMARY KEY (id));
CREATE NODE TABLE NAME(id INT32, name STRING, imdb_index STRING, imdb_id INT32, gender STRING, name_pcode_cf STRING, name_pcode_nf STRING, surname_pcode STRING, md5sum STRING, PRIMARY KEY (id));
CREATE NODE TABLE ROLE_TYPE(id INT32, role STRING, PRIMARY KEY (id));
CREATE NODE TABLE CAST_INFO(id INT32, person_id INT32, movie_id INT32, person_role_id INT32, note STRING, nr_order INT32, role_id INT32, PRIMARY KEY (id));
CREATE NODE TABLE COMPLETE_CAST(id INT32, movie_id INT32, subject_id INT32, status_id INT32, PRIMARY KEY (id));
CREATE NODE TABLE MOVIE_COMPANIES(id INT32, movie_id INT32, company_id INT32, company_type_id INT32, note STRING, PRIMARY KEY (id));
CREATE NODE TABLE MOVIE_LINK(id INT32, movie_id INT32, linked_movie_id INT32, link_type_id INT32, PRIMARY KEY (id));
CREATE REL TABLE ALSO_KNOWN_AS_NAME(FROM AKA_NAME TO NAME);
CREATE REL TABLE ALSO_KNOWN_AS_TITLE(FROM AKA_TITLE TO TITLE);
CREATE REL TABLE KIND_TYPE_TITLE(FROM KIND_TYPE TO TITLE);
CREATE REL TABLE MOVIE_INFO(FROM TITLE TO INFO_TYPE, info STRING, note STRING, id INT32);
CREATE REL TABLE MOVIE_INFO_IDX(FROM TITLE TO INFO_TYPE, info STRING, note STRING, id INT32);
CREATE REL TABLE PERSON_INFO(FROM NAME TO INFO_TYPE, info STRING, note STRING, id INT32);
CREATE REL TABLE MOVIE_KEYWORD(FROM TITLE TO KEYWORD, id INT32);
CREATE REL TABLE CAST_INFO_NAME(FROM CAST_INFO TO NAME);
CREATE REL TABLE CAST_INFO_TITLE(FROM CAST_INFO TO TITLE);
CREATE REL TABLE CAST_INFO_CHAR(FROM CAST_INFO TO CHAR_NAME);
CREATE REL TABLE CAST_INFO_ROLE(FROM CAST_INFO TO ROLE_TYPE);
CREATE REL TABLE COMPLETE_CAST_TITLE(FROM COMPLETE_CAST TO TITLE);
CREATE REL TABLE COMPLETE_CAST_SUBJECT(FROM COMPLETE_CAST TO COMP_CAST_TYPE);
CREATE REL TABLE COMPLETE_CAST_STATUS(FROM COMPLETE_CAST TO COMP_CAST_TYPE);
CREATE REL TABLE MOVIE_COMPANIES_TITLE(FROM MOVIE_COMPANIES TO TITLE);
CREATE REL TABLE MOVIE_COMPANIES_COMPANY_NAME(FROM MOVIE_COMPANIES TO COMPANY_NAME);
CREATE REL TABLE MOVIE_COMPANIES_TYPE(FROM MOVIE_COMPANIES TO COMPANY_TYPE);
CREATE REL TABLE MOVIE_LINK_LINKED_TITLE(FROM MOVIE_LINK TO TITLE);
CREATE REL TABLE MOVIE_LINK_TITLE(FROM MOVIE_LINK TO TITLE);
CREATE REL TABLE MOVIE_LINK_LINKED_TYPE(FROM MOVIE_LINK TO LINK_TYPE);
