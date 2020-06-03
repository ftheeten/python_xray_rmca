CREATE TABLE x_ray_drosera
(
  full_path character varying NOT NULL,
  file character varying,
  folder character varying,
  metadata_found character varying,
  object_desc character varying,
  CONSTRAINT x_ray_drosera_pkey PRIMARY KEY (full_path)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE x_ray_drosera
  OWNER TO darwin2;
  
  
CREATE OR REPLACE FUNCTION fct_rmca_compare_varchar_array_as_integer(p1 character varying[], p2 character varying[])
  RETURNS boolean AS
$BODY$
DECLARE
 returned boolean;
 i integer;
BEGIN
returned:=false;
	IF(ARRAY_LENGTH(p1,1)=ARRAY_LENGTH(p2,1)) THEN
		returned=true;
		i=1;
		WHILE returned=true AND i<= ARRAY_LENGTH(p1,1) LOOP 
			IF p1[i]::integer <>  p2[i]::integer THEN
				returned=false;
			END IF;
			i=i+1;
		END LOOP;
	END IF;
return returned;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION fct_rmca_compare_varchar_array_as_integer(character varying[], character varying[])
  OWNER TO darwin2;

  
  -- Function: fct_rmca_regexp_matches_one_line(character varying, character varying)

-- DROP FUNCTION fct_rmca_regexp_matches_one_line(character varying, character varying);

CREATE OR REPLACE FUNCTION fct_rmca_regexp_matches_one_line(main_text character varying, pattern character varying)
  RETURNS character varying[] AS
$BODY$
DECLARE
returned varchar[];
BEGIN
select array_agg(i) INTO returned
from (
 select (regexp_matches(main_text, pattern, 'g'))[1] i 
)  t;
RETURN returned;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION fct_rmca_regexp_matches_one_line(character varying, character varying)
  OWNER TO darwin2;
  
  
CREATE VIEW v_x_ray_drosera
AS
WITH 
x_ray_drosera_num
AS
(
select *,

regexp_replace(
regexp_replace(
regexp_replace(
regexp_replace(UPPER(file)
	, '([^[:alnum:]]|^)(A)(\d+)', '\1200\3'),
	'([^[:alnum:]]|^)(B)(\d+)', '\1201\3') ,
	'\(.*?\)','()'),
	'TAG.+','','g')


	as file_num,
	regexp_replace(
regexp_replace(	
regexp_replace(
regexp_replace(UPPER(object_desc)
	, '([^[:alnum:]]|^)(A)(\d+)', '\1200\3'),
	'([^[:alnum:]]|^)(B)(\d+)', '\1201\3'),
	'\(.*?\)','()','g'),
	'TAG.+','') as object_desc_num
 from x_ray_drosera
)
,
to_match_tmp
AS( 
SELECT full_path, file, folder, metadata_found, object_desc
,
fct_rmca_regexp_matches_one_line(file_num, '\d+|[a-c]d+') as file_num,

fct_rmca_regexp_matches_one_line(object_desc_num, '\d+') as object_num
  FROM x_ray_drosera_num 
),

to_match
AS
(
SELECT full_path, file, folder, metadata_found, object_desc,
CASE WHEN array_length(file_num,1) >=3 AND LENGTH(file_num[1])=2 THEN
('19'||file_num[1])::varchar||file_num[2:100]
ELSE
file_num
END file_num,
CASE WHEN array_length(object_num,1) >=3 AND LENGTH(object_num[1])=2 THEN
('19'||object_num[1])::varchar||object_num[2:100]
ELSE
object_num
END object_num
FROM to_match_tmp
)
,
code_fish AS
(SELECT codes.*, taxon_name as darwin_taxon_name FROM codes 
inner join specimens
ON record_id=specimens.id 
where referenced_relation='specimens' and code_category='main' and collection_ref=6),


 code_fish_catch as

 (
SELECT * , fct_rmca_regexp_matches_one_line(code, '\d+')  as code_num_parts, array_length( fct_rmca_regexp_matches_one_line(code, '\d+') ,1) as nb_code_num_parts  from code_fish
 )
 
,
catch_from_code1 as
(
select * from  
 to_match , code_fish_catch

where 
(
code_fish_catch.code_num_parts[1]::integer=to_match.file_num[1]::integer
AND  fct_rmca_compare_varchar_array_as_integer(code_fish_catch.code_num_parts,to_match.file_num)
AND array_length(code_fish_catch.code_num_parts,1)= array_length(to_match.file_num,1)
)

),

catch_from_code2 as
(
select * from  
 to_match , code_fish_catch

where 

(
code_fish_catch.code_num_parts[1]::integer=to_match.object_num[1]::integer
AND  fct_rmca_compare_varchar_array_as_integer(code_fish_catch.code_num_parts,to_match.object_num)
AND array_length(code_fish_catch.code_num_parts,1)= array_length(to_match.object_num,1)
)
),

tmp_match
As
(

SELECT * FROM catch_from_code1
union select * from catch_from_code2  
 )
,
unmatched as
(
SELECt to_match.* from 
  to_match LEFT JOIN tmp_match
ON to_match.full_path=tmp_match.full_path
WHERE tmp_match.full_path IS NULL
)
select 'MATCHED' as matched, * from tmp_match
UNION
SELECT 'UNMATCHED', * , 
NULL::varchar, 
NULL::integer,
 NULL::integer, 
NULL::varchar, 
NULL::varchar, 
NULL::varchar, 
NULL::varchar, 
NULL::varchar, 
NULL::varchar, 
NULL::varchar, 
NULL::timestamp without time zone, 
NULL::int,
NULL::int,
NULL::bigint, 
NULL::varchar, 
NULL::varchar[],
NULL::int FROM unmatched 


 

  ;
