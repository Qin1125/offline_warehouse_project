-- 5. 清洗收视行为表（先建有效用户临时表）
drop table if exists media_index_clean;
create table media_index_clean as
select m.*
from media_index m
inner join (select distinct phone_no from mediamatch_usermsg_clean) v
on m.phone_no = v.phone_no
where m.owner_name not rlike '^E[ABCDE]级'
  and m.owner_code not in ('02','09','10')
  and m.sm_name in ('珠江宽频','数字电视','互动电视','甜果电视')
  and cast(m.duration as double) >= 20000
  and cast(m.duration as double) <= 18000000
  and not (
      m.res_type = '0'
      and regexp_extract(m.origin_time, ':([0-9]{2})$', 1) = '00'
      and regexp_extract(m.end_time, ':([0-9]{2})$', 1) = '00'
  );