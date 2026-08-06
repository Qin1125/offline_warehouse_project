-- 3. 清洗账单表
drop table if exists mmconsume_billevents_clean;
create table mmconsume_billevents_clean as
select distinct *
from mmconsume_billevents
where owner_name not rlike '^E[ABCDE]级'
  and owner_code not in ('02','09','10')
  and sm_name in ('珠江宽频','数字电视','互动电视','甜果电视');