-- 4. 清洗订单表
drop table if exists order_index_clean;
create table order_index_clean as
select distinct *
from order_index
where owner_name not rlike '^E[ABCDE]级'
  and owner_code not in ('02','09','10')
  and sm_name in ('珠江宽频','数字电视','互动电视','甜果电视')
  and run_name in ('正常','主动暂停','欠费暂停','主动销户');