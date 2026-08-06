-- 2. 清洗用户状态变更表
drop table if exists mediamatch_userevent_clean;
create table mediamatch_userevent_clean as
select distinct *
from mediamatch_userevent
where run_name in ('正常','主动暂停','欠费暂停','主动销户');