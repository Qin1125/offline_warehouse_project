-- 1. 清洗用户信息表
drop table if exists mediamatch_usermsg_clean;
create table mediamatch_usermsg_clean as
with temp as (
    select *,
           row_number() over (partition by phone_no order by run_time desc) as rn
    from mediamatch_usermsg
    where owner_name not rlike '^E[ABCDE]级'
      and owner_code not in ('02','09','10')
      and sm_name in ('珠江宽频','数字电视','互动电视','甜果电视')
      and run_name in ('正常','主动暂停','欠费暂停','主动销户')
)
select * from temp where rn = 1;