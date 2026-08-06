-- 6. 统计清洗前后记录数变化
select 'usermsg' as 表名,
       (select count(*) from mediamatch_usermsg) as 原始行数,
       (select count(*) from mediamatch_usermsg_clean) as 清洗后行数,
       concat(cast((1 - (select count(*) from mediamatch_usermsg_clean) / (select count(*) from mediamatch_usermsg)) * 100 as decimal(5,2)), '%') as 剔除比例
union all
select 'userevent',
       (select count(*) from mediamatch_userevent),
       (select count(*) from mediamatch_userevent_clean),
       concat(cast((1 - (select count(*) from mediamatch_userevent_clean) / (select count(*) from mediamatch_userevent)) * 100 as decimal(5,2)), '%')
union all
select 'billevents',
       (select count(*) from mmconsume_billevents),
       (select count(*) from mmconsume_billevents_clean),
       concat(cast((1 - (select count(*) from mmconsume_billevents_clean) / (select count(*) from mmconsume_billevents)) * 100 as decimal(5,2)), '%')
union all
select 'order_index',
       (select count(*) from order_index),
       (select count(*) from order_index_clean),
       concat(cast((1 - (select count(*) from order_index_clean) / (select count(*) from order_index)) * 100 as decimal(5,2)), '%')
union all
select 'media_index',
       (select count(*) from media_index),
       (select count(*) from media_index_clean),
       concat(cast((1 - (select count(*) from media_index_clean) / (select count(*) from media_index)) * 100 as decimal(5,2)), '%');