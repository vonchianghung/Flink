

--Apache官方培训课程, 补全类目名称字段
CREATE VIEW rich_user_behavior AS
SELECT u.user_id,
        u.item_id,
        u.behavior,
        CASE c.parent_category_id
            WHEN 1 THEN '服装鞋包'
            WHEN 2 THEN '家电'
            WHEN 3 THEN '美妆'
            WHEN 4 THEN '母婴'
            WHEN 5 THEN '3C数码'
            WHEN 6 THEN '运动户外'
            WHEN 7 THEN '视频'
            WHEN 8 THEN '家具'
            ELSE '其他' END AS category_name
  FROM user_behavior AS u
  LEFT JOIN category_dim FOR SYSTEM_TIME AS OF u.proctime AS c
    ON u.category_id = C.sub_category_id


--统计每个类目的成交量 (直接在FLINK SQL Client中执行下面SQL,这个任务就会被提交了,在Flink WEBUI界面就可以看到对应任务
--对应的数据也就被插入到es中了。
INSERT INTO top_category
SELECT category_name,count(*) buy_cnt
  FROM rich_user_behavior
 WHERE behavior = 'buy'
 GROUP BY category_name