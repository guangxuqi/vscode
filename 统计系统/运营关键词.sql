-- 关键词的运营，我们开始加关键词的相关统计，
-- 包括每天多了多少新词，新词是什么，每个分类下面有多少词，词对应哪些文章。
-- 以及输入文章看文章有哪些关键词。这应该是多个页面。
-- 另外，词每天也要求都审完，所以要加次的积压统计，逻辑和内容那个完全一样。


-- 概况
-- 每天新增多少词，每天过审多少词， 其中当日新增的词有多少， 当日挤压多少词， 累计挤压多少词， 曝光，点击，点击率

SELECT '2021-11-09' log_date,
    sum(CASE WHEN status = 'waitting' THEN 1 ELSE 0 END) total_word_waitting,
    sum(CASE WHEN to_char(operatetm, 'yyyy-mm-dd') = '2021-11-09' AND status != 'waitting' THEN 1 ELSE 0 END) total_word_checked,
    sum(CASE WHEN to_char(operatetm, 'yyyy-mm-dd') = '2021-11-09' AND status = 'online' THEN 1 ELSE 0 END) total_word_passed,
    sum(CASE WHEN to_char(operatetm, 'yyyy-mm-dd') = '2021-11-09' AND status = 'offline' THEN 1 ELSE 0 END) total_word_unpassed,
	sum(CASE WHEN to_char(createtm, 'yyyy-mm-dd') = '2021-11-09' THEN 1 ELSE 0 END) new_word,
    sum(CASE WHEN to_char(createtm, 'yyyy-mm-dd') = '2021-11-09' AND to_char(operatetm, 'yyyy-mm-dd') = '2021-11-09' AND status = 'waitting' THEN 1 ELSE 0 END) new_word_waitting,
    sum(CASE WHEN to_char(createtm, 'yyyy-mm-dd') = '2021-11-09' AND to_char(operatetm, 'yyyy-mm-dd') = '2021-11-09' AND status != 'waitting' THEN 1 ELSE 0 END) new_word_checked,
	sum(CASE WHEN to_char(createtm, 'yyyy-mm-dd') = '2021-11-09' AND to_char(operatetm, 'yyyy-mm-dd') = '2021-11-09' AND status = 'online' THEN 1 ELSE 0 END) new_word_passed,
    sum(CASE WHEN to_char(createtm, 'yyyy-mm-dd') = '2021-11-09' AND to_char(operatetm, 'yyyy-mm-dd') = '2021-11-09' AND status = 'offline' THEN 1 ELSE 0 END) new_word_unpassed
FROM app.d_fangroup_cotaggroup
WHERE to_char(createtm, 'yyyy-mm-dd') <= '2021-11-09'


--词的详情
-- 词的名字，id， 创建人，创建时间，当前状态
SELECT groupname,
	groupid,
	creator,
	createtm,
	status
FROM app.d_fangroup_cotaggroup
WHERE to_char(createtm, 'yyyy-mm-dd') <= '2021-11-09'

SELECT groupname,
	groupid,
	creator,
	createtm,
	status
FROM app.d_fangroup_cotaggroup
WHERE to_char(createtm, 'yyyy-mm-dd') = '2021-11-09'

6863711398803804160
--- 词下有那些文章
-- 词名字， 文章id，文章标题, 创建人，创建时间， 文章类型（short 短内容  article  图文  miniVideo  小视频  video 视频）

SELECT word_name,
    docid,
    title,
    creator,
    createtime,
    content_type
FROM (
	SELECT wd.groupname word_name,
		rt.docid,
		rt.title,
		rt.creator,
		rt.createtime,
		CASE WHEN rt.type = 'short' THEN '短内容' WHEN rt.type = 'miniVideo' THEN '小视频' WHEN rt.type = 'article' THEN '图文' WHEN rt.type = 'video' THEN '视频' ELSE ' 其他' END content_type,
		opertype,
		row_number() OVER (PARTITION BY rt.groupid ORDER BY insert_tm DESC) rk
	FROM app.d_fangroup_cotaggroup wd
	JOIN app.rt_frq_editor_kafka_link rt ON wd.groupid = rt.groupid
	WHERE to_char(wd.createtm, 'yyyy-mm-dd') <= '2021-11-09'
		AND to_char(wd.createtm, 'yyyy-mm-dd') >= '2021-11-01'
	) tmp
WHERE rk = 1



-- opertype: create add del





select opertype, count(*) from app.rt_frq_editor_kafka_link  group by opertype

----输入文章看文章有哪些关键词
文章id， 标题， 哪些词（詹姆斯，科比），类型
select b.groupname, a.*  from app.rt_frq_editor_kafka_link a
left join
app.d_fangroup_cotaggroup b on a.groupid = b.groupid
 where docid = 'ucms_84PGUcnFIaF'


SELECT docid,
    title,
    content_type,
    wm_concat(groupname) word_list
FROM (
	SELECT wd.groupname,
		rt.docid,
		rt.title,
		rt.creator,
		rt.createtime,
		CASE WHEN rt.type = 'short' THEN '短内容' WHEN rt.type = 'miniVideo' THEN '小视频' WHEN rt.type = 'article' THEN '图文' WHEN rt.type = 'video' THEN '视频' ELSE ' 其他' END content_type,
		opertype,
		row_number() OVER (PARTITION BY rt.groupid ORDER BY insert_tm DESC) rk
	FROM app.d_fangroup_cotaggroup wd
	JOIN app.rt_frq_editor_kafka_link rt ON wd.groupid = rt.groupid
	WHERE to_char(wd.createtm, 'yyyy-mm-dd') <= '2021-11-09'
        AND rt.docid = 'ucms_8B3ly76Is23'
	) tmp
WHERE rk = 1
GROUP BY docid, title, content_type


SELECT docid,
    title,
    content_type,
    wm_concat(groupname) word_list
FROM (
  SELECT wd.groupname,
    rt.docid,
    rt.title,
    rt.creator,
    rt.createtime,
    CASE WHEN rt.type = 'short' THEN '短内容' WHEN rt.type = 'miniVideo' THEN '小视频' WHEN rt.type = 'article' THEN '图文' WHEN rt.type = 'video' THEN '视频' ELSE ' 其他' END content_type,
    opertype,
    row_number() OVER (PARTITION BY rt.groupid ORDER BY insert_tm DESC) rk
  FROM app.d_fangroup_cotaggroup wd
  JOIN app.rt_frq_editor_kafka_link rt ON wd.groupid = rt.groupid
  WHERE to_char(wd.createtm, 'yyyy-mm-dd') <= '2021-11-09'
        AND (rt.docid = 'ucms_8B3ly76Is123' OR rt.docid = 'ucms_' || '8B3ly76Is23')
  ) tmp
WHERE rk = 1 AND opertype != 'del'
GROUP BY docid, title, content_type


SELECT docid,
    title,
    content_type,
    wm_concat(groupname) word_list
FROM (
  SELECT wd.groupname,
    rt.docid,
    rt.title,
    rt.creator,
    rt.createtime,
    CASE WHEN rt.type = 'short' THEN '短内容' WHEN rt.type = 'miniVideo' THEN '小视频' WHEN rt.type = 'article' THEN '图文' WHEN rt.type = 'video' THEN '视频' ELSE ' 其他' END content_type,
    opertype,
    row_number() OVER (PARTITION BY rt.groupid ORDER BY insert_tm DESC) rk
  FROM app.d_fangroup_cotaggroup wd
  JOIN app.rt_frq_editor_kafka_link rt ON wd.groupid = rt.groupid
  WHERE to_char(wd.createtm, 'yyyy-mm-dd') <= '2021-11-09'
        AND  (rt.docid = 'ucms_8B3ly76Is23' OR rt.docid = 'ucms_' || 'ucms_8B3ly76Is23')
  ) tmp
WHERE rk = 1 AND opertype != 'del'
GROUP BY docid, title, content_type