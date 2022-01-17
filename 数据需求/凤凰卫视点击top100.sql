-- 凤凰卫视点击top100
-- id，标题、分享页链接、11月总点击量


SELECT a.docid,
	a.click_num,
	b.title,
	b.ucmsid
FROM (
	SELECT docid,
		count(1) click_num
	FROM cdm.dwd_log_newsapp_page_di
	WHERE is_click = 1
		AND dt >= '2021-12-01'
	GROUP BY docid
	) a
JOIN (
	SELECT docid,
		title,
		ucmsid
	FROM cdm.dim_content1new_di
	WHERE datasource_src IN ('凤凰精选', '凤凰卫视王牌', '凤凰卫视资讯', '凤凰卫视媒资')
	) b ON a.docid = b.docid
ORDER BY click_num DESC limit 100


-- 接口调用，获取URL
