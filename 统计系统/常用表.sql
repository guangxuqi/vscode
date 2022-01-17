视频剪辑
app.d_ucms_doc_detail



SELECT a.docid,
	a.click_num,
	b.title,
	b.ucmsid
FROM (
	SELECT docid,
		count(1) click_num
	FROM cdm.dwd_log_newsapp_page_di
	WHERE is_click = 1
		AND dt >= '2021-11-01'
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



CREATE TABLE `cdm.dwd_log_article_modify_di_demo` (
    `id` string COMMENT '文章ID',
    `title` string COMMENT '文章标题',
    `classv` string COMMENT '人群分类',
    `storagetime` string COMMENT '入库时间',
    `updatetime` string COMMENT '更新时间',
    `type` string COMMENT '类型',
    `artificialalgcatedata` string COMMENT 'artificialAlgCateData'
    )
    COMMENT '{"owner": "qigx","describe": "文章修改记录","dependencies":"hadoop fs -text /user/source/ucmsArticleInfo/"}'
    PARTITIONED BY (`dt` string COMMENT 'Partitioned by Date')
    row format delimited fields terminated by ','



		SELECT b.classname,
			b.keyword,
			a.creteym
        FROM app.d_fangroup_cotaggroup a
        JOIN app.tongji_yunying_keyword_class b ON a.groupid = b.keywordid
            AND a.status = 'waitting'
            AND b.classname in ()



			SELECT docid,
                infopv,
                clickpv,
                infoclickpv
            FROM cdm.dws_log_all_content1new_1d
            WHERE dt = '2021-11-24'
			
			
SELECT groupid,
groupname,
creator,
createtm,
operatetm,
status
FROM ods.d_fangroup_cotaggroup
WHERE dt = '2021-11-22'