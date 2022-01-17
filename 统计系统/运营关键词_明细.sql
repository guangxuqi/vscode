CREATE TABLE app.tongji_yunying_keyword_detail (
	tm DATE NOT NULL,
	keyword_id VARCHAR(128),
	keyword VARCHAR(128),
	keyword_creator VARCHAR(128),
	keyword_createtime DATE,
	keyword_status VARCHAR(128),
	infopv INT,
	clickpv INT,
	infoclickpv INT,
    article_id VARCHAR(123),
	article_title VARCHAR(128),
    article_creator VARCHAR(128),
	article_time DATE,
    article_type VARCHAR(128)
	)


-- 明细页面
SELECT tm,
    keyword_id,
    keyword,
    staff_name keyword_creator,
    keyword_status,
    content_applied,
    infopv,
    clickpv,
    infoclickrate
FROM (
    SELECT tm,
        keyword_id,
        keyword,
        keyword_creator,
        CASE WHEN keyword_status = 'waitting' THEN '审核中' WHEN keyword_status = 'online' THEN '上线' WHEN keyword_status = 'offline' THEN '下线'  END keyword_status,
        count(1) content_applied,
        sum(infopv) infopv,
        sum(clickpv) clickpv,
        CASE WHEN sum(infopv) IS NOT NULL THEN to_char(sum(infoclickpv) / sum(infopv) * 100, 'fm9990.00') || '%' END infoclickrate
    FROM app.tongji_yunying_keyword_detail
    WHERE tm = DATE '2021-11-11'
        GROUP BY tm,
        keyword_id,
        keyword,
        keyword_creator,
        keyword_status
    ) a
LEFT JOIN (
    SELECT staff_name,
        staff_id
    FROM app.rtx_staff
    WHERE tm IN (
            SELECT max(tm)
            FROM app.rtx_staff
            )
    ) b ON a.keyword_creator = b.staff_id
WHERE keyword = '脏腑'
ORDER BY tm DESC


keyword：关键词
keyword_creator：编辑
keyword_status：状态
content_applied: 文章数
infopv：曝光量
clickpv：点击量
infoclickrate：点击率
tm：日期

-- 点关键词下钻页面

SELECT article_id,
    article_title,
    staff_name article_creator,
    article_time,
    article_type,
    infopv,
    clickpv,
    infoclickrate
FROM (
    SELECT article_id,
        article_title,
        article_creator,
        article_time,
        CASE WHEN article_type = 'short' THEN '短内容' WHEN article_type = 'miniVideo' THEN '小视频' WHEN article_type = 'article' THEN '图文' WHEN article_type = 'video' THEN '视频' ELSE '其他' END article_type,
        infopv,
        clickpv,
        CASE WHEN infopv IS NOT NULL THEN to_char(infoclickpv / infopv * 100, 'fm9990.00') || '%' END infoclickrate
    FROM app.tongji_yunying_keyword_detail
    WHERE tm BETWEEN DATE '2021-11-10' - 29 AND DATE '2021-11-10'
        AND keyword_id = '6864023674345758720'
    ) a
LEFT JOIN (
    SELECT staff_name,
        staff_id
    FROM app.rtx_staff
    WHERE tm IN (
            SELECT max(tm)
            FROM app.rtx_staff
            )
    ) b ON a.article_creator = b.staff_id 
ORDER BY tm, article_time DESC


SELECT article_id,
    article_title,
    staff_name article_creator,
    article_time,
    article_type,
    infopv,
    clickpv,
    infoclickrate
FROM (
    SELECT article_id,
        article_title,
        article_creator,
        article_time,
        CASE WHEN article_type = 'short' THEN '短内容' WHEN article_type = 'miniVideo' THEN '小视频' WHEN article_type = 'article' THEN '图文' WHEN article_type = 'video' THEN '视频' ELSE '其他' END article_type,
        infopv,
        clickpv,
        CASE WHEN infopv IS NOT NULL THEN to_char(infoclickpv / infopv * 100, 'fm9990.00') || '%' END infoclickrate
    FROM app.tongji_yunying_keyword_detail
    WHERE tm = DATE '2021-11-10'
        AND keyword = '脏腑'
    ) a
LEFT JOIN (
    SELECT staff_name,
        staff_id
    FROM app.rtx_staff
    WHERE tm IN (
            SELECT max(tm)
            FROM app.rtx_staff
            )
    ) b ON a.article_creator = b.staff_id 
ORDER BY article_time DESC


article_title：标题
article_creator：编辑
article_time：创建时间
article_type：文章类型
infopv：曝光量
clickpv：点击量
infoclickrate：点击率


{'docId': '8B0u5QVmC6S', 'score12h': 7.397217419039605}

ods.dim_read_score_di

ods.dim_read_score_df


CREATE TABLE `ods.dim_read_score_di` (
  `docId` string COMMENT '文章ID', 
  `score` float COMMENT '文章分数',
  `score_type` string COMMENT '分数类型',
  `rk` int COMMENT '评分顺序'
  )
COMMENT '{"owner": "qigx","describe": "文章阅读评分","dependencies":"hadoop fs -text /user/source/aiArticleScore"}'
PARTITIONED BY ( `dt` string COMMENT 'Partitioned by Date')
row format delimited fields terminated by ',';



CREATE TABLE `ods.dim_read_score_df` (
  `docId` string COMMENT '文章ID', 
  `score` float COMMENT '最新文章分数',
  `score_type` string COMMENT '分数类型',
  `commented_date` date COMMENT '该评分的日期'
  )
COMMENT '{"owner": "qigx","describe": "文章阅读评分全量表","dependencies":"ods.dim_read_score_di"}'
PARTITIONED BY ( `dt` string COMMENT 'Partitioned by Date')

    INSERT overwrite TABLE ods.dim_read_score_df
    SELECT docId,
        score,
        score_type,
        dt commented_dt
    FROM (
        SELECT docId,
            score,
            score_type,
            dt,
            row_number() OVER (PARTITION BY docId ORDER BY dt DESC) rk
        FROM ods.dim_read_score_di
        ) tmp
    WHERE rk = 1


    INSERT overwrite TABLE ods.dim_read_score_df partition(dt='%s')
    SELECT docid,
        score,
        score_type,
        commented_date
    FROM (
        SELECT docid,
            score,
            score_type,
            commented_date,
            row_number() OVER (PARTITION BY docid, score_type ORDER BY dt DESC) rk
        FROM (
            SELECT docid,
                score,
                score_type,
                dt commented_date
            FROM (
                SELECT docid,
                    score,
                    score_type,
                    dt,
                    row_number() OVER (PARTITION BY docid, score_type ORDER BY dt DESC, rk DESC) rk
                FROM ods.dim_read_score_di
                WHERE dt = '2021-11-11'
                ) tmp
            WHERE rk = 1
            UNION ALL
            SELECT dicid,
                score,
                score_type,
                commented_date
            FROM ods.dim_read_score_df
            WHERE dt = date_sub('2021-11-11', 1)
        ) t
    ) t1



LOAD data LOCAL inpath '/data1/logs/qigx/work/article_score/data/data.csv' overwrite
INTO TABLE ods.dim_read_score_di PARTITION (dt = '%s')
