目前仍有17161条积压内容未处理，其中昨日积压xx



select * from app.d_ucms_doc_detail@cold_app #文章classv1


select * from app.tongji_group_tree_relation@cold_app #一二级人群对应关系

select* from app.d_graspingsta_crowdgroup #一级人群定义表





select* from app.d_fangroup_cotaggroup     #cotag词定义表

select* from app.rt_frq_editor_kafka_link #文章所属人群

select * from app.tongji_group_tree_relation@cold_app #一二级人群对应关系

select* from app.d_graspingsta_crowdgroup #一级人群定义表


app.d_fangroup_cotaggroup


SELECT groupid,
    classv1
FROM (
    SELECT groupid,
        classv1,
        row_number() OVER (PARTITION BY groupid ORDER BY classv1_num DESC) rk
    FROM (
        SELECT t1.groupid,
            fl.classv1,
            count(1) classv1_num
        FROM (
            SELECT groupid,
                docid
            FROM (
                SELECT wd.groupid,
                    rt.docid,
                    rt.opertype,
                    row_number() OVER (PARTITION BY rt.groupid, rt.docid ORDER BY insert_tm DESC) rk
                FROM app.d_fangroup_cotaggroup wd
                JOIN app.rt_frq_editor_kafka_link rt ON wd.groupid = rt.groupid
                WHERE to_char(wd.createtm, 'yyyy-mm-dd') <= '2021-11-09'
                    AND wd.status = 'waitting'
                ) t
            WHERE rk = 1
                AND opertype != 'del'
            ) t1
        JOIN app.d_ucms_doc_detail@cold_app fl
            ON t1.docid = fl.ucmsid
        GROUP BY t1.groupid, fl.classv1
        ) t2
    )t3
WHERE rk = 1



SELECT t3.keyword_id,
    t3.keyword,
    t3.docid,
    t3.groupid,
    CASE WHEN rq.classname IS NULL THEN rq1.classname ELSE rq.classname END classname
FROM (
    SELECT keyword_id,
        keyword,
        docid,
        groupid
    FROM (
        SELECT t1.keyword_id,
            t1.keyword,
            t1.docid,
            rt.groupid,
            rt.opertype,
            row_number() OVER (PARTITION BY rt.groupid, rt.docid ORDER BY insert_tm DESC) rk
        FROM (
            SELECT keyword_id,
                keyword,
                docid
            FROM (
                SELECT wd.groupid keyword_id,
                    wd.groupname keyword,
                    rt.docid,
                    rt.opertype,
                    row_number() OVER (PARTITION BY rt.groupid, rt.docid ORDER BY insert_tm DESC) rk
                FROM app.d_fangroup_cotaggroup wd
                JOIN app.rt_frq_editor_kafka_link rt ON wd.groupid = rt.groupid
                WHERE to_char(wd.createtm, 'yyyy-mm-dd') <= '2021-11-16'
                    -- AND to_char(wd.createtm, 'yyyy-mm-dd') >= '2021-11-01'
                    AND wd.status = 'waitting'
                ) t
            WHERE rk = 1
                AND opertype != 'del'
                AND keyword_id = '6768083130692669440'
            ) t1
        JOIN app.rt_frq_editor_kafka_link rt ON t1.docid = rt.docid
        ) t2
    WHERE rk = 1
        AND opertype != 'del'
    ) t3
LEFT JOIN app.tongji_group_tree_relation@cold_app rq ON t3.groupid = rq.groupid
    AND rq.type IN ( '一级人群', '二级人群')
LEFT JOIN app.d_ucms_doc_detail@cold_app fl ON t3.docid = fl.ucmsid
LEFT JOIN app.d_group_classv1_relation flrq ON fl.classv1 = flrq.classv1
LEFT JOIN app.tongji_group_tree_relation@cold_app rq1 ON flrq.groupid = rq1.groupid
    AND rq1.type IN ( '一级人群', '二级人群')
    



select * from app.tongji_group_tree_relation@cold_app #一二级人群对应关系

select* from app.d_graspingsta_crowdgroup #一级人群定义表


SELECT a.groupid,
	a.groupname,
	b.groupname,
	b.type,
    b.classname
FROM app.d_graspingsta_crowdgroup a
JOIN app.tongji_group_tree_relation@cold_app b ON a.groupid = b.groupid



select groupid,
    , classname
app.tongji_group_tree_relation@cold_app
where type in ( '一级人群', '二级人群')
group by groupid


SELECT a.docid FROM 
app.d_group_classv1_relation a
JOIN app.d_ucms_doc_detail@cold_app b ON a.classv1 = b.classv1

    SELECT keyword_id,
        keyword,
        classname
    FROM (
        SELECT keyword_id,
            keyword,
            classname,
            row_number() OVER (PARTITION BY keyword ORDER BY num DESC) rk
        FROM (
            SELECT rl.keyword_id,
                rl.keyword,
                CASE WHEN rq.classname IS NULL THEN rq1.classname ELSE rq.classname END classname,
                count(CASE WHEN rq.classname IS NULL THEN rq1.classname ELSE rq.classname END) num
            FROM app.tongji_yunying_keyword_mid rl
            LEFT JOIN app.tongji_group_tree_relation@cold_app rq ON rl.groupid = rq.groupid
                AND rq.type IN ('一级人群', '二级人群')
            LEFT JOIN app.d_ucms_doc_detail@cold_app fl ON rl.docid = fl.ucmsid
            LEFT JOIN app.d_group_classv1_relation flrq ON fl.classv1 = flrq.classv1
            LEFT JOIN app.tongji_group_tree_relation@cold_app rq1 ON flrq.groupid = rq1.groupid
                AND rq1.type IN ('一级人群', '二级人群')
            GROUP BY rl.keyword_id,
                rl.keyword,
                CASE WHEN rq.classname IS NULL THEN rq1.classname ELSE rq.classname END
            ) t
        ) t1
    WHERE rk = 1




SELECT rl.keyword_id,
    rl.keyword,
    rl.docid,
    rl.groupid,
FROM app.tongji_yunying_keyword_mid rl
LEFT JOIN app.d_ucms_doc_detail@cold_app fl ON rl.docid = fl.ucmsid



ucms_88Yf9SQPx8o