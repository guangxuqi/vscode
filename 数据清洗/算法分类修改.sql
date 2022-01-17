分视频和图文 第一次和最近一次不同的数据   int

SELECT a.id,
	a.title,
	b.classv first_classv,
	a.artificialalgcatedata last_artificialalgcatedata
FROM (
	SELECT *
	FROM (
		SELECT id,
			title,
			storagetime,
			updatetime,
			regexp_replace(classv, '"|\\[|\\]', '') classv,
			regexp_replace(artificialalgcatedata, '"', '') artificialalgcatedata,
			row_number() OVER (
				PARTITION BY id ORDER BY dt DESC,
					updatetime DESC
				) last_rk
		FROM cdm.dwd_log_article_modify_di
		WHERE updatetime != '#'
			AND type = 'video'
			AND artificialalgcatedata != '#'
			AND dt >= '2021-08-02'
		) t
	WHERE last_rk = 1
	) a
JOIN (
	SELECT *
	FROM (
		SELECT id,
			storagetime,
			updatetime,
			regexp_replace(classv, '"|\\[|\\]', '') classv,
			regexp_replace(artificialalgcatedata, '"', '') artificialalgcatedata,
			row_number() OVER (
				PARTITION BY id ORDER BY dt,
					updatetime
				) first_rk
		FROM cdm.dwd_log_article_modify_di
		WHERE dt >= '2021-08-02'
			AND type = 'video'
			AND classv != '#'
		) t
	WHERE first_rk = 1
	) b ON a.id = b.id
WHERE a.last_artificialalgcatedata != b.classv

SELECT a.id,
	a.title,
	b.classv first_classv,
	a.classv last_classv
FROM (
	SELECT *
	FROM (
		SELECT id,
			title,
			storagetime,
			updatetime,
			regexp_replace(classv, '"|\\[|\\]', '') classv,
			regexp_replace(artificialalgcatedata, '"', '') artificialalgcatedata,
			row_number() OVER (
				PARTITION BY id ORDER BY dt DESC,
					updatetime DESC
				) last_rk
		FROM cdm.dwd_log_article_modify_di
		WHERE updatetime != '#'
			AND type != 'video'
			AND classv != '#'
			AND dt >= '2021-08-02'
		) t
	WHERE last_rk = 1
	) a
JOIN (
	SELECT *
	FROM (
		SELECT id,
			storagetime,
			updatetime,
			regexp_replace(classv, '"|\\[|\\]', '') classv,
			regexp_replace(artificialalgcatedata, '"', '') artificialalgcatedata,
			row_number() OVER (
				PARTITION BY id ORDER BY dt,
					updatetime
				) first_rk
		FROM cdm.dwd_log_article_modify_di
		WHERE dt >= '2021-08-02'
			AND type != 'video'
			AND classv != '#'
		) t
	WHERE first_rk = 1
	) b ON a.id = b.id
WHERE a.classv != b.classv
