create table app.tongji_yunying_keyword (
    tm date not null,
    total_word_waitting int not null,
    total_word_checked int not null,
    total_word_passed int not null,
    total_word_unpassed int not null,
    new_word int not null,
    new_word_waitting int not null,
    new_word_checked int not null,
    new_word_passed int not null,
    new_word_unpassed int not null,
    total_infopv int not null,
    total_clickpv int not null,
    total_infoclickpv int not null,
    new_infopv int not null,
    new_clickpv int not null,
    new_infoclickpv int not null
)


new_word：当日新增量
total_word_checked：当日审核量
new_word_checked：其中:当日新增审核量
total_word_waitting：当前积压量
new_word_waitting：其中:当日新增积压量
total_infopv：曝光量
total_clickpv：点击量
total_infoclickrate：点击率
new_infopv：当日新增:曝光量
new_clickpv：点击量
new_infoclickrate：点击率
tm: 日期

SELECT tm,
    new_word,
    total_word_checked,
    total_word_waitting,
    new_word_checked,
    new_word_waitting,
    total_infopv,
    total_clickpv,
    to_char(total_infoclickpv / total_infopv * 100, 'fm9990.00') || '%' total_infoclickrate,
    new_infopv,
    new_clickpv,
    to_char(new_infoclickpv / new_infopv * 100, 'fm9990.00') || '%' new_infoclickrate
FROM app.tongji_yunying_keyword
where tm between date'2021-11-10'-29 and date'2021-11-10'
ORDER BY tm DESC