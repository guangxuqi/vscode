
select if(b.price is null or b.price = '#', b.price, int(int(b.price)/500)*500), count(distinct userkey) uv, count(if(in_type='#', null, in_type)) inpv, sum(infopv) infopv, 
       sum(staypv) staypv, sum(finishpv) finishpv, sum(readpv) readpv, sum(refreshpv) refreshpv, sum(visitdur) visitdur from
(select userkey, device, gps_city, in_type, infopv, staypv, finishpv, readpv, cdr+pnp refreshpv, visitdur from cdm.dwd_log_newsapp_visit_di where dt='2022-01-05' and first_tm>='2021-12-30')a
left join cdm.dim_device_df b on a.device=b.device
group by if(b.price is null or b.price = '#', b.price, int(int(b.price)/500)*500)


select if(b.level is null, a.gps_city, b.level), count(distinct userkey) uv, count(if(in_type='#', null, in_type)) inpv, sum(infopv) infopv, 
       sum(staypv) staypv, sum(finishpv) finishpv, sum(readpv) readpv, sum(refreshpv) refreshpv, sum(visitdur) visitdur from
(select userkey, device, gps_city, in_type, infopv, staypv, finishpv, readpv, cdr+pnp refreshpv, visitdur from cdm.dwd_log_newsapp_visit_di where dt='2022-01-05')a
left join cdm.dim_citylevel_df b on a.gps_city=b.city
group by if(b.level is null, a.gps_city, b.level) order by uv