show tables;
select quantile(0.01)(x) as quantilex,quantile(0.99)(x), median(progress), avg(progress), max(progress), min(progress), sum(progress), median(x), avg(x), max(x), min(x), sum(x) from `14f8d31e29b7eec7272ae64505a73686`;
-- numeric fields
select quantile(0.1)(count),quantile(0.9)(count), quantile(0.60)(count),quantile(0.40)(count), median(count), avg(count), max(count), min(count), sum(count) from `184df5f3ac115006e236071d01be5445`;

-- histogram much diap
with histogram(100)(count) as hist
select
    arrayJoin(hist).3 AS height,
    arrayJoin(hist).1 AS from,
    arrayJoin(hist).2 AS to,
    bar(height, 1, 611, 10) AS bar
from (
    select count from `184df5f3ac115006e236071d01be5445` where count between 1 and 50709
    )
order by height desc;

-- histogram fav
with histogram(10)(count) as hist
select
    arrayJoin(hist).3 AS height,
    arrayJoin(hist).1 AS from,
    arrayJoin(hist).2 AS to,
    bar(height, 5, 419, 10) AS bar
from (
    select count from `184df5f3ac115006e236071d01be5445` where count between 1 and 199
    )
order by from;

select count(*), projectid from `184df5f3ac115006e236071d01be5445` group by projectid order by count(*) desc limit 10;
select count(*), projectid from `184df5f3ac115006e236071d01be5445` group by projectid order by count(*) asc limit 10;
select count(*), ddbrandname from `14f8d31e29b7eec7272ae64505a73686` group by ddbrandname order by count(*) desc limit 10;

select count(*), ddbrandname, ddos, ddmodel, status, dddevicename from `14f8d31e29b7eec7272ae64505a73686` group by ddbrandname, ddos, ddmodel, status, dddevicename order by count(*) desc limit 10;
select count(*), ddbrandname, ddos, ddmodel, status, dddevicename from `14f8d31e29b7eec7272ae64505a73686` group by ddbrandname, ddos, ddmodel, status, dddevicename order by count(*) desc limit 10;

-- quantile from uniqs
select median(val), quantile(0.2)(val), quantile(0.8)(val) from (
                                                                    select uniq(user) as val from `14f8d31e29b7eec7272ae64505a73686`
                                                                    union all
                                                                    select uniq(type) as val from `14f8d31e29b7eec7272ae64505a73686`
                                                                    union all select uniq(articletype)as val from `14f8d31e29b7eec7272ae64505a73686`
                                                                    union all select uniq(status)as val from `14f8d31e29b7eec7272ae64505a73686`
                                                                    union all select uniq(ddbrandname)as val from `14f8d31e29b7eec7272ae64505a73686`
                                                                    union all select uniq(ddmodel)as val from `14f8d31e29b7eec7272ae64505a73686`
                                                                    union all select uniq(ddos)as val from `14f8d31e29b7eec7272ae64505a73686`
                                                                    union all select uniq(dddevicename)as val from `14f8d31e29b7eec7272ae64505a73686`
                                                                ) a ;
-- uniq counts
select
    uniq(user),
    uniq(type),
    uniq(articletype),
    uniq(status),
    uniq(ddbrandname),
    uniq(ddmodel),
    uniq(ddos),
    uniq(dddevicename) from `14f8d31e29b7eec7272ae64505a73686`;
-- most popular chains
select count(*), type,
       articletype,
       status,
       ddbrandname,
       ddos,
       dddevicename
from `14f8d31e29b7eec7272ae64505a73686`
group by     type,
             articletype,
             status,
             ddbrandname,
             ddos,
             dddevicename
order by count() desc limit 10;
-- most popular chains less
select count(*),
       ddbrandname,
       ddos,
       dddevicename
from `14f8d31e29b7eec7272ae64505a73686`
group by
    ddbrandname,
    ddos,
    dddevicename
order by count() desc limit 10;
-- most popular chains less
select count(*),
       type,
       ddbrandname,
       ddos,
       dddevicename
from `14f8d31e29b7eec7272ae64505a73686`
group by type,
         ddbrandname,
         ddos,
         dddevicename
order by count() desc limit 100;
-- worst chains
select count(*), type,
       articletype,
       status,
       ddbrandname,
       ddos,
       dddevicename
from `14f8d31e29b7eec7272ae64505a73686`
group by     type,
             articletype,
             status,
             ddbrandname,
             ddos,
             dddevicename
order by count() asc limit 10;

-- fav one column
select count(*), ddbrandname from `14f8d31e29b7eec7272ae64505a73686` group by ddbrandname order by count(*) asc limit 10;
describe table `14f8d31e29b7eec7272ae64505a73686`