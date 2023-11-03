DROP VIEW pollster_health_status;
CREATE VIEW pollster_health_status as select * from pollster_health_status_{{year}};