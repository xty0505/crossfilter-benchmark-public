drop table if exists sys.flights;
create table sys.flights(
  AIR_TIME DOUBLE PRECISION,
  ARR_DELAY DOUBLE PRECISION,
  ARR_TIME DOUBLE PRECISION,
  DEP_DELAY DOUBLE PRECISION,
  DEP_TIME DOUBLE PRECISION,
  DISTANCE DOUBLE PRECISION,
  FL_DATE TEXT
);
