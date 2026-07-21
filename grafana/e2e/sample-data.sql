# Sample htcondordb data for the Grafana plugin E2E test. One statement per line
# (the entrypoint runs each via htcondordb-cli). Lines starting with # are ignored.

# --- Machines: partitionable startd slots in various states ---
CREATE TABLE machines
INSERT INTO machines (Key,Name,State,Activity,Cpus,Memory,LoadAvg) VALUES ('slot1@n1','slot1@n1','Unclaimed','Idle',8,16384,0.05)
INSERT INTO machines (Key,Name,State,Activity,Cpus,Memory,LoadAvg) VALUES ('slot1@n2','slot1@n2','Claimed','Busy',16,32768,0.92)
INSERT INTO machines (Key,Name,State,Activity,Cpus,Memory,LoadAvg) VALUES ('slot1@n3','slot1@n3','Claimed','Busy',16,32768,0.78)
INSERT INTO machines (Key,Name,State,Activity,Cpus,Memory,LoadAvg) VALUES ('slot1@n4','slot1@n4','Unclaimed','Idle',32,65536,0.01)
INSERT INTO machines (Key,Name,State,Activity,Cpus,Memory,LoadAvg) VALUES ('slot1@n5','slot1@n5','Claimed','Retiring',8,16384,0.45)
INSERT INTO machines (Key,Name,State,Activity,Cpus,Memory,LoadAvg) VALUES ('slot1@n6','slot1@n6','Owner','Idle',4,8192,0.10)

# --- Jobs: idle (1), running (2), completed (4) across three owners ---
CREATE TABLE jobs
INSERT INTO jobs (Key,ClusterId,ProcId,Owner,JobStatus,RequestCpus,RequestMemory,QDate) VALUES ('101.0',101,0,'alice',2,4,4096,1752000000)
INSERT INTO jobs (Key,ClusterId,ProcId,Owner,JobStatus,RequestCpus,RequestMemory,QDate) VALUES ('101.1',101,1,'alice',2,4,4096,1752000300)
INSERT INTO jobs (Key,ClusterId,ProcId,Owner,JobStatus,RequestCpus,RequestMemory,QDate) VALUES ('101.2',101,2,'alice',1,4,4096,1752000600)
INSERT INTO jobs (Key,ClusterId,ProcId,Owner,JobStatus,RequestCpus,RequestMemory,QDate) VALUES ('202.0',202,0,'bob',2,8,16384,1752001000)
INSERT INTO jobs (Key,ClusterId,ProcId,Owner,JobStatus,RequestCpus,RequestMemory,QDate) VALUES ('202.1',202,1,'bob',1,8,16384,1752001200)
INSERT INTO jobs (Key,ClusterId,ProcId,Owner,JobStatus,RequestCpus,RequestMemory,QDate) VALUES ('202.2',202,2,'bob',4,8,16384,1752001500)
INSERT INTO jobs (Key,ClusterId,ProcId,Owner,JobStatus,RequestCpus,RequestMemory,QDate) VALUES ('303.0',303,0,'carol',4,2,2048,1752002000)
INSERT INTO jobs (Key,ClusterId,ProcId,Owner,JobStatus,RequestCpus,RequestMemory,QDate) VALUES ('303.1',303,1,'carol',1,2,2048,1752002400)
INSERT INTO jobs (Key,ClusterId,ProcId,Owner,JobStatus,RequestCpus,RequestMemory,QDate) VALUES ('303.2',303,2,'carol',2,2,2048,1752002700)

# --- Materialized view: jobs per Owner (label) with COUNT + total RequestCpus.
# Exposed on /metrics as jobs_by_owner_jobs{owner=...} and jobs_by_owner_cpus{owner=...}.
CREATE MATERIALIZED VIEW jobs_by_owner AS SELECT Owner AS label_owner, COUNT(*) AS metric_jobs, SUM(RequestCpus) AS metric_cpus FROM jobs GROUP BY Owner
