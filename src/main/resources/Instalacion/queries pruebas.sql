SELECT *
FROM control_process

SELECT *
FROM control_error

SELECT *
FROM control_processexec
ORDER BY 1 DESC

SELECT *
FROM control_processexecstep
WHERE processexec_id = '201906021838500087922858746189'
ORDER BY 1 desc

SELECT *
FROM control_dq

SELECT *
FROM control_testplan
WHERE testplangroup_id = 'pp041'
AND testplan_name = 'TestPlan_IsOK'

select cast(count(1) as Integer) as cantidad, cast(sum(testplan_isok) as Integer) as total_ok 
                    from control_testplan 

SELECT 'application_1559496407342_0002'
                                   , '39637'
                                   , 'http://172.17.0.2:4041'
                                   , '2019-06-02 17:38:08:074'
                                   , '01 - Plan pruebas Proc_PlanPruebas_CargaMaster'
FROM dual

SELECT * FROM dual


SELECT mdm_manualchange 
                    FROM control_process 
                    WHERE process_id = 'algo'